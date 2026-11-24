package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
	"eccom/internal/protocol"
)

// fakeAdapter answers every send the same way: ok, timeout or error.
type fakeAdapter struct {
	rec     *events.Recorder
	outcome string
}

func (f *fakeAdapter) Start() error { return nil }
func (f *fakeAdapter) Send(m protocol.Message) error {
	if err := f.fail(); err != nil {
		return err
	}
	f.rec.Record(events.SendCompleted, m.CorrelationID, m.SequenceNumber, nil)
	return nil
}
func (f *fakeAdapter) SendAndWaitReply(m protocol.Message, _ time.Duration) (protocol.Message, error) {
	if err := f.fail(); err != nil {
		return protocol.Message{}, err
	}
	return protocol.Message{CorrelationID: m.CorrelationID, SequenceNumber: m.SequenceNumber, Payload: append([]byte("re:"), m.Payload...)}, nil
}
func (f *fakeAdapter) Listen(protocol.Handler) error               { return nil }
func (f *fakeAdapter) ListenWithReply(protocol.ReplyHandler) error { return nil }
func (f *fakeAdapter) Stop(context.Context) error                  { return nil }
func (f *fakeAdapter) fail() error {
	switch f.outcome {
	case "timeout":
		return fmt.Errorf("nobody answered: %w", protocol.ErrTimeout)
	case "error":
		return errors.New("connection refused")
	}
	return nil
}

func testScenario(interaction string, total int64, warmupS float64) config.Scenario {
	s := config.Defaults()
	s.Name = "sender-test"
	s.Interaction = interaction
	s.Payload = config.Payload{SizeBytes: 8, Content: "random", Seed: 3}
	s.Load = config.Traffic{Mode: "constant_rate", MessagesPerSecond: 2000, TotalMessages: total, Concurrency: 4, Senders: 1}
	s.Experiment.WarmupS = warmupS
	s.Protocol.Type = "http"
	return s
}

func runSender(t *testing.T, scenario config.Scenario, outcome string) ([]events.Event, loadReport) {
	t.Helper()
	rec := events.NewRecorder("edge", filepath.Join(t.TempDir(), "edge-events.jsonl"))
	adapter := &fakeAdapter{rec: rec, outcome: outcome}
	report := NewSender(scenario, adapter, rec, 2).Run(context.Background())
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	return readEvents(t, rec.Path()), loadReport{report.MessagesAttempted, report.MessagesCompleted, report.MessagesFailed}
}

type loadReport struct{ attempted, completed, failed int64 }

func readEvents(t *testing.T, path string) []events.Event {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var recorded []events.Event
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var event events.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, event)
	}
	return recorded
}

// byMessage groups the event types of each correlation id in record order.
func byMessage(recorded []events.Event) map[string][]events.Type {
	grouped := map[string][]events.Type{}
	for _, event := range recorded {
		grouped[event.CorrelationID] = append(grouped[event.CorrelationID], event.EventType)
	}
	return grouped
}

func TestOneWayRecordsTheEventSequenceWithPhasesAndSequenceNumbers(t *testing.T) {
	recorded, report := runSender(t, testScenario("one_way", 5, 0.05), "ok")
	if report.attempted != 5 || report.completed != 5 {
		t.Fatalf("report %+v", report)
	}
	phases := map[string]int{}
	var lastWarmupSequence, firstMeasurementSequence int64 = -1, -1
	for _, event := range recorded {
		if event.Phase == nil {
			t.Fatalf("sender events carry a phase: %+v", event)
		}
		phases[*event.Phase]++
		if *event.Phase == "warmup" && event.SequenceNumber > lastWarmupSequence {
			lastWarmupSequence = event.SequenceNumber
		}
		if *event.Phase == "measurement" && (firstMeasurementSequence < 0 || event.SequenceNumber < firstMeasurementSequence) {
			firstMeasurementSequence = event.SequenceNumber
		}
	}
	if phases["warmup"] == 0 || phases["measurement"] != 15 {
		t.Fatalf("phases %v", phases)
	}
	if firstMeasurementSequence <= lastWarmupSequence {
		t.Fatalf("sequence numbers must continue across phases: warmup up to %d, measurement from %d", lastWarmupSequence, firstMeasurementSequence)
	}
	for id, types := range byMessage(recorded) {
		want := []events.Type{events.PayloadCreated, events.SendStarted, events.SendCompleted}
		if len(types) != 3 || types[0] != want[0] || types[1] != want[1] || types[2] != want[2] {
			t.Fatalf("message %s: %v", id, types)
		}
		if len(id) != 32 {
			t.Fatalf("correlation id %q is not uuid4().hex", id)
		}
	}
	for _, event := range recorded {
		if event.EventType == events.PayloadCreated && event.Metadata["size_bytes"] != float64(8) {
			t.Fatalf("PAYLOAD_CREATED metadata %v", event.Metadata)
		}
	}
}

func TestRequestResponseRecordsReplyReceivedWithTheReplySize(t *testing.T) {
	recorded, report := runSender(t, testScenario("request_response", 4, 0), "ok")
	if report.completed != 4 {
		t.Fatalf("report %+v", report)
	}
	for id, types := range byMessage(recorded) {
		if len(types) != 3 || types[2] != events.ReplyReceived {
			t.Fatalf("message %s: %v", id, types)
		}
	}
	for _, event := range recorded {
		if event.EventType == events.ReplyReceived && event.Metadata["size_bytes"] != float64(11) {
			t.Fatalf("REPLY_RECEIVED metadata %v", event.Metadata)
		}
	}
}

func TestTimeoutsAndErrorsAreRecordedAndCountedAsFailed(t *testing.T) {
	recorded, report := runSender(t, testScenario("request_response", 3, 0), "timeout")
	if report.attempted != 3 || report.failed != 3 {
		t.Fatalf("report %+v", report)
	}
	for id, types := range byMessage(recorded) {
		if len(types) != 3 || types[2] != events.Timeout {
			t.Fatalf("message %s: %v", id, types)
		}
	}
	recorded, report = runSender(t, testScenario("one_way", 3, 0), "error")
	if report.failed != 3 {
		t.Fatalf("report %+v", report)
	}
	for _, event := range recorded {
		if event.EventType == events.Error && event.Metadata["error"] != "connection refused" {
			t.Fatalf("ERROR metadata %v", event.Metadata)
		}
		if event.EventType == events.Timeout && event.Metadata != nil {
			t.Fatalf("TIMEOUT carries no metadata: %v", event.Metadata)
		}
	}
	if types := byMessage(recorded); len(types) != 3 {
		t.Fatalf("messages %v", types)
	}
}
