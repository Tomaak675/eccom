package protocol

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
)

// newTestMQTTSender builds the sender half on the receiver's base topic.
func newTestMQTTSender(t *testing.T, cfg config.MQTT) (*mqttAdapter, *events.Recorder) {
	t.Helper()
	rec := events.NewRecorder("edge", filepath.Join(t.TempDir(), "edge-events.jsonl"))
	t.Cleanup(func() { _ = rec.Close() })
	a := newMQTTAdapter(cfg, config.TLS{}, "", rec, true)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return a, rec
}

func TestMQTTSendRecordsSendCompletedWithTheQoS(t *testing.T) {
	host, port := brokerAddress(t)
	receiver, _ := newTestMQTTAdapter(t, host, port)
	handled := make(chan Message, 1)
	if err := receiver.Listen(func(m Message) { handled <- m }); err != nil {
		t.Fatal(err)
	}
	defer receiver.Stop(context.Background())
	sender, rec := newTestMQTTSender(t, receiver.cfg)

	if err := sender.Send(Message{CorrelationID: "corr-s1", SequenceNumber: 4, Payload: []byte("fire")}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-handled:
		if m.CorrelationID != "corr-s1" || m.SequenceNumber != 4 || string(m.Payload) != "fire" {
			t.Errorf("message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 1 || recorded[0].EventType != events.SendCompleted || recorded[0].Metadata["qos"] != float64(1) {
		t.Fatalf("events %+v", recorded)
	}
}

func TestMQTTQoS0SendCompletesOnceWritten(t *testing.T) {
	host, port := brokerAddress(t)
	receiver, _ := newTestMQTTAdapter(t, host, port)
	cfg := receiver.cfg
	cfg.Client.QoS = 0
	sender, rec := newTestMQTTSender(t, cfg)
	if err := sender.Send(Message{CorrelationID: "corr-q0", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 1 || recorded[0].Metadata["qos"] != float64(0) {
		t.Fatalf("events %+v", recorded)
	}
}

func TestMQTTSendAndWaitReplyGetsTheReplyThroughTheBroker(t *testing.T) {
	host, port := brokerAddress(t)
	receiver, receiverRec := newTestMQTTAdapter(t, host, port)
	if err := receiver.ListenWithReply(func(m Message) []byte { return append([]byte("re:"), m.Payload...) }); err != nil {
		t.Fatal(err)
	}
	sender, rec := newTestMQTTSender(t, receiver.cfg)

	reply, err := sender.SendAndWaitReply(Message{CorrelationID: "corr-rr", SequenceNumber: 2, Payload: []byte("ping")}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if reply.CorrelationID != "corr-rr" || reply.SequenceNumber != 2 || string(reply.Payload) != "re:ping" {
		t.Fatalf("reply %+v", reply)
	}
	if err := receiver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recorded := recordedEvents(t, rec, rec.Path()); len(recorded) != 1 || recorded[0].EventType != events.SendCompleted {
		t.Fatalf("sender events %+v", recorded)
	}
	recorded := recordedEvents(t, receiverRec, receiverRec.Path())
	if len(recorded) != 2 || recorded[0].EventType != events.MessageReceived || recorded[1].EventType != events.ReplySendStarted {
		t.Fatalf("receiver events %+v", recorded)
	}
}

func TestMQTTRequestWithoutAReceiverTimesOut(t *testing.T) {
	host, port := brokerAddress(t)
	receiver, _ := newTestMQTTAdapter(t, host, port) // never listens: nobody answers on its base topic
	sender, rec := newTestMQTTSender(t, receiver.cfg)
	_, err := sender.SendAndWaitReply(Message{CorrelationID: "corr-none", Payload: []byte("?")}, 300*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected a timeout, got %v", err)
	}
	// the broker still acknowledged the publish
	if recorded := recordedEvents(t, rec, rec.Path()); len(recorded) != 1 || recorded[0].EventType != events.SendCompleted {
		t.Fatalf("events %+v", recorded)
	}
}
