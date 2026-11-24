package load

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eccom/internal/config"
)

func constant(rate float64, total int64, concurrency int) config.Traffic {
	return config.Traffic{Mode: "constant_rate", MessagesPerSecond: rate, TotalMessages: total, Concurrency: concurrency, Senders: 1}
}

func ramp(rate, increment, stepS, pauseS, durationS float64, concurrency int) config.Traffic {
	return config.Traffic{
		Mode: "ramp", MessagesPerSecond: rate, RateIncrement: increment,
		StepDurationS: stepS, StepPauseS: pauseS, DurationS: durationS, Concurrency: concurrency, Senders: 1,
	}
}

func always(bool) SendOne {
	return func(map[string]any) bool { return true }
}

func TestFixedCountAttemptsExactlyNAcrossLanes(t *testing.T) {
	var calls atomic.Int64
	send := func(map[string]any) bool { calls.Add(1); return true }
	report := New(constant(1000, 25, 4), 0, 3).Run(context.Background(), send)
	if calls.Load() != 25 || report.MessagesAttempted != 25 || report.MessagesCompleted != 25 || report.MessagesFailed != 0 {
		t.Fatalf("calls %d report %+v", calls.Load(), report)
	}
	if report.RequestedMessageRate != 1000 || report.ActualMessageRate <= 0 || report.Steps != nil {
		t.Fatalf("report %+v", report)
	}
}

func TestTotalMessagesNeverOvershootsWithManyLanes(t *testing.T) {
	var calls atomic.Int64
	send := func(map[string]any) bool { calls.Add(1); return true }
	report := New(constant(1e6, 100, 50), 0, 8).Run(context.Background(), send)
	if calls.Load() != 100 || report.MessagesAttempted != 100 {
		t.Fatalf("calls %d attempted %d", calls.Load(), report.MessagesAttempted)
	}
}

func TestConcurrencyCapIsRespected(t *testing.T) {
	var inFlight, maxInFlight atomic.Int64
	send := func(map[string]any) bool {
		n := inFlight.Add(1)
		for {
			seen := maxInFlight.Load()
			if n <= seen || maxInFlight.CompareAndSwap(seen, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return true
	}
	report := New(constant(10_000, 12, 3), 0, 4).Run(context.Background(), send)
	if report.MessagesAttempted != 12 || maxInFlight.Load() > 3 {
		t.Fatalf("attempted %d, max in flight %d", report.MessagesAttempted, maxInFlight.Load())
	}
}

func TestFailuresAreCounted(t *testing.T) {
	var counter atomic.Int64
	send := func(map[string]any) bool { return counter.Add(1)%3 == 0 }
	report := New(constant(1000, 9, 1), 0, 1).Run(context.Background(), send)
	if report.MessagesAttempted != 9 || report.MessagesCompleted != 3 || report.MessagesFailed != 6 {
		t.Fatalf("report %+v", report)
	}
}

func TestDurationModeStopsAndMeasuresRate(t *testing.T) {
	cfg := config.Traffic{Mode: "constant_rate", MessagesPerSecond: 100, DurationS: 0.3, Concurrency: 2, Senders: 1}
	report := New(cfg, 0, 2).Run(context.Background(), always(true))
	// ~30 messages expected; generous slack for coarse OS timers
	if report.MessagesAttempted < 10 || report.MessagesAttempted > 45 || report.SendWindowS < 0.3 || report.ActualMessageRate <= 0 {
		t.Fatalf("report %+v", report)
	}
}

func TestConstantRatePassesNilMetadataAndReportsNoSteps(t *testing.T) {
	var mu sync.Mutex
	var seen []map[string]any
	send := func(m map[string]any) bool { mu.Lock(); seen = append(seen, m); mu.Unlock(); return true }
	report := New(constant(1000, 3, 1), 0, 1).Run(context.Background(), send)
	if len(seen) != 3 || seen[0] != nil || report.Steps != nil {
		t.Fatalf("metadata %v steps %v", seen, report.Steps)
	}
}

func TestRampTagsEverySendWithItsStepAndReportsTheSteps(t *testing.T) {
	var mu sync.Mutex
	rates := map[int]float64{}
	send := func(m map[string]any) bool {
		mu.Lock()
		rates[m["step"].(int)] = m["target_rate"].(float64)
		mu.Unlock()
		return true
	}
	report := New(ramp(100, 100, 0.1, 0, 0.3, 10), 0, 2).Run(context.Background(), send)
	if len(rates) != 3 || rates[0] != 100 || rates[1] != 200 || rates[2] != 300 {
		t.Fatalf("step tags %v", rates)
	}
	if len(report.Steps) != 3 || report.RequestedMessageRate != 100 {
		t.Fatalf("report %+v", report)
	}
	for i, step := range report.Steps {
		if step.Step != i || step.TargetMessageRate != float64(100*(i+1)) || step.MessagesAttempted == 0 || step.DurationS <= 0 || step.PayloadSizeBytes != nil {
			t.Errorf("step %+v", step)
		}
	}
	if report.Steps[2].MessagesAttempted <= report.Steps[0].MessagesAttempted {
		t.Errorf("the rate should rise along the ramp: %+v", report.Steps)
	}
}

func TestPauseSchedulesNothingInTheGapAndEndsTheStep(t *testing.T) {
	start := time.Now()
	var mu sync.Mutex
	var sendTimes []time.Duration
	send := func(map[string]any) bool {
		mu.Lock()
		sendTimes = append(sendTimes, time.Since(start))
		mu.Unlock()
		return true
	}
	report := New(ramp(200, 0, 0.1, 0.1, 0.2, 10), 0, 2).Run(context.Background(), send)
	if len(report.Steps) != 2 {
		t.Fatalf("steps %+v", report.Steps)
	}
	// 2 steps of 0.1 s with one 0.1 s pause between them: ~0.3 s of wall time
	if report.SendWindowS < 0.28 || report.Steps[0].DurationS > 0.15 {
		t.Fatalf("window %.3f steps %+v", report.SendWindowS, report.Steps)
	}
	for _, at := range sendTimes {
		if at > 120*time.Millisecond && at < 195*time.Millisecond {
			t.Fatalf("a send ran inside the pause, at %s", at)
		}
	}
}

func TestStepsTheScheduleNeverReachedAreReportedWithZeroSends(t *testing.T) {
	send := func(map[string]any) bool { time.Sleep(120 * time.Millisecond); return true }
	report := New(ramp(1000, 0, 0.05, 0, 0.15, 1), 0, 1).Run(context.Background(), send)
	if len(report.Steps) != 3 || report.Steps[1].MessagesAttempted != 0 || report.Steps[0].MessagesAttempted != 1 {
		t.Fatalf("steps %+v", report.Steps)
	}
}

func TestSizeMultiplierTagsTheTargetSize(t *testing.T) {
	var mu sync.Mutex
	sizes := map[int]int{}
	send := func(m map[string]any) bool {
		mu.Lock()
		sizes[m["step"].(int)] = m["target_size"].(int)
		mu.Unlock()
		return true
	}
	cfg := config.Traffic{Mode: "ramp", MessagesPerSecond: 100, SizeMultiplier: 2, StepDurationS: 0.05, DurationS: 0.1, Concurrency: 4, Senders: 1}
	report := New(cfg, 10, 1).Run(context.Background(), send)
	if sizes[0] != 10 || sizes[1] != 20 || len(report.Steps) != 2 {
		t.Fatalf("sizes %v steps %+v", sizes, report.Steps)
	}
	if *report.Steps[0].PayloadSizeBytes != 10 || *report.Steps[1].PayloadSizeBytes != 20 || report.Steps[1].TargetMessageRate != 100 {
		t.Fatalf("steps %+v", report.Steps)
	}
}

func TestCancelledContextStopsTheScheduleAndDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	cfg := config.Traffic{Mode: "constant_rate", MessagesPerSecond: 100, DurationS: 10, Concurrency: 2, Senders: 1}
	done := make(chan Report, 1)
	go func() { done <- New(cfg, 0, 2).Run(ctx, always(true)) }()
	select {
	case report := <-done:
		if report.MessagesAttempted == 0 || report.MessagesAttempted != report.MessagesCompleted {
			t.Fatalf("report %+v", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestReportJSONHasThePythonKeysInOrder(t *testing.T) {
	report := New(ramp(1000, 0, 0.02, 0, 0.02, 4), 0, 1).Run(context.Background(), always(true))
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"requested_message_rate", "actual_message_rate", "messages_attempted", "messages_completed",
		"messages_failed", "send_window_s", "total_duration_s", "per_device", "steps"}
	if got := topLevelKeys(t, data); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("report keys %v", got)
	}
	if !strings.Contains(string(data), `"per_device":null`) {
		t.Fatalf("per_device must be null: %s", data)
	}
	stepData, _ := json.Marshal(report.Steps[0])
	wantStep := []string{"step", "target_message_rate", "actual_message_rate", "messages_attempted",
		"messages_completed", "messages_failed", "duration_s", "payload_size_bytes"}
	if got := topLevelKeys(t, stepData); strings.Join(got, ",") != strings.Join(wantStep, ",") {
		t.Fatalf("step keys %v", got)
	}
	constantData, _ := json.Marshal(New(constant(1000, 1, 1), 0, 1).Run(context.Background(), always(true)))
	if !strings.HasSuffix(string(constantData), `"per_device":null,"steps":null}`) {
		t.Fatalf("constant-rate report: %s", constantData)
	}
}

// topLevelKeys lists an object's keys in document order.
func topLevelKeys(t *testing.T, data []byte) []string {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	var keys []string
	depth := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return keys
		}
		switch v := token.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
			} else {
				depth--
			}
		case string:
			if depth == 1 && decoder.More() {
				keys = append(keys, v)
				// skip the value
				var value json.RawMessage
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
