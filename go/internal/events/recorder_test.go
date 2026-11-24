package events

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCloseWritesLinesShapedLikeThePythonRecorder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloud-events.jsonl")
	r := NewRecorder("cloud", path)
	r.Record(MessageReceived, "abc", 7, map[string]any{"size_bytes": 32})
	r.Record(HandlerStarted, "abc", 7, nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %q", lines)
	}
	// Same keys in the same order as json.dumps(asdict(MeasurementEvent)).
	first := regexp.MustCompile(`^\{"event_type":"MESSAGE_RECEIVED","correlation_id":"abc","sequence_number":7,` +
		`"host":"cloud","phase":null,"monotonic_ns":\d+,"wall_time_ns":\d+,"metadata":\{"size_bytes":32\}\}$`)
	if !first.MatchString(lines[0]) {
		t.Errorf("unexpected first line: %s", lines[0])
	}
	if !strings.HasPrefix(lines[1], `{"event_type":"HANDLER_STARTED",`) || !strings.HasSuffix(lines[1], `"metadata":null}`) {
		t.Errorf("unexpected second line: %s", lines[1])
	}
	var decoded Event
	if err := json.Unmarshal([]byte(lines[1]), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.MonotonicNs <= 0 || decoded.WallTimeNs <= 0 || decoded.Phase != nil {
		t.Errorf("clocks or phase wrong: %+v", decoded)
	}
}

func TestSetPhaseTagsSubsequentEvents(t *testing.T) {
	r := NewRecorder("edge", filepath.Join(t.TempDir(), "edge-events.jsonl"))
	r.Record(PayloadCreated, "a", 0, nil)
	r.SetPhase("warmup")
	r.Record(SendStarted, "a", 0, nil)
	r.SetPhase("measurement")
	r.Record(SendCompleted, "a", 0, nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(r.Path())
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], `"phase":null`) ||
		!strings.Contains(lines[1], `"phase":"warmup"`) || !strings.Contains(lines[2], `"phase":"measurement"`) {
		t.Fatalf("phases: %q", lines)
	}
}

func TestRecordFromManyGoroutines(t *testing.T) {
	r := NewRecorder("cloud", filepath.Join(t.TempDir(), "events.jsonl"))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				r.Record(HandlerCompleted, "x", int64(i), nil)
			}
		}()
	}
	wg.Wait()
	if r.Len() != 8000 {
		t.Fatalf("expected 8000 events, got %d", r.Len())
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(r.Path())
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 8000 {
		t.Fatalf("expected 8000 lines on disk, got %d", lines)
	}
}

func TestEventsReachTheDiskBeforeClose(t *testing.T) {
	flushInterval = 10 * time.Millisecond
	defer func() { flushInterval = time.Second }()
	path := filepath.Join(t.TempDir(), "cloud-events.jsonl")
	r := NewRecorder("cloud", path)
	for i := 0; i < 100; i++ {
		r.Record(MessageReceived, "x", int64(i), nil)
	}
	deadline := time.Now().Add(2 * time.Second)
	var lines int
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			lines = strings.Count(string(data), "\n")
			if lines == 100 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lines != 100 {
		t.Fatalf("expected 100 lines on disk before Close, got %d", lines)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordAfterCloseIsDroppedNotPanicking(t *testing.T) {
	r := NewRecorder("cloud", filepath.Join(t.TempDir(), "events.jsonl"))
	r.Record(MessageReceived, "x", 0, nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r.Record(MessageReceived, "y", 1, nil)
	if err := r.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
	if r.Len() != 1 {
		t.Fatalf("expected the late event to be dropped, Len = %d", r.Len())
	}
}

func TestCloseReportsAnUnwritableDirectory(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRecorder("cloud", filepath.Join(blocker, "events.jsonl")) // parent is a file
	r.Record(MessageReceived, "x", 0, nil)
	if err := r.Close(); err == nil {
		t.Fatal("expected an error for an unwritable path")
	}
}
