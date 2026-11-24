package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var pythonKeys = []string{
	"host", "phase", "wall_time_ns", "monotonic_ns", "system_cpu_percent", "process_cpu_percent",
	"process_cpu_seconds", "system_memory_percent", "process_rss_bytes", "net_bytes_sent",
	"net_bytes_recv", "loop_lag_ms_max",
}

func TestSamplesCarryThePythonKeysAndPhases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloud-system-metrics.jsonl")
	s, err := New("cloud", 20*time.Millisecond, path)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	s.MeasureBaseline(70 * time.Millisecond)
	time.Sleep(70 * time.Millisecond)
	// samples stream to the file as they are taken, not at Stop
	if early, err := os.ReadFile(path); err != nil || strings.Count(string(early), "\n") < 2 {
		t.Fatalf("expected streamed samples before Stop, got %q (%v)", early, err)
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 4 {
		t.Fatalf("expected several samples, got %d", len(lines))
	}
	phases := map[string]bool{}
	for _, line := range lines {
		var sample map[string]any
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			t.Fatal(err)
		}
		for _, key := range pythonKeys {
			if _, ok := sample[key]; !ok {
				t.Fatalf("missing key %q in %s", key, line)
			}
		}
		phases[sample["phase"].(string)] = true
		if sample["process_rss_bytes"].(float64) <= 0 || sample["process_cpu_seconds"].(float64) < 0 {
			t.Errorf("implausible sample %s", line)
		}
	}
	if !phases["baseline"] || !phases["traffic"] {
		t.Errorf("phases %v", phases)
	}
}
