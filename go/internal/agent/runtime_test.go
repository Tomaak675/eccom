package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"eccom/internal/events"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitHealthy(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("receiver never became healthy")
}

func TestRunSenderAgainstRunReceiverWritesTheArtifactsOfBothRoles(t *testing.T) {
	t.Cleanup(func(previous int) func() { return func() { runtime.GOMAXPROCS(previous) } }(runtime.GOMAXPROCS(0)))
	dir := t.TempDir()
	endpointPort, controlPort := freePort(t), freePort(t)
	scenarioPath := filepath.Join(dir, "scenario.yaml")
	scenario := fmt.Sprintf(`name: go_sender_e2e
interaction: one_way
payload: {size_bytes: 16, content: random, seed: 1}
load: {messages_per_second: 400, total_messages: 40, concurrency: 4}
sender: {implementation: go, threads: 2}
receiver: {control_port: %d, control_bind_host: 127.0.0.1}
experiment: {warmup_s: 0.1, cooldown_s: 0.1}
instrumentation: {system_metrics: true, system_metrics_interval_s: 0.1, baseline_s: 0.1}
protocol:
  type: http
  http:
    endpoint: {host: cloud, port: %d}
    server: {bind_host: 127.0.0.1}
`, controlPort, endpointPort)
	if err := os.WriteFile(scenarioPath, []byte(scenario), 0o644); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(dir, "run")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiverDone := make(chan error, 1)
	go func() { receiverDone <- RunReceiver(ctx, scenarioPath, runDir, "127.0.0.1") }()
	control := fmt.Sprintf("127.0.0.1:%d", controlPort)
	waitHealthy(t, "http://"+control+"/health")

	if err := RunSender(ctx, scenarioPath, runDir, control, "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if runtime.GOMAXPROCS(0) != 2 {
		t.Fatalf("sender.threads should set GOMAXPROCS, got %d", runtime.GOMAXPROCS(0))
	}
	if resp, err := http.Post("http://"+control+"/shutdown", "", nil); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("shutdown: %v %v", resp, err)
	}
	select {
	case err := <-receiverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("receiver did not stop")
	}

	var report map[string]any
	readJSON(t, filepath.Join(runDir, "edge-load-report.json"), &report)
	if report["messages_attempted"] != float64(40) || report["messages_completed"] != float64(40) || report["steps"] != nil {
		t.Fatalf("load report %v", report)
	}
	var environment map[string]any
	readJSON(t, filepath.Join(runDir, "edge-environment.json"), &environment)
	if environment["implementation"] != "go" || environment["go_max_procs"] != float64(2) || environment["host_role"] != "edge" {
		t.Fatalf("environment %v", environment)
	}
	var clockSync map[string]any
	readJSON(t, filepath.Join(runDir, "edge-clock-sync.json"), &clockSync)
	if clockSync["method"] != "control-endpoint-ntp-probe" || clockSync["after"] == nil {
		t.Fatalf("clock sync %v", clockSync)
	}
	for _, name := range []string{"edge-system-metrics.jsonl", "cloud-system-metrics.jsonl", "scenario.yaml", "cloud-environment.json"} {
		if info, err := os.Stat(filepath.Join(runDir, name)); err != nil || info.Size() == 0 {
			t.Fatalf("%s missing or empty: %v", name, err)
		}
	}

	sent := readEvents(t, filepath.Join(runDir, "edge-events.jsonl"))
	measurement := map[string]bool{}
	for _, event := range sent {
		if event.EventType == events.SendStarted && event.Phase != nil && *event.Phase == "measurement" {
			measurement[event.CorrelationID] = true
		}
	}
	if len(measurement) != 40 {
		t.Fatalf("expected 40 measurement sends, got %d", len(measurement))
	}
	received := 0
	for _, event := range readEvents(t, filepath.Join(runDir, "cloud-events.jsonl")) {
		if event.EventType == events.MessageReceived && measurement[event.CorrelationID] {
			received++
		}
	}
	if received != 40 {
		t.Fatalf("the receiver saw %d of the 40 measurement messages", received)
	}
}

func readJSON(t *testing.T, path string, into any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}
