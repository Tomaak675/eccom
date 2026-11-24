package control

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestEndpointsMatchThePythonControlServer(t *testing.T) {
	s := New("127.0.0.1", 0)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	base := "http://" + s.Addr().String()

	var health map[string]string
	getJSON(t, base+HealthPath, &health)
	if health["status"] != "ok" {
		t.Errorf("health: %v", health)
	}

	var clock map[string]int64
	getJSON(t, base+ClockPath, &clock)
	if clock["receive_wall_time_ns"] <= 0 || clock["transmit_wall_time_ns"] < clock["receive_wall_time_ns"] {
		t.Errorf("clock: %v", clock)
	}

	if resp, err := http.Get(base + ShutdownPath); err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /shutdown should be 405, got %v %v", resp, err)
	}
	select {
	case <-s.ShutdownRequested():
		t.Fatal("shutdown requested before POST")
	default:
	}

	resp, err := http.Post(base+ShutdownPath, "", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /shutdown: %v %v", resp, err)
	}
	var shutdown map[string]string
	json.NewDecoder(resp.Body).Decode(&shutdown)
	if shutdown["status"] != "shutting down" {
		t.Errorf("shutdown body: %v", shutdown)
	}
	select {
	case <-s.ShutdownRequested():
	case <-time.After(time.Second):
		t.Fatal("shutdown channel not closed")
	}
	s.RequestShutdown() // idempotent
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
}
