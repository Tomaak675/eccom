package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
)

const testCertsDir = "../../../tests/certs"

func newTestHTTPAdapter(t *testing.T, tlsConfig config.TLS, noDelay bool) (*httpAdapter, *events.Recorder) {
	t.Helper()
	rec := events.NewRecorder("cloud", filepath.Join(t.TempDir(), "cloud-events.jsonl"))
	t.Cleanup(func() { _ = rec.Close() }) // the writer goroutine holds the file open
	cfg := config.HTTP{
		Endpoint: config.Endpoint{Host: "127.0.0.1", Port: 0},
		Server:   config.HTTPServer{BindHost: "127.0.0.1"},
	}
	return newHTTPAdapter(cfg, tlsConfig, testCertsDir, rec, noDelay), rec
}

func post(t *testing.T, client *http.Client, url string, headers map[string]string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func recordedEvents(t *testing.T, rec *events.Recorder, path string) []events.Event {
	t.Helper()
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil
	}
	var recorded []events.Event
	for _, line := range strings.Split(content, "\n") {
		var event events.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, event)
	}
	return recorded
}

func TestOneWayAcknowledgesWith204AndHandlesAsynchronously(t *testing.T) {
	a, rec := newTestHTTPAdapter(t, config.TLS{}, true)
	handled := make(chan Message, 1)
	if err := a.Listen(func(m Message) { handled <- m }); err != nil {
		t.Fatal(err)
	}
	url := "http://" + a.Addr().String() + MessagesPath
	resp := post(t, http.DefaultClient, url, map[string]string{CorrelationHeader: "c-1", SequenceHeader: "5"}, "hello")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	select {
	case m := <-handled:
		if m.CorrelationID != "c-1" || m.SequenceNumber != 5 || string(m.Payload) != "hello" {
			t.Errorf("message %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 1 || recorded[0].EventType != events.MessageReceived || recorded[0].Metadata["size_bytes"] != float64(5) {
		t.Errorf("events %+v", recorded)
	}
}

func TestRequestResponseReturnsTheReplyWithTheCorrelationHeader(t *testing.T) {
	a, rec := newTestHTTPAdapter(t, config.TLS{}, true)
	err := a.ListenWithReply(func(m Message) []byte { return append([]byte("re:"), m.Payload...) })
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + a.Addr().String() + MessagesPath
	resp := post(t, http.DefaultClient, url, map[string]string{CorrelationHeader: "c-2"}, "ping")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "re:ping" || resp.Header.Get(CorrelationHeader) != "c-2" {
		t.Fatalf("status %d body %q headers %v", resp.StatusCode, body, resp.Header)
	}
	a.Stop(context.Background())
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 2 || recorded[0].EventType != events.MessageReceived || recorded[1].EventType != events.ReplySendStarted {
		t.Fatalf("events %+v", recorded)
	}
	if recorded[0].SequenceNumber != -1 || recorded[1].Metadata["size_bytes"] != float64(7) {
		t.Errorf("events %+v", recorded)
	}
}

func TestMissingCorrelationHeaderIsRejected(t *testing.T) {
	a, _ := newTestHTTPAdapter(t, config.TLS{}, true)
	if err := a.Listen(func(Message) { t.Error("handler must not run") }); err != nil {
		t.Fatal(err)
	}
	defer a.Stop(context.Background())
	url := "http://" + a.Addr().String() + MessagesPath
	if resp := post(t, http.DefaultClient, url, nil, "x"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d", resp.StatusCode)
	}
	if resp := post(t, http.DefaultClient, url, map[string]string{CorrelationHeader: "c", SequenceHeader: "nan"}, "x"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d", resp.StatusCode)
	}
}

func TestTLSServesHTTP1Only(t *testing.T) {
	tlsConfig := config.TLS{Enabled: true, CertFile: "server.pem", KeyFile: "server-key.pem"}
	a, _ := newTestHTTPAdapter(t, tlsConfig, true)
	if err := a.Listen(func(Message) {}); err != nil {
		t.Fatal(err)
	}
	defer a.Stop(context.Background())
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2: true,
	}}
	url := "https://" + a.Addr().String() + MessagesPath
	resp := post(t, client, url, map[string]string{CorrelationHeader: "c-3"}, "secure")
	if resp.StatusCode != http.StatusNoContent || resp.ProtoMajor != 1 {
		t.Fatalf("status %d proto %s", resp.StatusCode, resp.Proto)
	}
}
