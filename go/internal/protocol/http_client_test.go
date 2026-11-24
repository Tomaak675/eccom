package protocol

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
)

// newTestHTTPSender builds the sender half pointed at address.
func newTestHTTPSender(t *testing.T, address string, tlsConfig config.TLS, client config.HTTPClient) (*httpAdapter, *events.Recorder) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	rec := events.NewRecorder("edge", filepath.Join(t.TempDir(), "edge-events.jsonl"))
	t.Cleanup(func() { _ = rec.Close() })
	cfg := config.HTTP{Endpoint: config.Endpoint{Host: host, Port: port}, Client: client}
	a := newHTTPAdapter(cfg, tlsConfig, testCertsDir, rec, true)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return a, rec
}

func defaultClient() config.HTTPClient {
	return config.HTTPClient{KeepAlive: true, TimeoutS: 5}
}

func TestSendRecordsSendCompletedOnceThe204Arrived(t *testing.T) {
	receiver, _ := newTestHTTPAdapter(t, config.TLS{}, true)
	handled := make(chan Message, 1)
	if err := receiver.Listen(func(m Message) { handled <- m }); err != nil {
		t.Fatal(err)
	}
	defer receiver.Stop(context.Background())
	sender, rec := newTestHTTPSender(t, receiver.Addr().String(), config.TLS{}, defaultClient())

	if err := sender.Send(Message{CorrelationID: "c-1", SequenceNumber: 7, Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-handled:
		if m.CorrelationID != "c-1" || m.SequenceNumber != 7 || string(m.Payload) != "hello" {
			t.Errorf("message %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 1 || recorded[0].EventType != events.SendCompleted || recorded[0].CorrelationID != "c-1" || recorded[0].Metadata != nil {
		t.Fatalf("events %+v", recorded)
	}
}

func TestSendAndWaitReplyReturnsTheBodyUnderTheOriginalCorrelationID(t *testing.T) {
	receiver, _ := newTestHTTPAdapter(t, config.TLS{}, true)
	if err := receiver.ListenWithReply(func(m Message) []byte { return append([]byte("re:"), m.Payload...) }); err != nil {
		t.Fatal(err)
	}
	defer receiver.Stop(context.Background())
	sender, rec := newTestHTTPSender(t, receiver.Addr().String(), config.TLS{}, defaultClient())

	reply, err := sender.SendAndWaitReply(Message{CorrelationID: "c-2", SequenceNumber: 1, Payload: []byte("ping")}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if reply.CorrelationID != "c-2" || reply.SequenceNumber != 1 || string(reply.Payload) != "re:ping" {
		t.Fatalf("reply %+v", reply)
	}
	if recorded := recordedEvents(t, rec, rec.Path()); len(recorded) != 0 {
		t.Fatalf("the adapter records no sender event in request/response, got %+v", recorded)
	}
}

func TestDeadlinesAreReportedAsTimeouts(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer slow.Close()
	sender, _ := newTestHTTPSender(t, slow.Listener.Addr().String(), config.TLS{}, config.HTTPClient{KeepAlive: true, TimeoutS: 0.05})

	if err := sender.Send(Message{CorrelationID: "c"}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("one-way: %v", err)
	}
	if _, err := sender.SendAndWaitReply(Message{CorrelationID: "c"}, 50*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("request/response: %v", err)
	}
}

func TestUnexpectedStatusesAreErrorsNotTimeouts(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	sender, rec := newTestHTTPSender(t, failing.Listener.Addr().String(), config.TLS{}, defaultClient())
	if err := sender.Send(Message{CorrelationID: "c"}); err == nil || errors.Is(err, ErrTimeout) {
		t.Fatalf("one-way: %v", err)
	}
	if recorded := recordedEvents(t, rec, rec.Path()); len(recorded) != 0 {
		t.Fatalf("no SEND_COMPLETED on a failed send, got %+v", recorded)
	}

	oneWayOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer oneWayOnly.Close()
	sender2, _ := newTestHTTPSender(t, oneWayOnly.Listener.Addr().String(), config.TLS{}, defaultClient())
	if _, err := sender2.SendAndWaitReply(Message{CorrelationID: "c"}, time.Second); err == nil || errors.Is(err, ErrTimeout) {
		t.Fatalf("request/response wants a 200: %v", err)
	}
}

// countingServer answers 204 after delay and counts the TCP connections opened to it.
func countingServer(t *testing.T, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, &connections
}

func TestKeepAliveOffOpensAConnectionPerRequest(t *testing.T) {
	server, connections := countingServer(t, 0)
	noKeepAlive, _ := newTestHTTPSender(t, server.Listener.Addr().String(), config.TLS{}, config.HTTPClient{KeepAlive: false, TimeoutS: 5})
	for i := 0; i < 3; i++ {
		if err := noKeepAlive.Send(Message{CorrelationID: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	if connections.Load() != 3 {
		t.Fatalf("expected 3 connections without keep-alive, got %d", connections.Load())
	}

	server2, connections2 := countingServer(t, 0)
	pooled, _ := newTestHTTPSender(t, server2.Listener.Addr().String(), config.TLS{}, defaultClient())
	for i := 0; i < 3; i++ {
		if err := pooled.Send(Message{CorrelationID: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	if connections2.Load() != 1 {
		t.Fatalf("expected one reused connection with keep-alive, got %d", connections2.Load())
	}
}

func TestMaxConnectionsBoundsThePoolAndZeroDoesNot(t *testing.T) {
	sendConcurrently := func(sender *httpAdapter, n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sender.Send(Message{CorrelationID: "c"}); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	}
	bounded, connections := countingServer(t, 30*time.Millisecond)
	sender, _ := newTestHTTPSender(t, bounded.Listener.Addr().String(), config.TLS{}, config.HTTPClient{KeepAlive: true, TimeoutS: 5, MaxConnections: 2})
	sendConcurrently(sender, 6)
	if connections.Load() > 2 {
		t.Fatalf("max_connections 2 opened %d connections", connections.Load())
	}

	unbounded, connections2 := countingServer(t, 30*time.Millisecond)
	sender2, _ := newTestHTTPSender(t, unbounded.Listener.Addr().String(), config.TLS{}, defaultClient())
	sendConcurrently(sender2, 6)
	if connections2.Load() < 3 {
		t.Fatalf("an unbounded pool should open a connection per in-flight request, got %d", connections2.Load())
	}
	// and keep them: a second burst reuses the idle ones instead of churning
	before := connections2.Load()
	sendConcurrently(sender2, 6)
	if connections2.Load() > before+1 {
		t.Fatalf("idle connections were not kept: %d then %d", before, connections2.Load())
	}
}

func TestTLSClientVerifiesTheServerAgainstTheCAFile(t *testing.T) {
	serverTLS := config.TLS{Enabled: true, CertFile: "server.pem", KeyFile: "server-key.pem"}
	receiver, _ := newTestHTTPAdapter(t, serverTLS, true)
	if err := receiver.Listen(func(Message) {}); err != nil {
		t.Fatal(err)
	}
	defer receiver.Stop(context.Background())

	verified, _ := newTestHTTPSender(t, receiver.Addr().String(), config.TLS{Enabled: true, CertFile: "server.pem", KeyFile: "server-key.pem", CAFile: "server.pem"}, defaultClient())
	if err := verified.Send(Message{CorrelationID: "c"}); err != nil {
		t.Fatalf("verified: %v", err)
	}
	unverified, _ := newTestHTTPSender(t, receiver.Addr().String(), serverTLS, defaultClient())
	if err := unverified.Send(Message{CorrelationID: "c"}); err != nil {
		t.Fatalf("encrypt-only: %v", err)
	}
}
