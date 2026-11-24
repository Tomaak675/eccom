package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
)

const (
	CorrelationHeader = "X-Correlation-ID"
	SequenceHeader    = "X-Sequence-Number"
	MessagesPath      = "/messages"

	maxRequestBodyBytes = 64 << 20
	// aiohttp's keepalive_timeout: an idle pooled connection is closed
	// after this long on the Python side too.
	idleConnTimeout = 15 * time.Second
	// With max_connections 0 the pool is unbounded, as aiohttp's is; Go's
	// default would keep only two idle connections and churn the rest.
	unboundedIdleConns = math.MaxInt32
)

// httpAdapter is eccom/protocols/http.py: an HTTP/1.1 client on the sender
// (h2 off, compression off, one pooled connection per in-flight request)
// and a plain HTTP/1.1 server with one POST route on the receiver. Go turns
// Nagle off on every socket by default; tcp_nodelay: false puts it back on
// the client's dials and the server's accepted connections.
type httpAdapter struct {
	cfg     config.HTTP
	tls     config.TLS
	baseDir string
	rec     *events.Recorder
	noDelay bool

	url    string
	client *http.Client

	server   *http.Server
	listener net.Listener
	oneWay   Handler
	reply    ReplyHandler
	handlers sync.WaitGroup
}

func newHTTPAdapter(cfg config.HTTP, t config.TLS, baseDir string, rec *events.Recorder, noDelay bool) *httpAdapter {
	scheme := "http"
	if t.Enabled {
		scheme = "https"
	}
	return &httpAdapter{
		cfg: cfg, tls: t, baseDir: baseDir, rec: rec, noDelay: noDelay,
		url: scheme + "://" + net.JoinHostPort(cfg.Endpoint.Host, strconv.Itoa(cfg.Endpoint.Port)) + MessagesPath,
	}
}

// --- sender side -----------------------------------------------------------

func (a *httpAdapter) Start() error {
	idle := a.cfg.Client.MaxConnections
	if idle == 0 {
		idle = unboundedIdleConns
	}
	transport := &http.Transport{
		DialContext:         a.dial,
		MaxConnsPerHost:     a.cfg.Client.MaxConnections,
		MaxIdleConnsPerHost: idle,
		IdleConnTimeout:     idleConnTimeout,
		DisableKeepAlives:   !a.cfg.Client.KeepAlive,
		DisableCompression:  true,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	if a.tls.Enabled {
		tlsConfig, err := clientTLSConfig(a.tls, a.baseDir)
		if err != nil {
			return err
		}
		transport.TLSClientConfig = tlsConfig
	}
	a.client = &http.Client{Transport: transport}
	return nil
}

func (a *httpAdapter) dial(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn.(*net.TCPConn).SetNoDelay(a.noDelay)
	return conn, nil
}

func (a *httpAdapter) Send(m Message) error {
	status, _, err := a.post(m, config.Seconds(a.cfg.Client.TimeoutS))
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("unexpected HTTP status %d", status)
	}
	// One-way: "send completed" means the server's 204 arrived, the
	// transport-level acknowledgement; the handler runs asynchronously.
	a.rec.Record(events.SendCompleted, m.CorrelationID, m.SequenceNumber, nil)
	return nil
}

func (a *httpAdapter) SendAndWaitReply(m Message, timeout time.Duration) (Message, error) {
	status, body, err := a.post(m, timeout)
	if err != nil {
		return Message{}, err
	}
	if status != http.StatusOK {
		return Message{}, fmt.Errorf("unexpected HTTP status %d", status)
	}
	// HTTP correlates request and response on the connection, so the
	// original correlation id is authoritative.
	return Message{CorrelationID: m.CorrelationID, SequenceNumber: m.SequenceNumber, Payload: body}, nil
}

// post sends m and reads the whole response within timeout.
// Returns the status and body, or an error wrapping ErrTimeout when the
// deadline passed.
func (a *httpAdapter) post(m Message, timeout time.Duration) (int, []byte, error) {
	if a.client == nil {
		return 0, nil, errors.New("adapter not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(m.Payload))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set(CorrelationHeader, m.CorrelationID)
	request.Header.Set(SequenceHeader, strconv.FormatInt(m.SequenceNumber, 10))
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := a.client.Do(request)
	if err != nil {
		return 0, nil, classify(err, timeout)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, classify(err, timeout)
	}
	return response.StatusCode, body, nil
}

func classify(err error, timeout time.Duration) error {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return fmt.Errorf("no response within %s: %w", timeout, ErrTimeout)
	}
	return fmt.Errorf("HTTP request failed: %w", err)
}

// --- receiver side ---------------------------------------------------------

func (a *httpAdapter) Listen(h Handler) error {
	a.oneWay = h
	return a.start()
}

func (a *httpAdapter) ListenWithReply(h ReplyHandler) error {
	a.reply = h
	return a.start()
}

// Addr is the bound address, valid after Listen/ListenWithReply.
func (a *httpAdapter) Addr() net.Addr {
	return a.listener.Addr()
}

func (a *httpAdapter) start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+MessagesPath, a.handle)
	a.server = &http.Server{
		Handler:      mux,
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(a.cfg.Server.BindHost, strconv.Itoa(a.cfg.Endpoint.Port)))
	if err != nil {
		return err
	}
	if !a.noDelay {
		listener = nagleListener{listener}
	}
	scheme := "HTTP"
	if a.tls.Enabled {
		tlsConfig, err := serverTLSConfig(a.tls, a.baseDir)
		if err != nil {
			listener.Close()
			return err
		}
		listener = tls.NewListener(listener, tlsConfig)
		scheme = "HTTPS"
	}
	a.listener = listener
	go a.server.Serve(listener)
	slog.Info(scheme+" server listening", "address", listener.Addr().String())
	return nil
}

func (a *httpAdapter) Stop(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	if a.server == nil {
		return nil
	}
	err := a.server.Shutdown(ctx)
	done := make(chan struct{})
	go func() {
		a.handlers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return err
}

func (a *httpAdapter) handle(w http.ResponseWriter, r *http.Request) {
	correlationID := r.Header.Get(CorrelationHeader)
	if correlationID == "" {
		http.Error(w, "missing "+CorrelationHeader+" header", http.StatusBadRequest)
		return
	}
	sequence := int64(-1)
	if raw := r.Header.Get(SequenceHeader); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid "+SequenceHeader+" header", http.StatusBadRequest)
			return
		}
		sequence = parsed
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	a.rec.Record(events.MessageReceived, correlationID, sequence, map[string]any{"size_bytes": len(payload)})
	message := Message{CorrelationID: correlationID, SequenceNumber: sequence, Payload: payload}
	if a.reply != nil {
		reply := a.reply(message)
		a.rec.Record(events.ReplySendStarted, correlationID, sequence, map[string]any{"size_bytes": len(reply)})
		w.Header().Set(CorrelationHeader, correlationID)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(reply)
		return
	}
	// One-way semantics: acknowledge receipt immediately, process
	// asynchronously (mirrors MQTT, where the broker acks and the
	// subscriber processes independently).
	a.handlers.Add(1)
	go func() {
		defer a.handlers.Done()
		a.oneWay(message)
	}()
	w.WriteHeader(http.StatusNoContent)
}

// nagleListener re-enables Nagle's algorithm on accepted connections.
type nagleListener struct {
	net.Listener
}

func (l nagleListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	conn.(*net.TCPConn).SetNoDelay(false)
	return conn, nil
}
