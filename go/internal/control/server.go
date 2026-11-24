// Package control is the receiver's out-of-band HTTP endpoint for the
// orchestrator: readiness, graceful shutdown and NTP-style clock probes.
// Same paths and JSON bodies as eccom/agents/control.py; only contacted
// outside the measurement window.
package control

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync"

	"eccom/internal/clock"
)

const (
	HealthPath   = "/health"
	ShutdownPath = "/shutdown"
	ClockPath    = "/clock"
)

// Server serves the control endpoint and exposes the shutdown request.
type Server struct {
	addr     string
	server   *http.Server
	listener net.Listener
	shutdown chan struct{}
	once     sync.Once
}

// New prepares a control server bound to bindHost:port (port 0 picks a free one).
func New(bindHost string, port int) *Server {
	mux := http.NewServeMux()
	s := &Server{
		addr:     net.JoinHostPort(bindHost, strconv.Itoa(port)),
		server:   &http.Server{Handler: mux},
		shutdown: make(chan struct{}),
	}
	mux.HandleFunc("GET "+HealthPath, s.handleHealth)
	mux.HandleFunc("POST "+ShutdownPath, s.handleShutdown)
	mux.HandleFunc("GET "+ClockPath, s.handleClock)
	return s
}

// Start binds the listener and serves in the background.
// Returns the listen error.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	go s.server.Serve(listener)
	return nil
}

// Addr is the bound address, valid after Start.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// ShutdownRequested is closed once a shutdown was asked for.
func (s *Server) ShutdownRequested() <-chan struct{} {
	return s.shutdown
}

// RequestShutdown asks for a shutdown from outside the HTTP path (signals).
func (s *Server) RequestShutdown() {
	s.once.Do(func() { close(s.shutdown) })
}

// Stop closes the listener and waits for in-flight requests until ctx ends.
// Returns the error of http.Server.Shutdown.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleShutdown(w http.ResponseWriter, _ *http.Request) {
	s.RequestShutdown()
	writeJSON(w, map[string]string{"status": "shutting down"})
}

func (s *Server) handleClock(w http.ResponseWriter, _ *http.Request) {
	receive := clock.WallNs()
	writeJSON(w, map[string]int64{
		"receive_wall_time_ns":  receive,
		"transmit_wall_time_ns": clock.WallNs(),
	})
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(body)
}
