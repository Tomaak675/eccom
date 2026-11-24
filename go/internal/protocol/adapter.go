// Package protocol holds the eccom protocol adapters, HTTP (net/http) and
// MQTT v5 (paho.golang), in both roles like eccom/protocols/*.py: the
// sender side sends and waits for replies, the receiver side listens.
// Correlation metadata travels out of band (HTTP headers, MQTT v5
// properties) and the payload bytes arrive unchanged. The adapters record
// only wire-boundary events (SEND_COMPLETED, MESSAGE_RECEIVED,
// REPLY_SEND_STARTED); the agents record the interaction-level ones.
package protocol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
)

// Message is one payload with its correlation metadata.
type Message struct {
	CorrelationID  string
	SequenceNumber int64
	Payload        []byte
}

// Handler processes a one-way message.
type Handler func(Message)

// ReplyHandler processes a request and returns the reply payload.
type ReplyHandler func(Message) []byte

// ErrTimeout marks a send or reply that missed its deadline
// (ProtocolTimeoutError); every other error is a protocol failure.
var ErrTimeout = errors.New("timed out")

// Adapter is one protocol in either role.
type Adapter interface {
	// Start acquires the sender's connections; Send and SendAndWaitReply
	// need it, the Listen methods do not.
	Start() error
	// Send sends a one-way message and records SEND_COMPLETED once the
	// transport acknowledged it.
	Send(m Message) error
	// SendAndWaitReply sends a request and waits for the correlated reply.
	// Returns an error wrapping ErrTimeout when none arrives within timeout.
	SendAndWaitReply(m Message, timeout time.Duration) (Message, error)
	// Listen serves one-way messages, acknowledging before h runs.
	Listen(h Handler) error
	// ListenWithReply serves request/response: h's result is sent back.
	ListenWithReply(h ReplyHandler) error
	// Stop releases connections and servers, waiting for in-flight
	// handlers until ctx ends.
	Stop(ctx context.Context) error
}

// New builds the adapter for p; baseDir resolves TLS file paths.
// Returns an error for an unknown protocol type.
func New(p config.Protocol, baseDir string, rec *events.Recorder) (Adapter, error) {
	switch p.Type {
	case "http":
		return newHTTPAdapter(p.HTTP, p.TLS, baseDir, rec, p.TCPNoDelay), nil
	case "mqtt":
		return newMQTTAdapter(p.MQTT, p.TLS, baseDir, rec, p.TCPNoDelay), nil
	}
	return nil, fmt.Errorf("unknown protocol type: %q", p.Type)
}
