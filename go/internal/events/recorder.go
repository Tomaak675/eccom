// Package events is the Go counterpart of eccom/core/events.py: the
// measurement event model and the per-host recorder that writes events as
// JSON lines, with the same keys and value types so the Python analysis
// reads both implementations alike. The recorder streams: a writer goroutine
// drains a channel into a buffered file, so memory stays flat however long
// the run.
package events

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"eccom/internal/clock"
)

// Type names a measurement event (EventType in events.py).
type Type string

const (
	PayloadCreated   Type = "PAYLOAD_CREATED"
	SendStarted      Type = "SEND_STARTED"
	SendCompleted    Type = "SEND_COMPLETED"
	MessageReceived  Type = "MESSAGE_RECEIVED"
	HandlerStarted   Type = "HANDLER_STARTED"
	HandlerCompleted Type = "HANDLER_COMPLETED"
	ReplySendStarted Type = "REPLY_SEND_STARTED"
	ReplyReceived    Type = "REPLY_RECEIVED"
	Timeout          Type = "TIMEOUT"
	Error            Type = "ERROR"
)

// Event mirrors MeasurementEvent; the field order is the JSON key order.
// Phase is the experiment phase as the sender sees it (SetPhase) and null
// on the receiver, where the analysis joins it by correlation id.
type Event struct {
	EventType      Type           `json:"event_type"`
	CorrelationID  string         `json:"correlation_id"`
	SequenceNumber int64          `json:"sequence_number"`
	Host           string         `json:"host"`
	Phase          *string        `json:"phase"`
	MonotonicNs    int64          `json:"monotonic_ns"`
	WallTimeNs     int64          `json:"wall_time_ns"`
	Metadata       map[string]any `json:"metadata"`
}

// queueSize bounds the events waiting for the writer; a full queue makes
// Record block, which is backpressure on a disk slower than the network
// rather than silent loss.
const queueSize = 1 << 16

// flushInterval bounds how much a killed process loses: the writer flushes
// its buffer to the OS at least this often.
var flushInterval = time.Second

// Recorder collects events from any goroutine; the hot path is two clock
// reads and one channel send. The writer goroutine owns the file.
type Recorder struct {
	host string
	path string

	queue chan Event
	done  chan struct{}
	count atomic.Int64
	phase atomic.Pointer[string]

	mu     sync.RWMutex // guards closed against a Record racing Close
	closed bool
	err    error // the writer's first file error, read after done
}

// NewRecorder records events for host into the JSONL file at path, which
// the writer goroutine creates on its first event.
func NewRecorder(host, path string) *Recorder {
	r := &Recorder{
		host:  host,
		path:  path,
		queue: make(chan Event, queueSize),
		done:  make(chan struct{}),
	}
	go r.write()
	return r
}

// SetPhase tags subsequent events with an experiment phase (sender side).
func (r *Recorder) SetPhase(phase string) {
	r.phase.Store(&phase)
}

// Record stamps one event with the current clocks and hands it to the
// writer. Events recorded after Close are dropped.
func (r *Recorder) Record(t Type, correlationID string, sequence int64, metadata map[string]any) {
	event := Event{
		EventType:      t,
		CorrelationID:  correlationID,
		SequenceNumber: sequence,
		Host:           r.host,
		Phase:          r.phase.Load(),
		MonotonicNs:    clock.MonotonicNs(),
		WallTimeNs:     clock.WallNs(),
		Metadata:       metadata,
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}
	r.count.Add(1)
	r.queue <- event
}

// Path is the JSONL file the recorder writes.
func (r *Recorder) Path() string {
	return r.path
}

// Len is the number of events recorded so far (handed to the writer, not
// necessarily on disk yet).
func (r *Recorder) Len() int {
	return int(r.count.Load())
}

// Close stops accepting events, waits for the writer to drain and flush,
// and closes the file.
// Returns the first file error of the whole run.
func (r *Recorder) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
	}
	r.mu.Unlock()
	<-r.done
	return r.err
}

// write is the writer goroutine: it creates the file, encodes every queued
// event, flushes periodically and on close, and keeps the first error.
func (r *Recorder) write() {
	defer close(r.done)
	file, err := r.create()
	if err != nil {
		r.err = err
		for range r.queue { // drain so Record never blocks on a dead writer
		}
		return
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case event, ok := <-r.queue:
			if !ok {
				r.fail(writer.Flush())
				r.fail(file.Close())
				return
			}
			r.fail(encoder.Encode(event))
		case <-ticker.C:
			r.fail(writer.Flush())
		}
	}
}

func (r *Recorder) create() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(r.path)
}

// fail keeps the first error; later ones are consequences of it.
func (r *Recorder) fail(err error) {
	if err != nil && r.err == nil {
		r.err = err
	}
}
