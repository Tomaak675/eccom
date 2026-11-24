// Package agent runs the receiver role: the handler semantics of
// eccom/agents/receiver.py and the process lifecycle of
// eccom/agents/runtime.py::_run_receiver.
package agent

import (
	"time"

	"eccom/internal/events"
	"eccom/internal/protocol"
)

// Receiver records handler-level events around the (optional) simulated work.
type Receiver struct {
	rec   *events.Recorder
	delay time.Duration
}

// NewReceiver builds a receiver with artificial processing delay delay.
func NewReceiver(rec *events.Recorder, delay time.Duration) *Receiver {
	return &Receiver{rec: rec, delay: delay}
}

// Process handles a one-way message.
func (r *Receiver) Process(m protocol.Message) {
	r.rec.Record(events.HandlerStarted, m.CorrelationID, m.SequenceNumber, nil)
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.rec.Record(events.HandlerCompleted, m.CorrelationID, m.SequenceNumber, nil)
}

// HandleWithReply handles a request and returns the empty reply payload.
func (r *Receiver) HandleWithReply(m protocol.Message) []byte {
	r.Process(m)
	return nil
}
