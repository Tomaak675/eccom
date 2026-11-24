package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"eccom/internal/config"
	"eccom/internal/events"
	"eccom/internal/load"
	"eccom/internal/payload"
	"eccom/internal/protocol"
)

// Sender is eccom/agents/sender.py: it generates the traffic and records
// the interaction-level events; everything protocol-specific is behind the
// adapter. Only the paced stream (constant_rate and ramp) exists here, the
// Python validator rejects devices mode for the Go sender.
type Sender struct {
	scenario config.Scenario
	adapter  protocol.Adapter
	rec      *events.Recorder
	payloads *payload.Generator
	lanes    int
	timeout  time.Duration
	oneWay   bool
	// Sequence numbers keep increasing across warmup and measurement; the
	// phases are separated by the recorder's phase tag.
	sequence atomic.Int64
}

// NewSender builds a sender that paces the scenario's load over lanes
// goroutines.
func NewSender(scenario config.Scenario, adapter protocol.Adapter, rec *events.Recorder, lanes int) *Sender {
	return &Sender{
		scenario: scenario,
		adapter:  adapter,
		rec:      rec,
		payloads: payload.New(scenario.Payload),
		lanes:    lanes,
		timeout:  scenario.Protocol.RequestTimeout(),
		oneWay:   scenario.Interaction == "one_way",
	}
}

// Run sends the warmup (if configured) and the measurement traffic and
// returns the measurement phase's load report.
func (s *Sender) Run(ctx context.Context) load.Report {
	if warmup := s.scenario.Experiment.WarmupS; warmup > 0 {
		slog.Info("warmup", "seconds", warmup)
		s.rec.SetPhase("warmup")
		s.runPhase(ctx, warmup)
	}
	slog.Info("measurement phase started")
	s.rec.SetPhase("measurement")
	report := s.runPhase(ctx, 0)
	slog.Info("measurement phase done",
		"attempted", report.MessagesAttempted,
		"completed", report.MessagesCompleted,
		"failed", report.MessagesFailed,
		"requested_rate", report.RequestedMessageRate,
		"actual_rate", report.ActualMessageRate,
	)
	return report
}

// runPhase runs the load, truncated to durationOverrideS when positive
// (the warmup: the same schedule, cut short).
func (s *Sender) runPhase(ctx context.Context, durationOverrideS float64) load.Report {
	cfg := s.scenario.Load
	if durationOverrideS > 0 {
		cfg.DurationS = durationOverrideS
		cfg.TotalMessages = 0
	}
	return load.New(cfg, s.scenario.Payload.SizeBytes, s.lanes).Run(ctx, s.sendOne)
}

// sendOne is the generator's SendOne: build the payload, record the
// interaction events around the adapter call, report success.
func (s *Sender) sendOne(metadata map[string]any) bool {
	size, _ := metadata["target_size"].(int)
	body := s.payloads.Generate(size)
	m := protocol.Message{
		CorrelationID:  newCorrelationID(),
		SequenceNumber: s.sequence.Add(1) - 1,
		Payload:        body,
	}
	created := make(map[string]any, len(metadata)+1)
	created["size_bytes"] = len(body)
	for key, value := range metadata {
		created[key] = value
	}
	s.rec.Record(events.PayloadCreated, m.CorrelationID, m.SequenceNumber, created)
	s.rec.Record(events.SendStarted, m.CorrelationID, m.SequenceNumber, nil)
	var reply protocol.Message
	var err error
	if s.oneWay {
		// SEND_COMPLETED is recorded by the adapter (wire boundary).
		err = s.adapter.Send(m)
	} else {
		reply, err = s.adapter.SendAndWaitReply(m, s.timeout)
	}
	switch {
	case errors.Is(err, protocol.ErrTimeout):
		s.rec.Record(events.Timeout, m.CorrelationID, m.SequenceNumber, nil)
		return false
	case err != nil:
		s.rec.Record(events.Error, m.CorrelationID, m.SequenceNumber, map[string]any{"error": err.Error()})
		return false
	case s.oneWay:
		return true
	}
	s.rec.Record(events.ReplyReceived, m.CorrelationID, m.SequenceNumber, map[string]any{"size_bytes": len(reply.Payload)})
	return true
}

// newCorrelationID is uuid4().hex: 32 hex characters, no dashes.
func newCorrelationID() string {
	var id [16]byte
	rand.Read(id[:])
	return hex.EncodeToString(id[:])
}
