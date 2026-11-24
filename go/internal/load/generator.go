// Package load is eccom/generators/load.py: open-loop load generation at a
// constant or ramped rate. The schedule never waits for replies; only the
// concurrency cap applies backpressure, and the achieved rate is measured
// and reported next to the requested one.
//
// One process generates the whole schedule, paced by several lanes:
// goroutine i fires the sends k*lanes+i of one common cadence, so the
// aggregate is exactly the scenario's schedule while each lane sleeps and
// wakes at a lanes-th of the rate. (The Python sender splits the same
// schedule across processes instead, see LoadConfig.for_instance.) A lane
// that falls behind catches up in a burst, as the Python loop does, and the
// deviation shows up in the reported actual rate.
//
// ramp mode raises the rate (rate_multiplier or rate_increment) and/or the
// payload size (size_multiplier) every step_duration_s on the one continuous
// schedule; nothing is drained at a boundary unless step_pause_s inserts a
// quiet gap after every step but the last. Every send is tagged with its
// step so the analysis attributes messages to steps exactly.
package load

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"eccom/internal/config"
)

// SendOne sends one message end-to-end and reports success. It must not
// panic. metadata is attached to the message's PAYLOAD_CREATED event: nil
// for constant_rate, {"step", "target_rate"} for ramp plus "target_size"
// when the ramp grows the payload.
type SendOne func(metadata map[string]any) bool

// StepReport is one ramp step; the json tags are the Python StepReport.
type StepReport struct {
	Step              int     `json:"step"`
	TargetMessageRate float64 `json:"target_message_rate"`
	ActualMessageRate float64 `json:"actual_message_rate"`
	MessagesAttempted int64   `json:"messages_attempted"`
	MessagesCompleted int64   `json:"messages_completed"`
	MessagesFailed    int64   `json:"messages_failed"`
	DurationS         float64 `json:"duration_s"`
	PayloadSizeBytes  *int    `json:"payload_size_bytes"`
}

// Report is the Python LoadReport, written as <sender>-load-report.json.
type Report struct {
	RequestedMessageRate float64        `json:"requested_message_rate"`
	ActualMessageRate    float64        `json:"actual_message_rate"`
	MessagesAttempted    int64          `json:"messages_attempted"`
	MessagesCompleted    int64          `json:"messages_completed"`
	MessagesFailed       int64          `json:"messages_failed"`
	SendWindowS          float64        `json:"send_window_s"`
	TotalDurationS       float64        `json:"total_duration_s"`
	PerDevice            map[string]any `json:"per_device"` // devices mode only, never set here
	Steps                []StepReport   `json:"steps"`      // ramp mode only
}

// step holds one ramp step's counters; completed is bumped by in-flight
// sends after the step has already been closed.
type step struct {
	index      int
	targetRate float64
	metadata   map[string]any
	targetSize int // 0 unless the ramp grows the payload
	openedAt   time.Time
	closedAt   time.Time // zero while open
	attempted  atomic.Int64
	completed  atomic.Int64
}

func (s *step) report() StepReport {
	duration := s.closedAt.Sub(s.openedAt).Seconds()
	attempted := s.attempted.Load()
	completed := s.completed.Load()
	var rate float64
	if duration > 0 {
		rate = float64(attempted) / duration
	}
	var size *int
	if s.targetSize > 0 {
		size = &s.targetSize
	}
	return StepReport{
		Step:              s.index,
		TargetMessageRate: s.targetRate,
		ActualMessageRate: rate,
		MessagesAttempted: attempted,
		MessagesCompleted: completed,
		MessagesFailed:    attempted - completed,
		DurationS:         duration,
		PayloadSizeBytes:  size,
	}
}

// Generator paces sends according to one Load config.
type Generator struct {
	cfg         config.Traffic
	payloadSize int
	lanes       int
}

// New prepares a generator. payloadSizeBytes is the first step's payload
// size (needed when the ramp sets size_multiplier); lanes is the number of
// pacing goroutines, at least 1.
func New(cfg config.Traffic, payloadSizeBytes, lanes int) *Generator {
	return &Generator{cfg: cfg, payloadSize: payloadSizeBytes, lanes: max(lanes, 1)}
}

// run is the state of one Run call, shared by its lanes.
type run struct {
	g         *Generator
	ctx       context.Context
	send      SendOne
	start     time.Time
	semaphore chan struct{}
	inFlight  sync.WaitGroup
	attempted atomic.Int64

	mu    sync.Mutex
	steps []*step
}

// Run generates the schedule until it ends or ctx is cancelled, waits for
// the in-flight sends to drain and returns the report.
func (g *Generator) Run(ctx context.Context, send SendOne) Report {
	r := &run{
		g:         g,
		ctx:       ctx,
		send:      send,
		start:     time.Now(),
		semaphore: make(chan struct{}, g.cfg.Concurrency),
	}
	var lanes sync.WaitGroup
	for lane := 0; lane < g.lanes; lane++ {
		lanes.Add(1)
		go func() {
			defer lanes.Done()
			r.lane(lane)
		}()
	}
	lanes.Wait()
	now := time.Now()
	r.closeOpenStep(now)
	sendWindow := math.Max(now.Sub(r.start).Seconds(), 1e-9)
	r.inFlight.Wait()
	total := time.Since(r.start).Seconds()

	attempted := r.attempted.Load()
	var completed int64
	var steps []StepReport
	for _, s := range r.steps {
		completed += s.completed.Load()
		steps = append(steps, s.report())
	}
	if g.cfg.Mode != "ramp" {
		steps = nil
	}
	return Report{
		RequestedMessageRate: g.cfg.MessagesPerSecond,
		ActualMessageRate:    float64(attempted) / sendWindow,
		MessagesAttempted:    attempted,
		MessagesCompleted:    completed,
		MessagesFailed:       attempted - completed,
		SendWindowS:          sendWindow,
		TotalDurationS:       total,
		Steps:                steps,
	}
}

// lane is one pacing goroutine: it fires the sends index, index+lanes, ...
// of the common cadence.
func (r *run) lane(index int) {
	g := r.g
	offset, spacing := g.cadence(0, index)
	nextSend := r.start.Add(offset)
	current := 0
	timer := time.NewTimer(0)
	<-timer.C
	for {
		if delay := time.Until(nextSend); delay > 0 {
			timer.Reset(delay)
			select {
			case <-timer.C:
			case <-r.ctx.Done():
				return
			}
		}
		// Behind schedule (slow sends, coarse timers, the concurrency cap):
		// no sleep, catch up in a burst; actual_message_rate shows it.
		select {
		case r.semaphore <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		now := time.Now()
		elapsed := now.Sub(r.start).Seconds()
		// Re-checked after the wait so a send that only got its slot after
		// the deadline is not scheduled (it would land outside every step).
		if r.ctx.Err() != nil || !g.withinDuration(elapsed) {
			<-r.semaphore
			return
		}
		stepIndex := g.stepIndex(elapsed)
		if stepIndex != current {
			current = stepIndex
			_, spacing = g.cadence(stepIndex, index)
		}
		s := r.enter(stepIndex, now)
		if g.inPause(elapsed) {
			// The quiet gap after a step: nothing is scheduled until the
			// next step starts and the backlog drains.
			r.close(s, now)
			<-r.semaphore
			offset, spacing = g.cadence(stepIndex+1, index)
			nextSend = r.start.Add(config.Seconds(float64(stepIndex+1) * g.period())).Add(offset)
			continue
		}
		if !r.reserve() {
			<-r.semaphore
			return
		}
		r.inFlight.Add(1)
		go r.sendOne(s)
		s.attempted.Add(1)
		nextSend = nextSend.Add(spacing)
	}
}

func (r *run) sendOne(s *step) {
	defer r.inFlight.Done()
	defer func() { <-r.semaphore }()
	if r.send(s.metadata) {
		s.completed.Add(1)
	}
}

// reserve counts one more attempted send; false when total_messages is
// reached (the only stop condition that depends on the count).
func (r *run) reserve() bool {
	n := r.attempted.Add(1)
	if total := r.g.cfg.TotalMessages; total > 0 && n > total {
		r.attempted.Add(-1)
		return false
	}
	return true
}

// enter returns the step for index, opening it and every step before it
// that no lane reached (opened and closed at once, with zero sends) and
// closing the step that was open until now.
func (r *run) enter(index int, now time.Time) *step {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.steps) <= index {
		if last := len(r.steps) - 1; last >= 0 && r.steps[last].closedAt.IsZero() {
			r.steps[last].closedAt = now
		}
		r.steps = append(r.steps, r.g.newStep(len(r.steps), now))
	}
	return r.steps[index]
}

func (r *run) close(s *step, now time.Time) {
	r.mu.Lock()
	if s.closedAt.IsZero() {
		s.closedAt = now
	}
	r.mu.Unlock()
}

func (r *run) closeOpenStep(now time.Time) {
	r.mu.Lock()
	if last := len(r.steps) - 1; last >= 0 && r.steps[last].closedAt.IsZero() {
		r.steps[last].closedAt = now
	}
	r.mu.Unlock()
}

func (g *Generator) newStep(index int, now time.Time) *step {
	rate := g.stepRate(index)
	s := &step{index: index, targetRate: rate, openedAt: now}
	if g.cfg.Mode == "ramp" {
		s.metadata = map[string]any{"step": index, "target_rate": rate}
		if g.cfg.SizeMultiplier > 0 {
			s.targetSize = int(math.Round(float64(g.payloadSize) * math.Pow(g.cfg.SizeMultiplier, float64(index))))
			s.metadata["target_size"] = s.targetSize
		}
	}
	return s
}

// stepRate is the offered rate of step index (the whole process, all lanes).
func (g *Generator) stepRate(index int) float64 {
	if g.cfg.Mode != "ramp" {
		return g.cfg.MessagesPerSecond
	}
	multiplier := g.cfg.RateMultiplier
	if multiplier == 0 {
		multiplier = 1
	}
	return g.cfg.MessagesPerSecond*math.Pow(multiplier, float64(index)) + g.cfg.RateIncrement*float64(index)
}

// cadence is lane's share of step index's cadence: its first send offset
// into the step and the spacing between its sends.
func (g *Generator) cadence(index, lane int) (offset, spacing time.Duration) {
	interval := 1 / g.stepRate(index)
	return config.Seconds(float64(lane) * interval), config.Seconds(float64(g.lanes) * interval)
}

// withinDuration is the duration-based stop condition (duration_s counts
// sending time only); always true in total_messages mode.
func (g *Generator) withinDuration(elapsedS float64) bool {
	if g.cfg.TotalMessages > 0 {
		return true
	}
	return g.sendingTime(elapsedS) < g.cfg.DurationS
}

// period is one ramp step plus the pause that follows it.
func (g *Generator) period() float64 {
	return g.cfg.StepDurationS + g.cfg.StepPauseS
}

func (g *Generator) stepIndex(elapsedS float64) int {
	if g.cfg.Mode != "ramp" {
		return 0
	}
	return int(elapsedS / g.period())
}

// inPause is whether elapsedS falls in the quiet gap after a step.
func (g *Generator) inPause(elapsedS float64) bool {
	if g.cfg.Mode != "ramp" || g.cfg.StepPauseS == 0 {
		return false
	}
	return math.Mod(elapsedS, g.period()) >= g.cfg.StepDurationS
}

// sendingTime is elapsedS minus the pauses already spent.
func (g *Generator) sendingTime(elapsedS float64) float64 {
	if g.cfg.Mode != "ramp" {
		return elapsedS
	}
	index := math.Floor(elapsedS / g.period())
	offset := elapsedS - index*g.period()
	return index*g.cfg.StepDurationS + math.Min(offset, g.cfg.StepDurationS)
}
