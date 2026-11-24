// Package clocksync is the probe half of eccom/instrumentation/clock_sync.py:
// NTP-style clock offset measurement against the receiver's control
// endpoint. Each probe records the sender's send/receive wall times (t0, t3)
// and the receiver's receive/transmit wall times (t1, t2);
//
//	offset = ((t1 - t0) + (t2 - t3)) / 2      // receiver clock - sender clock
//	rtt    = (t3 - t0) - (t2 - t1)
//
// The probe with the smallest RTT wins and its worst-case error is RTT/2.
// Probing before and after the run exposes drift, which widens the bound.
// The record is written with the same keys as the Python one, so the
// analysis reads both.
package clocksync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"eccom/internal/clock"
	"eccom/internal/control"
)

// Method names the probe in the clock-sync record (PROBE_METHOD).
const Method = "control-endpoint-ntp-probe"

const (
	samples       = 15
	sampleGap     = 20 * time.Millisecond
	sampleTimeout = 5 * time.Second
)

// Probe is one measurement: the best of several exchanges.
type Probe struct {
	OffsetNs         int64 `json:"offset_ns"`
	UncertaintyNs    int64 `json:"uncertainty_ns"`
	MinRTTNs         int64 `json:"min_rtt_ns"`
	Samples          int   `json:"samples"`
	MeasuredAtWallNs int64 `json:"measured_at_wall_ns"`
}

// Record is the run's clock-sync file (combine_clock_probes).
type Record struct {
	Method        string `json:"method"`
	OffsetNs      int64  `json:"offset_ns"`
	UncertaintyNs int64  `json:"uncertainty_ns"`
	DriftNs       *int64 `json:"drift_ns"`
	Before        Probe  `json:"before"`
	After         *Probe `json:"after"`
}

// OffsetAndRTT computes the offset (receiver minus sender) and the round
// trip of one exchange.
func OffsetAndRTT(t0, t1, t2, t3 int64) (offset, rtt int64) {
	return ((t1 - t0) + (t2 - t3)) / 2, (t3 - t0) - (t2 - t1)
}

// ProbeOffset runs the probes against the control endpoint at
// controlBaseURL and keeps the best one.
// Returns the first request or decoding error.
func ProbeOffset(ctx context.Context, controlBaseURL string) (Probe, error) {
	url := strings.TrimRight(controlBaseURL, "/") + control.ClockPath
	client := &http.Client{Timeout: sampleTimeout}
	best := Probe{MinRTTNs: -1}
	for i := 0; i < samples; i++ {
		t0 := clock.WallNs()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return Probe{}, err
		}
		response, err := client.Do(request)
		if err != nil {
			return Probe{}, err
		}
		var times struct {
			T1 int64 `json:"receive_wall_time_ns"`
			T2 int64 `json:"transmit_wall_time_ns"`
		}
		err = json.NewDecoder(response.Body).Decode(&times)
		response.Body.Close()
		if err != nil {
			return Probe{}, fmt.Errorf("decoding %s: %w", url, err)
		}
		t3 := clock.WallNs()
		offset, rtt := OffsetAndRTT(t0, times.T1, times.T2, t3)
		if best.MinRTTNs < 0 || rtt < best.MinRTTNs {
			best.OffsetNs, best.MinRTTNs = offset, rtt
		}
		best.Samples++
		time.Sleep(sampleGap)
	}
	best.UncertaintyNs = max(best.MinRTTNs/2, 0)
	best.MeasuredAtWallNs = clock.WallNs()
	return best, nil
}

// Combine merges the pre-run probe and the optional post-run probe into
// the run's record; drift between them widens the uncertainty.
func Combine(before Probe, after *Probe) Record {
	if after == nil {
		return Record{Method: Method, OffsetNs: before.OffsetNs, UncertaintyNs: before.UncertaintyNs, Before: before}
	}
	drift := after.OffsetNs - before.OffsetNs
	return Record{
		Method:        Method,
		OffsetNs:      (before.OffsetNs + after.OffsetNs) / 2,
		UncertaintyNs: max(before.UncertaintyNs, after.UncertaintyNs) + abs(drift)/2,
		DriftNs:       &drift,
		Before:        before,
		After:         after,
	}
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
