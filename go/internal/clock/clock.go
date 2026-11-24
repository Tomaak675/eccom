// Package clock provides the two timestamps every eccom artifact carries,
// mirroring the Python agent's time.perf_counter_ns / time.time_ns pair.
package clock

import "time"

var start = time.Now()

// MonotonicNs returns nanoseconds since process start from the runtime's
// monotonic clock. Only comparable within this process, which is all the
// analysis does with monotonic_ns.
func MonotonicNs() int64 {
	return int64(time.Since(start))
}

// WallNs returns the wall clock in nanoseconds since the Unix epoch;
// comparable across hosts once their clock offset is known.
func WallNs() int64 {
	return time.Now().UnixNano()
}
