// Package metrics samples process and host resource usage at a fixed
// interval and writes the samples as JSON lines with the same keys as the
// Python SystemMetricsSampler (psutil), via gopsutil, so eccom compare
// treats both agents alike. Every sample goes to the file as it is taken,
// so nothing accumulates over a long run. loop_lag_ms_max is the worst
// overshoot of a 10 ms sleep since the previous sample: scheduler and GC
// latency, the Go analogue of asyncio event-loop lag.
package metrics

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"

	"eccom/internal/clock"
)

const lagTick = 10 * time.Millisecond

// Sample is one row of <host>-system-metrics.jsonl; field order = key order.
type Sample struct {
	Host                string  `json:"host"`
	Phase               string  `json:"phase"`
	WallTimeNs          int64   `json:"wall_time_ns"`
	MonotonicNs         int64   `json:"monotonic_ns"`
	SystemCPUPercent    float64 `json:"system_cpu_percent"`
	ProcessCPUPercent   float64 `json:"process_cpu_percent"`
	ProcessCPUSeconds   float64 `json:"process_cpu_seconds"`
	SystemMemoryPercent float64 `json:"system_memory_percent"`
	ProcessRSSBytes     uint64  `json:"process_rss_bytes"`
	NetBytesSent        uint64  `json:"net_bytes_sent"`
	NetBytesRecv        uint64  `json:"net_bytes_recv"`
	LoopLagMsMax        float64 `json:"loop_lag_ms_max"`
}

// Sampler runs the sampling and lag goroutines between Start and Stop; the
// sampling goroutine owns the output file.
type Sampler struct {
	host     string
	interval time.Duration
	proc     *process.Process
	file     *os.File
	writer   *bufio.Writer
	encoder  *json.Encoder
	err      error // first file error, read after the goroutines stopped

	mu       sync.Mutex
	phase    string
	lagMaxMs float64

	done chan struct{}
	wg   sync.WaitGroup
}

// New prepares a sampler for host writing to path every interval; the
// output file is created here.
// Returns an error when the own process cannot be opened or the file not
// be created.
func New(host string, interval time.Duration, path string) (*Sampler, error) {
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	return &Sampler{
		host:     host,
		interval: interval,
		proc:     proc,
		file:     file,
		writer:   writer,
		encoder:  json.NewEncoder(writer),
		phase:    "startup",
		done:     make(chan struct{}),
	}, nil
}

// Start primes the CPU counters (the first percent reading is always 0)
// and launches the goroutines.
func (s *Sampler) Start() {
	cpu.Percent(0, false)
	s.proc.Percent(0)
	s.wg.Add(2)
	go s.sampleLoop()
	go s.lagLoop()
}

// SetPhase tags subsequent samples.
func (s *Sampler) SetPhase(phase string) {
	s.mu.Lock()
	s.phase = phase
	s.mu.Unlock()
}

// MeasureBaseline sits idle for d with samples tagged "baseline", then
// switches to "traffic".
func (s *Sampler) MeasureBaseline(d time.Duration) {
	if d <= 0 {
		return
	}
	s.SetPhase("baseline")
	time.Sleep(d)
	s.SetPhase("traffic")
}

// Stop ends sampling and closes the file.
// Returns the first file error.
func (s *Sampler) Stop() error {
	close(s.done)
	s.wg.Wait()
	s.fail(s.writer.Flush())
	s.fail(s.file.Close())
	return s.err
}

func (s *Sampler) sampleLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.fail(s.encoder.Encode(s.sample()))
			s.fail(s.writer.Flush())
		}
	}
}

func (s *Sampler) lagLoop() {
	defer s.wg.Done()
	for {
		due := time.Now().Add(lagTick)
		select {
		case <-s.done:
			return
		case <-time.After(lagTick):
		}
		lagMs := float64(time.Since(due)) / float64(time.Millisecond)
		s.mu.Lock()
		if lagMs > s.lagMaxMs {
			s.lagMaxMs = lagMs
		}
		s.mu.Unlock()
	}
}

func (s *Sampler) sample() Sample {
	s.mu.Lock()
	phase, lagMaxMs := s.phase, s.lagMaxMs
	s.lagMaxMs = 0
	s.mu.Unlock()

	sample := Sample{
		Host:         s.host,
		Phase:        phase,
		WallTimeNs:   clock.WallNs(),
		MonotonicNs:  clock.MonotonicNs(),
		LoopLagMsMax: lagMaxMs,
	}
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		sample.SystemCPUPercent = percents[0]
	}
	if percent, err := s.proc.Percent(0); err == nil {
		sample.ProcessCPUPercent = percent
	}
	if times, err := s.proc.Times(); err == nil {
		sample.ProcessCPUSeconds = times.User + times.System
	}
	// psutil's percent is (total - available) / total; gopsutil's UsedPercent
	// is defined differently on Linux, so compute it the psutil way.
	if vm, err := mem.VirtualMemory(); err == nil && vm.Total > 0 {
		sample.SystemMemoryPercent = 100 * float64(vm.Total-vm.Available) / float64(vm.Total)
	}
	if info, err := s.proc.MemoryInfo(); err == nil {
		sample.ProcessRSSBytes = info.RSS
	}
	if counters, err := net.IOCounters(false); err == nil && len(counters) > 0 {
		sample.NetBytesSent = counters[0].BytesSent
		sample.NetBytesRecv = counters[0].BytesRecv
	}
	return sample
}

// fail keeps the first error; later ones are consequences of it.
func (s *Sampler) fail(err error) {
	if err != nil && s.err == nil {
		s.err = err
	}
}
