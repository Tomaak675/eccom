package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"eccom/internal/clocksync"
	"eccom/internal/config"
	"eccom/internal/control"
	"eccom/internal/environment"
	"eccom/internal/events"
	"eccom/internal/metrics"
	"eccom/internal/protocol"
)

const stopTimeout = 10 * time.Second

// RunReceiver serves the scenario at scenarioPath until the control endpoint
// (or SIGTERM/SIGINT, or ctx) requests shutdown, then flushes the artifacts
// into runDir. A non-empty endpointHost replaces the scenario's endpoint
// host (the runner passes the address it resolved for a role label).
// Returns the first error of setup or of writing the artifacts.
func RunReceiver(ctx context.Context, scenarioPath, runDir, endpointHost string) (err error) {
	scenario, err := loadScenario(scenarioPath, runDir, endpointHost)
	if err != nil {
		return err
	}
	host := scenario.Topology.Receiver
	if err := environment.Write(host, runDir); err != nil {
		return err
	}

	recorder := events.NewRecorder(host, filepath.Join(runDir, host+"-events.jsonl"))
	adapter, err := protocol.New(scenario.Protocol, filepath.Dir(scenarioPath), recorder)
	if err != nil {
		return err
	}
	receiver := NewReceiver(recorder, time.Duration(scenario.Receiver.ArtificialProcessingDelayMs*float64(time.Millisecond)))
	ctl := control.New(scenario.Receiver.ControlBindHost, scenario.Receiver.ControlPort)
	sampler, err := newSampler(scenario, host, runDir)
	if err != nil {
		return err
	}

	listening, controlling, sampling := false, false, false
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		if sampling {
			if stopErr := sampler.Stop(); stopErr != nil {
				err = errors.Join(err, stopErr)
			}
		}
		if listening {
			if stopErr := adapter.Stop(stopCtx); stopErr != nil {
				slog.Warn("stopping the protocol adapter failed", "error", stopErr)
			}
		}
		if controlling {
			ctl.Stop(stopCtx)
		}
		if closeErr := recorder.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if scenario.Interaction == "one_way" {
		err = adapter.Listen(receiver.Process)
	} else {
		err = adapter.ListenWithReply(receiver.HandleWithReply)
	}
	if err != nil {
		return err
	}
	listening = true
	if err = ctl.Start(); err != nil {
		return err
	}
	controlling = true
	if sampler != nil {
		sampler.Start()
		sampling = true
		sampler.MeasureBaseline(config.Seconds(scenario.Instrumentation.BaselineS))
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	slog.Info("receiver ready", "protocol", scenario.Protocol.Type, "control_port", scenario.Receiver.ControlPort)
	select {
	case <-ctl.ShutdownRequested():
	case sig := <-signals:
		slog.Info("signal received", "signal", sig.String())
	case <-ctx.Done():
	}
	slog.Info("receiver shutting down", "events_recorded", recorder.Len())
	return nil
}

// RunSender runs the scenario's load to completion (runtime.py::_run_sender)
// and writes the sender's artifacts into runDir: events, load report,
// system metrics, environment and, with a peerControl address (host:port of
// the receiver's control endpoint), the clock-sync record. A cancelled ctx
// stops the schedule early; the in-flight sends still drain. endpointHost
// is as for RunReceiver.
// Returns the first error of setup or of writing the artifacts.
func RunSender(ctx context.Context, scenarioPath, runDir, peerControl, endpointHost string) (err error) {
	scenario, err := loadScenario(scenarioPath, runDir, endpointHost)
	if err != nil {
		return err
	}
	if scenario.Sender.Threads > 0 {
		runtime.GOMAXPROCS(scenario.Sender.Threads)
	}
	lanes := runtime.GOMAXPROCS(0)
	host := scenario.Topology.Sender
	if err := environment.Write(host, runDir); err != nil {
		return err
	}

	recorder := events.NewRecorder(host, filepath.Join(runDir, host+"-events.jsonl"))
	adapter, err := protocol.New(scenario.Protocol, filepath.Dir(scenarioPath), recorder)
	if err != nil {
		return err
	}
	sender := NewSender(scenario, adapter, recorder, lanes)
	sampler, err := newSampler(scenario, host, runDir)
	if err != nil {
		return err
	}

	started, sampling := false, false
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		if sampling {
			if stopErr := sampler.Stop(); stopErr != nil {
				err = errors.Join(err, stopErr)
			}
		}
		if started {
			if stopErr := adapter.Stop(stopCtx); stopErr != nil {
				slog.Warn("stopping the protocol adapter failed", "error", stopErr)
			}
		}
		if closeErr := recorder.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err = adapter.Start(); err != nil {
		return err
	}
	started = true
	if sampler != nil {
		sampler.Start()
		sampling = true
	}
	clockBefore := probeClock(ctx, peerControl)
	if sampler != nil {
		// after the probe, before any traffic: a genuinely quiet window
		sampler.MeasureBaseline(config.Seconds(scenario.Instrumentation.BaselineS))
	}
	slog.Info("sender ready", "protocol", scenario.Protocol.Type, "lanes", lanes, "go_max_procs", runtime.GOMAXPROCS(0))
	report := sender.Run(ctx)
	if err := writeJSON(filepath.Join(runDir, host+"-load-report.json"), report); err != nil {
		return err
	}
	if cooldown := config.Seconds(scenario.Experiment.CooldownS); cooldown > 0 {
		time.Sleep(cooldown)
	}
	if clockBefore != nil {
		record := clocksync.Combine(*clockBefore, probeClock(ctx, peerControl))
		if err := writeJSON(filepath.Join(runDir, host+"-clock-sync.json"), record); err != nil {
			return err
		}
		slog.Info("clock offset vs receiver",
			"offset_ms", float64(record.OffsetNs)/1e6,
			"uncertainty_ms", float64(record.UncertaintyNs)/1e6,
		)
	}
	return nil
}

// loadScenario reads the scenario, applies the endpoint host override and
// puts a copy of the file next to the artifacts.
func loadScenario(scenarioPath, runDir, endpointHost string) (config.Scenario, error) {
	scenario, err := config.Load(scenarioPath)
	if err != nil {
		return scenario, err
	}
	if endpointHost != "" {
		scenario.Protocol.SetConnectHost(endpointHost)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return scenario, err
	}
	return scenario, copyScenario(scenarioPath, filepath.Join(runDir, "scenario.yaml"))
}

func newSampler(scenario config.Scenario, host, runDir string) (*metrics.Sampler, error) {
	if !scenario.Instrumentation.SystemMetrics {
		return nil, nil
	}
	interval := config.Seconds(scenario.Instrumentation.SystemMetricsIntervalS)
	return metrics.New(host, interval, filepath.Join(runDir, host+"-system-metrics.jsonl"))
}

// probeClock measures the clock offset against the receiver's control
// endpoint; nil without an address or when the probe fails (logged).
func probeClock(ctx context.Context, peerControl string) *clocksync.Probe {
	if peerControl == "" {
		return nil
	}
	probe, err := clocksync.ProbeOffset(ctx, "http://"+peerControl)
	if err != nil {
		slog.Warn("clock offset probe failed", "peer", peerControl, "error", err)
		return nil
	}
	return &probe
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// copyScenario puts the scenario next to the artifacts unless the runner
// already did (runner-less flows such as docker compose).
func copyScenario(from, to string) error {
	if _, err := os.Stat(to); err == nil {
		return nil
	}
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		target.Close()
		return fmt.Errorf("copying scenario: %w", err)
	}
	return target.Close()
}
