package config

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const scenariosDir = "../../../scenarios"

func TestDefaultsMatchThePythonModels(t *testing.T) {
	s := Defaults()
	if s.Interaction != "request_response" || s.Topology.Sender != "edge" || s.Topology.Receiver != "cloud" {
		t.Fatalf("top-level defaults: %+v", s)
	}
	if s.Payload.Content != "zeros" || s.Load.Mode != "constant_rate" || s.Load.Concurrency != 1 || s.Load.Senders != 1 {
		t.Fatalf("payload/load defaults: %+v %+v", s.Payload, s.Load)
	}
	if s.Sender.Implementation != "python" || s.Sender.Threads != 0 || s.Experiment.WarmupS != 0 || s.Experiment.CooldownS != 0 {
		t.Fatalf("sender/experiment defaults: %+v %+v", s.Sender, s.Experiment)
	}
	if s.Receiver.ControlPort != 8081 || s.Receiver.ControlBindHost != "127.0.0.1" {
		t.Fatalf("receiver defaults: %+v", s.Receiver)
	}
	if !s.Instrumentation.SystemMetrics || s.Instrumentation.SystemMetricsIntervalS != 1 || s.Instrumentation.BaselineS != 3 {
		t.Fatalf("instrumentation defaults: %+v", s.Instrumentation)
	}
	p := s.Protocol
	if !p.TCPNoDelay || p.HTTP.Endpoint.Port != 8080 || p.HTTP.Server.BindHost != "127.0.0.1" {
		t.Fatalf("http defaults: %+v", p)
	}
	if !p.HTTP.Client.KeepAlive || p.HTTP.Client.TimeoutS != 5 || p.HTTP.Client.MaxConnections != 0 {
		t.Fatalf("http client defaults: %+v", p.HTTP.Client)
	}
	if p.MQTT.Endpoint.Port != 1883 || p.MQTT.Client.QoS != 1 || p.MQTT.Client.KeepaliveS != 60 || !p.MQTT.Client.CleanStart || p.MQTT.Client.TimeoutS != 5 || p.MQTT.BaseTopic != "eccom" {
		t.Fatalf("mqtt defaults: %+v", p.MQTT)
	}
	if p.RequestTimeout() != 5*time.Second || Seconds(0.25) != 250*time.Millisecond {
		t.Fatalf("durations: %s", p.RequestTimeout())
	}
}

func TestEveryShippedScenarioLoads(t *testing.T) {
	var paths []string
	err := filepath.WalkDir(scenariosDir, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".yaml") {
			paths = append(paths, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 30 {
		t.Fatalf("expected the shipped scenario suites, found %d files", len(paths))
	}
	for _, path := range paths {
		s, err := Load(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if s.Name == "" || (s.Protocol.Type != "http" && s.Protocol.Type != "mqtt") {
			t.Errorf("%s: name %q, protocol %q", path, s.Name, s.Protocol.Type)
		}
		if s.Sender.Implementation == "go" && s.Load.Senders != 1 {
			t.Errorf("%s: Go sender with %d sender processes", path, s.Load.Senders)
		}
	}
}

func TestLoadKeepsDefaultsForAbsentKeysAndOverridesPresentOnes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	content := `name: partial
interaction: one_way
payload: {size_bytes: 32, content: random, seed: 42}
load:
  mode: ramp
  messages_per_second: 1800
  rate_increment: 1200
  step_duration_s: 10
  step_pause_s: 10
  duration_s: 240
  concurrency: 10000
sender: {implementation: go, threads: 6}
receiver:
  control_bind_host: 0.0.0.0
experiment: {warmup_s: 5, cooldown_s: 10}
protocol:
  type: http
  tcp_nodelay: false
  http:
    endpoint: {host: 10.0.0.1, port: 9000}
    client: {keep_alive: false, max_connections: 1500}
    server: {bind_host: 0.0.0.0}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Interaction != "one_way" || s.Receiver.ControlBindHost != "0.0.0.0" || s.Receiver.ControlPort != 8081 {
		t.Fatalf("receiver: %+v interaction %q", s.Receiver, s.Interaction)
	}
	if s.Payload != (Payload{SizeBytes: 32, Content: "random", Seed: 42}) || s.Sender != (Sender{Implementation: "go", Threads: 6}) {
		t.Fatalf("payload/sender: %+v %+v", s.Payload, s.Sender)
	}
	l := s.Load
	if l.Mode != "ramp" || l.MessagesPerSecond != 1800 || l.RateIncrement != 1200 || l.RateMultiplier != 0 || l.StepDurationS != 10 ||
		l.StepPauseS != 10 || l.DurationS != 240 || l.TotalMessages != 0 || l.Concurrency != 10000 || l.Senders != 1 {
		t.Fatalf("load: %+v", l)
	}
	if s.Experiment != (Experiment{WarmupS: 5, CooldownS: 10}) {
		t.Fatalf("experiment: %+v", s.Experiment)
	}
	if s.Protocol.TCPNoDelay || s.Protocol.HTTP.Endpoint.Port != 9000 || s.Protocol.HTTP.Server.BindHost != "0.0.0.0" {
		t.Fatalf("protocol: %+v", s.Protocol)
	}
	if c := s.Protocol.HTTP.Client; c.KeepAlive || c.TimeoutS != 5 || c.MaxConnections != 1500 {
		t.Fatalf("http client: %+v", c)
	}
	if s.Protocol.MQTT.Client.QoS != 1 || s.Instrumentation.BaselineS != 3 {
		t.Fatalf("untouched defaults changed: %+v", s)
	}
}

func TestSetConnectHostTouchesOnlyTheActiveProtocol(t *testing.T) {
	p := Defaults().Protocol
	p.Type = "mqtt"
	p.SetConnectHost("20.0.0.1")
	if p.MQTT.Endpoint.Host != "20.0.0.1" || p.HTTP.Endpoint.Host != "127.0.0.1" {
		t.Fatalf("mqtt: %+v", p)
	}
	p = Defaults().Protocol
	p.Type = "http"
	p.SetConnectHost("20.0.0.2")
	if p.HTTP.Endpoint.Host != "20.0.0.2" || p.MQTT.Endpoint.Host != "127.0.0.1" {
		t.Fatalf("http: %+v", p)
	}
}
