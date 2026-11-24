// Package config reads the subset of an eccom scenario file that the Go
// agent needs. The Python side validates the whole file
// (eccom/core/config.py); this side trusts it, ignores the keys it does not
// use and fills in the same defaults for absent ones. Optional numeric
// fields keep Python's None as the zero value: every one of them is
// validated as strictly positive when set.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Scenario is the agent-relevant part of a scenario file.
type Scenario struct {
	Name            string          `yaml:"name"`
	Interaction     string          `yaml:"interaction"`
	Topology        Topology        `yaml:"topology"`
	Payload         Payload         `yaml:"payload"`
	Load            Traffic         `yaml:"load"`
	Sender          Sender          `yaml:"sender"`
	Receiver        Receiver        `yaml:"receiver"`
	Experiment      Experiment      `yaml:"experiment"`
	Instrumentation Instrumentation `yaml:"instrumentation"`
	Protocol        Protocol        `yaml:"protocol"`
}

// Topology holds the host role labels that name the per-host artifacts.
type Topology struct {
	Sender   string `yaml:"sender"`
	Receiver string `yaml:"receiver"`
}

// Payload mirrors PayloadConfig (constant_rate and ramp modes).
type Payload struct {
	SizeBytes int    `yaml:"size_bytes"`
	Content   string `yaml:"content"`
	Seed      int64  `yaml:"seed"`
}

// Traffic mirrors LoadConfig, the load block; zero means unset for the
// optional fields.
type Traffic struct {
	Mode              string  `yaml:"mode"`
	MessagesPerSecond float64 `yaml:"messages_per_second"`
	DurationS         float64 `yaml:"duration_s"`
	TotalMessages     int64   `yaml:"total_messages"`
	Concurrency       int     `yaml:"concurrency"`
	Senders           int     `yaml:"senders"`
	RateMultiplier    float64 `yaml:"rate_multiplier"`
	RateIncrement     float64 `yaml:"rate_increment"`
	SizeMultiplier    float64 `yaml:"size_multiplier"`
	StepDurationS     float64 `yaml:"step_duration_s"`
	StepPauseS        float64 `yaml:"step_pause_s"`
}

// Sender mirrors SenderConfig: which program sends and, for the Go sender,
// how many threads it may use (0 = the runtime's default).
type Sender struct {
	Implementation string `yaml:"implementation"`
	Threads        int    `yaml:"threads"`
}

// Receiver mirrors ReceiverConfig.
type Receiver struct {
	ArtificialProcessingDelayMs float64 `yaml:"artificial_processing_delay_ms"`
	ControlPort                 int     `yaml:"control_port"`
	ControlBindHost             string  `yaml:"control_bind_host"`
}

// Experiment mirrors ExperimentConfig.
type Experiment struct {
	WarmupS   float64 `yaml:"warmup_s"`
	CooldownS float64 `yaml:"cooldown_s"`
}

// Instrumentation mirrors the sampling part of InstrumentationConfig.
type Instrumentation struct {
	SystemMetrics          bool    `yaml:"system_metrics"`
	SystemMetricsIntervalS float64 `yaml:"system_metrics_interval_s"`
	BaselineS              float64 `yaml:"baseline_s"`
}

// Protocol mirrors ProtocolConfig.
type Protocol struct {
	Type       string `yaml:"type"`
	HTTP       HTTP   `yaml:"http"`
	MQTT       MQTT   `yaml:"mqtt"`
	TLS        TLS    `yaml:"tls"`
	TCPNoDelay bool   `yaml:"tcp_nodelay"`
}

// SetConnectHost points the active endpoint (HTTP receiver or MQTT broker)
// at host: the address the runner resolved for a topology role label such
// as "cloud". The inactive protocol block is left alone.
func (p *Protocol) SetConnectHost(host string) {
	if p.Type == "mqtt" {
		p.MQTT.Endpoint.Host = host
	} else {
		p.HTTP.Endpoint.Host = host
	}
}

// RequestTimeout is the sender's per-message deadline of the active
// protocol (ProtocolConfig.request_timeout_s).
func (p Protocol) RequestTimeout() time.Duration {
	if p.Type == "mqtt" {
		return Seconds(p.MQTT.Client.TimeoutS)
	}
	return Seconds(p.HTTP.Client.TimeoutS)
}

// Endpoint is where a server (HTTP receiver or MQTT broker) is reachable.
type Endpoint struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// HTTP mirrors HttpConfig.
type HTTP struct {
	Endpoint Endpoint   `yaml:"endpoint"`
	Client   HTTPClient `yaml:"client"`
	Server   HTTPServer `yaml:"server"`
}

// HTTPClient mirrors HttpClientConfig (sender side).
type HTTPClient struct {
	KeepAlive      bool    `yaml:"keep_alive"`
	TimeoutS       float64 `yaml:"timeout_s"`
	MaxConnections int     `yaml:"max_connections"`
}

// HTTPServer mirrors HttpServerConfig.
type HTTPServer struct {
	BindHost string `yaml:"bind_host"`
}

// MQTT mirrors MqttConfig's agent-side fields.
type MQTT struct {
	Endpoint  Endpoint   `yaml:"endpoint"`
	Client    MQTTClient `yaml:"client"`
	BaseTopic string     `yaml:"base_topic"`
}

// MQTTClient mirrors the MqttClientConfig fields both roles use.
// max_inflight_messages is a Python-paho knob with no counterpart here:
// paho.golang honours the broker's Receive Maximum instead.
type MQTTClient struct {
	QoS        int     `yaml:"qos"`
	KeepaliveS int     `yaml:"keepalive_s"`
	CleanStart bool    `yaml:"clean_start"`
	TimeoutS   float64 `yaml:"timeout_s"`
}

// TLS mirrors TlsConfig; paths are relative to the scenario file's directory.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

// Seconds converts a scenario's float seconds into a Duration.
func Seconds(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}

// Defaults returns a scenario with every default of the Python models.
func Defaults() Scenario {
	return Scenario{
		Interaction: "request_response",
		Topology:    Topology{Sender: "edge", Receiver: "cloud"},
		Payload:     Payload{Content: "zeros"},
		Load:        Traffic{Mode: "constant_rate", Concurrency: 1, Senders: 1},
		Sender:      Sender{Implementation: "python"},
		Receiver: Receiver{
			ControlPort:     8081,
			ControlBindHost: "127.0.0.1",
		},
		Instrumentation: Instrumentation{
			SystemMetrics:          true,
			SystemMetricsIntervalS: 1.0,
			BaselineS:              3.0,
		},
		Protocol: Protocol{
			HTTP: HTTP{
				Endpoint: Endpoint{Host: "127.0.0.1", Port: 8080},
				Client:   HTTPClient{KeepAlive: true, TimeoutS: 5.0},
				Server:   HTTPServer{BindHost: "127.0.0.1"},
			},
			MQTT: MQTT{
				Endpoint:  Endpoint{Host: "127.0.0.1", Port: 1883},
				Client:    MQTTClient{QoS: 1, KeepaliveS: 60, CleanStart: true, TimeoutS: 5.0},
				BaseTopic: "eccom",
			},
			TCPNoDelay: true,
		},
	}
}

// Load reads path over Defaults().
// Returns an error when the file cannot be read or is not valid YAML.
func Load(path string) (Scenario, error) {
	scenario := Defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		return scenario, err
	}
	if err := yaml.Unmarshal(data, &scenario); err != nil {
		return scenario, fmt.Errorf("scenario %s: %w", path, err)
	}
	return scenario, nil
}
