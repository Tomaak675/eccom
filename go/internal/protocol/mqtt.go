package protocol

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"eccom/internal/config"
	"eccom/internal/events"
)

const (
	SequenceProperty = "sequence_number"

	connectTimeout = 15 * time.Second // covers a managed broker container still starting up
	connectRetry   = 500 * time.Millisecond
)

// mqttAdapter is eccom/protocols/mqtt.py: one MQTT v5 connection per
// process. The sender subscribes its own reply topic
// <base_topic>/replies/<client-id> in Start and publishes requests on
// <base_topic>/requests; the receiver subscribes the request topic in
// Listen. Correlation Data carries the correlation id, a user property the
// sequence number and Response Topic routes the reply, which is published
// at the same QoS as the request. SEND_COMPLETED is recorded when Publish
// returns: for QoS 0 once the packet is written, for QoS 1/2 on
// PUBACK/PUBCOMP. MESSAGE_RECEIVED is stamped in the client's receive
// callback, before the PUBACK goes out and before any queue; the Python
// adapter stamps after aiomqtt's incoming queue, so under a backlog the two
// differ. paho.golang honours the broker's Receive Maximum for publishes.
//
// The connection is handed to paho wrapped in a lockedConn, and a dropped
// connection is re-established and resubscribed (see those two for why).
type mqttAdapter struct {
	cfg     config.MQTT
	tls     config.TLS
	baseDir string
	rec     *events.Recorder
	noDelay bool

	clientID      string
	requestTopic  string
	replyTopic    string
	subscriptions []string     // what this role subscribes: set by Start / Listen, restored by reconnect
	clientMu      sync.RWMutex // guards client, swapped by a reconnect
	client        *paho.Client
	pending       sync.Map // correlation id -> chan Message, one per in-flight request
	oneWay        Handler
	reply         ReplyHandler
	handlers      sync.WaitGroup
	stopping      atomic.Bool
	reconnecting  atomic.Bool
}

// lockedConn makes paho.golang write every control packet under one lock:
// packets.ControlPacket.WriteTo locks a Conn that implements sync.Locker.
// A tls.Conn needs that, because a packet's buffers go out as separate
// Write calls (a plain TCPConn uses a single writev), so the pinger's
// PINGREQ could land inside a PUBACK written by the receive goroutine and
// the broker drops the client for a "malformed packet".
type lockedConn struct {
	net.Conn
	sync.Mutex
}

func newMQTTAdapter(cfg config.MQTT, t config.TLS, baseDir string, rec *events.Recorder, noDelay bool) *mqttAdapter {
	random := make([]byte, 6)
	rand.Read(random)
	clientID := "eccom-" + hex.EncodeToString(random)
	return &mqttAdapter{
		cfg:          cfg,
		tls:          t,
		baseDir:      baseDir,
		rec:          rec,
		noDelay:      noDelay,
		clientID:     clientID,
		requestTopic: cfg.BaseTopic + "/requests",
		replyTopic:   cfg.BaseTopic + "/replies/" + clientID,
	}
}

// --- sender side -----------------------------------------------------------

// Start connects and subscribes the own reply topic eagerly, which makes
// SendAndWaitReply race-free; an unused reply topic carries no traffic.
func (a *mqttAdapter) Start() error {
	a.subscriptions = []string{a.replyTopic}
	return a.connectAndSubscribe()
}

func (a *mqttAdapter) Send(m Message) error {
	return a.publish(m, "")
}

func (a *mqttAdapter) SendAndWaitReply(m Message, timeout time.Duration) (Message, error) {
	replies := make(chan Message, 1)
	a.pending.Store(m.CorrelationID, replies)
	defer a.pending.Delete(m.CorrelationID)
	// The publish runs on its own goroutine so the RTT is never floored at
	// the broker's PUBACK time (for QoS 1/2 the ack can arrive after the
	// reply); publish bounds itself with the client timeout.
	published := make(chan error, 1)
	go func() { published <- a.publish(m, a.replyTopic) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case reply := <-replies:
			return reply, nil
		case err := <-published:
			if err != nil {
				return Message{}, err
			}
			published = nil // acknowledged; keep waiting for the reply
		case <-timer.C:
			return Message{}, fmt.Errorf("no reply within %s: %w", timeout, ErrTimeout)
		}
	}
}

// publish sends m on the request topic and records SEND_COMPLETED once the
// broker (QoS 1/2) or the socket (QoS 0) took it.
func (a *mqttAdapter) publish(m Message, responseTopic string) error {
	client := a.getClient()
	if client == nil {
		return errors.New("adapter not started")
	}
	timeout := config.Seconds(a.cfg.Client.TimeoutS)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	properties := &paho.PublishProperties{
		CorrelationData: []byte(m.CorrelationID),
		User:            paho.UserProperties{{Key: SequenceProperty, Value: strconv.FormatInt(m.SequenceNumber, 10)}},
	}
	if responseTopic != "" {
		properties.ResponseTopic = responseTopic
	}
	qos := a.cfg.Client.QoS
	_, err := client.Publish(ctx, &paho.Publish{
		Topic:      a.requestTopic,
		QoS:        byte(qos),
		Payload:    m.Payload,
		Properties: properties,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("broker did not acknowledge publish within %s: %w", timeout, ErrTimeout)
		}
		return fmt.Errorf("MQTT publish failed: %w", err)
	}
	a.rec.Record(events.SendCompleted, m.CorrelationID, m.SequenceNumber, map[string]any{"qos": qos})
	return nil
}

func (a *mqttAdapter) resolveReply(packet *paho.Publish) {
	if packet.Properties == nil || packet.Properties.CorrelationData == nil {
		slog.Warn("reply without correlation data ignored")
		return
	}
	correlationID := string(packet.Properties.CorrelationData)
	waiting, ok := a.pending.Load(correlationID)
	if !ok {
		slog.Debug("uncorrelated or late reply ignored", "correlation_id", correlationID)
		return
	}
	reply := Message{
		CorrelationID:  correlationID,
		SequenceNumber: sequenceNumber(packet.Properties.User),
		Payload:        packet.Payload,
	}
	select {
	case waiting.(chan Message) <- reply:
	default: // a duplicate delivery; the first reply already won
	}
}

// --- receiver side ---------------------------------------------------------

func (a *mqttAdapter) Listen(h Handler) error {
	a.oneWay = h
	a.subscriptions = []string{a.requestTopic}
	return a.connectAndSubscribe()
}

func (a *mqttAdapter) ListenWithReply(h ReplyHandler) error {
	a.reply = h
	a.subscriptions = []string{a.requestTopic}
	return a.connectAndSubscribe()
}

func (a *mqttAdapter) Stop(ctx context.Context) error {
	a.stopping.Store(true)
	done := make(chan struct{})
	go func() {
		a.handlers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	client := a.getClient()
	if client == nil {
		return nil
	}
	return client.Disconnect(&paho.Disconnect{ReasonCode: 0})
}

// --- connection ------------------------------------------------------------

func (a *mqttAdapter) connectAndSubscribe() error {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	client, err := a.connect(ctx)
	if err != nil {
		return err
	}
	if err := a.subscribeWith(ctx, client); err != nil {
		return err
	}
	a.setClient(client)
	return nil
}

func (a *mqttAdapter) subscribeWith(ctx context.Context, client *paho.Client) error {
	options := make([]paho.SubscribeOptions, 0, len(a.subscriptions))
	for _, topic := range a.subscriptions {
		options = append(options, paho.SubscribeOptions{Topic: topic, QoS: byte(a.cfg.Client.QoS)})
	}
	if _, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: options}); err != nil {
		return fmt.Errorf("subscribing %v: %w", a.subscriptions, err)
	}
	slog.Info("MQTT subscribed", "topics", a.subscriptions, "qos", a.cfg.Client.QoS)
	return nil
}

func (a *mqttAdapter) setClient(client *paho.Client) {
	a.clientMu.Lock()
	a.client = client
	a.clientMu.Unlock()
}

func (a *mqttAdapter) getClient() *paho.Client {
	a.clientMu.RLock()
	defer a.clientMu.RUnlock()
	return a.client
}

// reconnect restores the connection and the subscriptions after the broker
// dropped the client mid-run; without it a receiver sits silent while the
// broker acknowledges every publish and discards it. One attempt loop runs at a time and
// retries until it succeeds or Stop is called.
func (a *mqttAdapter) reconnect() {
	if !a.reconnecting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer a.reconnecting.Store(false)
		for !a.stopping.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
			client, err := a.connect(ctx)
			if err == nil {
				if err = a.subscribeWith(ctx, client); err != nil {
					_ = client.Disconnect(&paho.Disconnect{ReasonCode: 0})
				}
			}
			cancel()
			if err != nil {
				slog.Warn("MQTT reconnect failed, retrying", "error", err)
				continue
			}
			a.setClient(client)
			slog.Info("MQTT reconnected")
			return
		}
	}()
}

// connect opens one broker connection, retrying until ctx ends.
func (a *mqttAdapter) connect(ctx context.Context) (*paho.Client, error) {
	address := net.JoinHostPort(a.cfg.Endpoint.Host, strconv.Itoa(a.cfg.Endpoint.Port))
	for {
		client, err := a.dialAndConnect(ctx, address)
		if err == nil {
			return client, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("cannot connect to MQTT broker at %s: %w", address, err)
		case <-time.After(connectRetry):
		}
	}
}

func (a *mqttAdapter) dialAndConnect(ctx context.Context, address string) (*paho.Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	conn.(*net.TCPConn).SetNoDelay(a.noDelay)
	if a.tls.Enabled {
		tlsConfig, err := clientTLSConfig(a.tls, a.baseDir)
		if err != nil {
			conn.Close()
			return nil, err
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tlsConn
	}
	conn = &lockedConn{Conn: conn}
	client := paho.NewClient(paho.ClientConfig{
		ClientID:          a.clientID,
		Conn:              conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){a.onPublish},
		OnClientError: func(err error) {
			if !a.stopping.Load() {
				slog.Error("MQTT connection lost; reconnecting", "error", err)
				a.reconnect()
			}
		},
		OnServerDisconnect: func(d *paho.Disconnect) {
			if !a.stopping.Load() {
				slog.Error("MQTT broker disconnected the client; reconnecting", "reason_code", d.ReasonCode)
				a.reconnect()
			}
		},
	})
	connack, err := client.Connect(ctx, &paho.Connect{
		ClientID:   a.clientID,
		CleanStart: a.cfg.Client.CleanStart,
		KeepAlive:  uint16(a.cfg.Client.KeepaliveS),
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if connack.ReasonCode != 0 {
		conn.Close()
		return nil, fmt.Errorf("CONNACK reason code %d", connack.ReasonCode)
	}
	return client, nil
}

// --- message routing -------------------------------------------------------

// onPublish runs on the client's receive goroutine: route replies to their
// waiting request, stamp requests and hand them off so a slow handler never
// blocks reading (mirrors the per-request asyncio task).
func (a *mqttAdapter) onPublish(received paho.PublishReceived) (bool, error) {
	packet := received.Packet
	if packet.Topic == a.replyTopic {
		a.resolveReply(packet)
		return true, nil
	}
	if packet.Properties == nil || packet.Properties.CorrelationData == nil {
		slog.Warn("request without correlation data ignored", "topic", packet.Topic)
		return true, nil
	}
	if a.stopping.Load() {
		return true, nil
	}
	message := Message{
		CorrelationID:  string(packet.Properties.CorrelationData),
		SequenceNumber: sequenceNumber(packet.Properties.User),
		Payload:        packet.Payload,
	}
	a.rec.Record(events.MessageReceived, message.CorrelationID, message.SequenceNumber,
		map[string]any{"size_bytes": len(message.Payload)})
	responseTopic := packet.Properties.ResponseTopic
	a.handlers.Add(1)
	go func() {
		defer a.handlers.Done()
		a.handle(message, responseTopic)
	}()
	return true, nil
}

func (a *mqttAdapter) handle(message Message, responseTopic string) {
	if a.reply == nil {
		a.oneWay(message)
		return
	}
	reply := a.reply(message)
	if responseTopic == "" {
		slog.Warn("request has no response topic; dropping reply", "correlation_id", message.CorrelationID)
		return
	}
	a.rec.Record(events.ReplySendStarted, message.CorrelationID, message.SequenceNumber,
		map[string]any{"size_bytes": len(reply)})
	_, err := a.getClient().Publish(context.Background(), &paho.Publish{
		Topic:   responseTopic,
		QoS:     byte(a.cfg.Client.QoS),
		Payload: reply,
		Properties: &paho.PublishProperties{
			CorrelationData: []byte(message.CorrelationID),
			User: paho.UserProperties{
				{Key: SequenceProperty, Value: strconv.FormatInt(message.SequenceNumber, 10)},
			},
		},
	})
	if err != nil && !a.stopping.Load() {
		slog.Warn("publishing reply failed", "correlation_id", message.CorrelationID, "error", err)
	}
}

func sequenceNumber(properties paho.UserProperties) int64 {
	if raw := properties.Get(SequenceProperty); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return parsed
		}
	}
	return -1
}
