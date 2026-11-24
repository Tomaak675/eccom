package protocol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"eccom/internal/config"
	"eccom/internal/events"
)

// brokerAddress is a real MQTT broker to test against, host:port in
// ECCOM_TEST_MQTT_BROKER; the tests skip without one.
func brokerAddress(t *testing.T) (string, int) {
	t.Helper()
	address := os.Getenv("ECCOM_TEST_MQTT_BROKER")
	if address == "" {
		t.Skip("ECCOM_TEST_MQTT_BROKER not set")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

type testSender struct {
	client   *paho.Client
	received chan *paho.Publish
}

func connectSender(t *testing.T, host string, port int, replyTopic string) *testSender {
	t.Helper()
	conn, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	sender := &testSender{received: make(chan *paho.Publish, 8)}
	sender.client = paho.NewClient(paho.ClientConfig{
		ClientID: "eccom-test-sender",
		Conn:     conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) { sender.received <- pr.Packet; return true, nil },
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sender.client.Connect(ctx, &paho.Connect{ClientID: "eccom-test-sender", CleanStart: true, KeepAlive: 30}); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.client.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: replyTopic, QoS: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sender.client.Disconnect(&paho.Disconnect{}) })
	return sender
}

func newTestMQTTAdapter(t *testing.T, host string, port int) (*mqttAdapter, *events.Recorder) {
	t.Helper()
	random := make([]byte, 4)
	rand.Read(random)
	rec := events.NewRecorder("cloud", filepath.Join(t.TempDir(), "cloud-events.jsonl"))
	t.Cleanup(func() { _ = rec.Close() }) // the writer goroutine holds the file open
	cfg := config.MQTT{
		Endpoint:  config.Endpoint{Host: host, Port: port},
		Client:    config.MQTTClient{QoS: 1, KeepaliveS: 30, CleanStart: true, TimeoutS: 5},
		BaseTopic: "eccom-test-" + hex.EncodeToString(random),
	}
	return newMQTTAdapter(cfg, config.TLS{}, "", rec, true), rec
}

func publishRequest(t *testing.T, sender *testSender, topic, replyTopic, correlationID string, sequence int, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := sender.client.Publish(ctx, &paho.Publish{
		Topic:   topic,
		QoS:     1,
		Payload: []byte(payload),
		Properties: &paho.PublishProperties{
			CorrelationData: []byte(correlationID),
			ResponseTopic:   replyTopic,
			User:            paho.UserProperties{{Key: SequenceProperty, Value: strconv.Itoa(sequence)}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMQTTRequestResponseThroughABroker(t *testing.T) {
	host, port := brokerAddress(t)
	a, rec := newTestMQTTAdapter(t, host, port)
	if err := a.ListenWithReply(func(m Message) []byte { return append([]byte("re:"), m.Payload...) }); err != nil {
		t.Fatal(err)
	}
	replyTopic := a.cfg.BaseTopic + "/replies/eccom-test-sender"
	sender := connectSender(t, host, port, replyTopic)

	publishRequest(t, sender, a.requestTopic, replyTopic, "corr-1", 3, "ping")
	select {
	case reply := <-sender.received:
		if string(reply.Payload) != "re:ping" || string(reply.Properties.CorrelationData) != "corr-1" ||
			reply.Properties.User.Get(SequenceProperty) != "3" || reply.QoS != 1 {
			t.Errorf("reply %+v props %+v", reply, reply.Properties)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	recorded := recordedEvents(t, rec, rec.Path())
	if len(recorded) != 2 || recorded[0].EventType != events.MessageReceived || recorded[1].EventType != events.ReplySendStarted {
		t.Fatalf("events %+v", recorded)
	}
	if recorded[0].SequenceNumber != 3 || recorded[0].Metadata["size_bytes"] != float64(4) || recorded[1].Metadata["size_bytes"] != float64(7) {
		t.Errorf("events %+v", recorded)
	}
}

func TestMQTTOneWayDeliversToTheHandler(t *testing.T) {
	host, port := brokerAddress(t)
	a, rec := newTestMQTTAdapter(t, host, port)
	handled := make(chan Message, 1)
	if err := a.Listen(func(m Message) { handled <- m }); err != nil {
		t.Fatal(err)
	}
	sender := connectSender(t, host, port, a.cfg.BaseTopic+"/replies/eccom-test-sender")
	publishRequest(t, sender, a.requestTopic, "", "corr-2", 9, "fire")
	select {
	case m := <-handled:
		if m.CorrelationID != "corr-2" || m.SequenceNumber != 9 || string(m.Payload) != "fire" {
			t.Errorf("message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
	a.Stop(context.Background())
	if recorded := recordedEvents(t, rec, rec.Path()); len(recorded) != 1 || recorded[0].EventType != events.MessageReceived {
		t.Errorf("events %+v", recorded)
	}
}
