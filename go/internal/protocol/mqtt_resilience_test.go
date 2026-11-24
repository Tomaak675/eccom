package protocol

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// slowConn writes one buffer per call and yields between calls, the way a
// tls.Conn lets a multi-buffer packet interleave with another goroutine's
// packet; each single Write is atomic, as in tls.Conn.
type slowConn struct {
	net.Conn
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *slowConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	runtime.Gosched()
	return c.buf.Write(b)
}

func TestLockedConnKeepsConcurrentPacketsWhole(t *testing.T) {
	sink := &slowConn{}
	conn := &lockedConn{Conn: sink}
	const writers, rounds = 8, 50
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				puback := packets.NewControlPacket(packets.PUBACK)
				puback.Content.(*packets.Puback).PacketID = id
				if _, err := puback.WriteTo(conn); err != nil {
					t.Error(err)
				}
				if _, err := packets.NewControlPacket(packets.PINGREQ).WriteTo(conn); err != nil {
					t.Error(err)
				}
			}
		}(uint16(i + 1))
	}
	wg.Wait()
	reader := bytes.NewReader(sink.buf.Bytes())
	parsed := 0
	for reader.Len() > 0 {
		if _, err := packets.ReadPacket(reader); err != nil {
			t.Fatalf("stream corrupted after %d packets: %v", parsed, err)
		}
		parsed++
	}
	if parsed != writers*rounds*2 {
		t.Fatalf("expected %d packets, got %d", writers*rounds*2, parsed)
	}
}

func TestReceiverResubscribesAfterLosingItsConnection(t *testing.T) {
	host, port := brokerAddress(t)
	adapter, _ := newTestMQTTAdapter(t, host, port)
	received := make(chan Message, 8)
	if err := adapter.Listen(func(m Message) { received <- m }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Stop(context.Background()) })
	sender := connectSender(t, host, port, adapter.cfg.BaseTopic+"/replies")

	first := adapter.getClient()
	_ = first.Disconnect(&paho.Disconnect{ReasonCode: 0}) // the broker's drop, as seen by paho
	adapter.reconnect()
	deadline := time.Now().Add(10 * time.Second)
	for adapter.getClient() == first && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if adapter.getClient() == first {
		t.Fatal("the adapter did not reconnect")
	}

	publishRequest(t, sender, adapter.requestTopic, adapter.cfg.BaseTopic+"/replies", "after-drop", 1, "hello")
	select {
	case m := <-received:
		if m.CorrelationID != "after-drop" {
			t.Fatalf("unexpected message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message after the reconnect: the subscription was not restored")
	}
}
