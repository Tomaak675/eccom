//go:build unix

package protocol

import (
	"net"
	"syscall"
	"testing"
)

func noDelayOf(t *testing.T, conn net.Conn) int {
	t.Helper()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var value int
	var sockErr error
	raw.Control(func(fd uintptr) {
		value, sockErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
	})
	if sockErr != nil {
		t.Fatal(sockErr)
	}
	return value
}

func acceptOne(t *testing.T, listener net.Listener) (server, client net.Conn) {
	t.Helper()
	dialed := make(chan net.Conn, 1)
	go func() {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Error(err)
		}
		dialed <- conn
	}()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return server, <-dialed
}

func TestNagleListenerPutsNagleBackOnAcceptedSockets(t *testing.T) {
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	server, client := acceptOne(t, plain)
	defer server.Close()
	defer client.Close()
	if noDelayOf(t, server) != 1 {
		t.Fatal("Go should accept with TCP_NODELAY on by default")
	}

	wrapped := nagleListener{plain}
	server2, client2 := acceptOne(t, wrapped)
	defer server2.Close()
	defer client2.Close()
	if noDelayOf(t, server2) != 0 {
		t.Fatal("nagleListener should clear TCP_NODELAY")
	}
}
