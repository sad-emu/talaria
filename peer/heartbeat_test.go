package peer

import (
	"net"
	"testing"
	"time"

	"talaria/protocol"
)

func TestHeartbeatSender_SendsOnInterval(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	pc := newPeerConn(client, "hb-node", func(protocol.MessageType, []byte) {})

	const interval = 50 * time.Millisecond
	go heartbeatSender(pc, "hb-node", interval)

	// Expect at least one heartbeat within a generous window.
	server.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	msgType, _, err := protocol.ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != protocol.MsgHeartbeatReq {
		t.Errorf("msgType = %v, want MsgHeartbeatReq", msgType)
	}
}

func TestHeartbeatSender_StopsOnClose(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	pc := newPeerConn(client, "hb-node", func(protocol.MessageType, []byte) {})

	done := make(chan struct{})
	go func() {
		heartbeatSender(pc, "hb-node", 50*time.Millisecond)
		close(done)
	}()

	pc.Close()

	select {
	case <-done:
		// heartbeatSender exited cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatSender did not stop after PeerConn closed")
	}
}

func TestHeartbeatSender_DefaultInterval(t *testing.T) {
	// Passing interval <= 0 should not block forever or panic.
	// We can't easily wait 30s, so just verify it starts without panicking
	// and then close the connection immediately.
	client, server := net.Pipe()
	defer server.Close()

	pc := newPeerConn(client, "hb-node", func(protocol.MessageType, []byte) {})

	done := make(chan struct{})
	go func() {
		heartbeatSender(pc, "hb-node", 0) // triggers default of 30s
		close(done)
	}()

	// Close immediately — goroutine must exit.
	pc.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatSender did not exit after close with 0 interval")
	}
}

func TestHeartbeatSender_MultipleHeartbeats(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	pc := newPeerConn(client, "hb-node", func(protocol.MessageType, []byte) {})

	const interval = 30 * time.Millisecond
	go heartbeatSender(pc, "hb-node", interval)

	server.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	count := 0
	for {
		_, _, err := protocol.ReadMessage(server)
		if err != nil {
			break
		}
		count++
	}
	if count < 2 {
		t.Errorf("expected at least 2 heartbeats, got %d", count)
	}
}
