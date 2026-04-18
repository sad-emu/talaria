package peer

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"talaria/protocol"
)

// ---------------------------------------------------------------------------
// PeerConn tests
// ---------------------------------------------------------------------------

func TestPeerConn_Send_Receive(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	received := make(chan protocol.MessageType, 1)
	pc := newPeerConn(server, "test-node", func(msgType protocol.MessageType, _ []byte) {
		received <- msgType
	})
	go pc.runReadLoop()

	payload := protocol.HeartbeatPayload{ID: "1", Timestamp: 42, NodeName: "sender"}
	if err := protocol.WriteMessage(client, protocol.MsgHeartbeatReq, payload); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	select {
	case mt := <-received:
		if mt != protocol.MsgHeartbeatReq {
			t.Errorf("got msgType %v, want MsgHeartbeatReq", mt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPeerConn_Close_Idempotent(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	pc := newPeerConn(server, "test-node", func(protocol.MessageType, []byte) {})

	// Calling Close multiple times must not panic.
	pc.Close()
	pc.Close()
	pc.Close()
}

func TestPeerConn_IsClosed(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	pc := newPeerConn(server, "test-node", func(protocol.MessageType, []byte) {})
	if pc.IsClosed() {
		t.Fatal("expected IsClosed = false before Close")
	}
	pc.Close()
	if !pc.IsClosed() {
		t.Fatal("expected IsClosed = true after Close")
	}
}

func TestPeerConn_RemoteAddr(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	pc := newPeerConn(server, "test-node", func(protocol.MessageType, []byte) {})
	addr := pc.RemoteAddr()
	if addr == "" {
		t.Error("RemoteAddr should not be empty")
	}
}

func TestPeerConn_ReadLoop_ExitsOnClose(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	pc := newPeerConn(server, "test-node", func(protocol.MessageType, []byte) {})
	done := make(chan struct{})
	go func() {
		pc.runReadLoop()
		close(done)
	}()

	pc.Close()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("runReadLoop did not exit after Close")
	}
}

func TestPeerConn_Send(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	pc := newPeerConn(client, "test-node", func(protocol.MessageType, []byte) {})

	done := make(chan error, 1)
	go func() {
		_, _, err := protocol.ReadMessage(server)
		done <- err
	}()

	payload := protocol.HeartbeatPayload{ID: "42", Timestamp: 1, NodeName: "node"}
	if err := pc.Send(protocol.MsgHeartbeatResp, payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if err := <-done; err != nil {
		t.Errorf("ReadMessage on server side: %v", err)
	}
}

// ---------------------------------------------------------------------------
// handleMessage tests
// ---------------------------------------------------------------------------

func TestHandleMessage_HeartbeatReq_SendsResp(t *testing.T) {
	// Use net.Pipe: the handler sends a response back to the other end.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	localName := "handler-node"
	var pc *PeerConn
	pc = newPeerConn(server, localName, func(msgType protocol.MessageType, body []byte) {
		handleMessage(pc, localName, msgType, body)
	})
	go pc.runReadLoop()

	// Send a heartbeat request from client.
	req := protocol.HeartbeatPayload{ID: "req-1", Timestamp: time.Now().UnixNano(), NodeName: "sender"}
	if err := protocol.WriteMessage(client, protocol.MsgHeartbeatReq, req); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// The handler should send back a MsgHeartbeatResp.
	msgType, body, err := protocol.ReadMessage(client)
	if err != nil {
		t.Fatalf("ReadMessage (response): %v", err)
	}
	if msgType != protocol.MsgHeartbeatResp {
		t.Errorf("response msgType = %v, want MsgHeartbeatResp", msgType)
	}
	var resp protocol.HeartbeatPayload
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
	if resp.ID != req.ID {
		t.Errorf("resp.ID = %q, want %q", resp.ID, req.ID)
	}
	if resp.NodeName != localName {
		t.Errorf("resp.NodeName = %q, want %q", resp.NodeName, localName)
	}
}

func TestHandleMessage_HeartbeatResp_NoResponse(t *testing.T) {
	// A heartbeat response should be logged but not cause another message to be sent.
	client, server := net.Pipe()
	defer server.Close()

	localName := "resp-node"
	var pc *PeerConn
	pc = newPeerConn(server, localName, func(msgType protocol.MessageType, body []byte) {
		handleMessage(pc, localName, msgType, body)
	})
	go pc.runReadLoop()

	resp := protocol.HeartbeatPayload{ID: "r1", Timestamp: time.Now().UnixNano(), NodeName: "other"}
	if err := protocol.WriteMessage(client, protocol.MsgHeartbeatResp, resp); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// No response should arrive; set a read deadline and confirm timeout.
	client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, _, err := protocol.ReadMessage(client)
	if err == nil {
		t.Error("expected timeout/error reading after heartbeat resp, got nil")
	}
	client.Close()
}

func TestHandleMessage_UnknownType_NoResponse(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	localName := "unknown-node"
	var pc *PeerConn
	pc = newPeerConn(server, localName, func(msgType protocol.MessageType, body []byte) {
		handleMessage(pc, localName, msgType, body)
	})
	go pc.runReadLoop()

	// Write a raw message with an unknown type directly onto the pipe.
	header := [5]byte{0xFF, 0, 0, 0, 2}
	client.Write(header[:])
	client.Write([]byte("{}"))

	// No response expected.
	client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, _, err := protocol.ReadMessage(client)
	if err == nil {
		t.Error("expected timeout/error, got nil")
	}
	client.Close()
}
