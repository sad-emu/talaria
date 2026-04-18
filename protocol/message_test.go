package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"
)

// pipeConn wraps one end of a net.Pipe() as a net.Conn.
type pipeConn struct{ net.Conn }

func TestWriteReadMessage_Roundtrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := HeartbeatPayload{ID: "abc", Timestamp: 123, NodeName: "node-1"}

	done := make(chan error, 1)
	go func() {
		done <- WriteMessage(client, MsgHeartbeatReq, payload)
	}()

	msgType, body, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage error: %v", err)
	}
	if writeErr := <-done; writeErr != nil {
		t.Fatalf("WriteMessage error: %v", writeErr)
	}
	if msgType != MsgHeartbeatReq {
		t.Errorf("msgType = %v, want MsgHeartbeatReq", msgType)
	}
	var got HeartbeatPayload
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got.ID != payload.ID || got.NodeName != payload.NodeName || got.Timestamp != payload.Timestamp {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", got, payload)
	}
}

func TestWriteReadMessage_HeartbeatResp(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := HeartbeatPayload{ID: "xyz", Timestamp: 999, NodeName: "node-2"}

	go WriteMessage(client, MsgHeartbeatResp, payload)

	msgType, _, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage error: %v", err)
	}
	if msgType != MsgHeartbeatResp {
		t.Errorf("msgType = %v, want MsgHeartbeatResp", msgType)
	}
}

func TestReadMessage_OversizePayloadHeader(t *testing.T) {
	// Build a header that claims a payload larger than maxMessageSize.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		header := [5]byte{}
		header[0] = byte(MsgHeartbeatReq)
		binary.BigEndian.PutUint32(header[1:], maxMessageSize+1)
		client.Write(header[:])
		client.Close()
	}()

	_, _, err := ReadMessage(server)
	if err == nil {
		t.Fatal("expected error for oversized payload header, got nil")
	}
}

func TestWriteMessage_OversizePayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Build a raw map whose JSON encoding exceeds maxMessageSize.
	bigData := make([]byte, maxMessageSize+1)
	for i := range bigData {
		bigData[i] = 'a'
	}
	payload := map[string]string{"data": string(bigData)}

	done := make(chan error, 1)
	go func() {
		done <- WriteMessage(client, MsgHeartbeatReq, payload)
	}()
	// Drain server so the writer isn't blocked on the pipe.
	go io.Copy(io.Discard, server)

	if err := <-done; err == nil {
		t.Fatal("expected error for oversized payload, got nil")
	}
}

func TestNewHeartbeatReq(t *testing.T) {
	before := time.Now().UnixNano()
	req := NewHeartbeatReq("my-node")
	after := time.Now().UnixNano()

	if req.NodeName != "my-node" {
		t.Errorf("NodeName = %q, want %q", req.NodeName, "my-node")
	}
	if req.ID == "" {
		t.Error("ID should not be empty")
	}
	if req.Timestamp < before || req.Timestamp > after {
		t.Errorf("Timestamp %d out of range [%d, %d]", req.Timestamp, before, after)
	}
}

func TestWriteReadMessage_MultipleMessages(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	const count = 5
	go func() {
		for i := 0; i < count; i++ {
			p := HeartbeatPayload{ID: "id", Timestamp: int64(i), NodeName: "node"}
			WriteMessage(client, MsgHeartbeatReq, p)
		}
		client.Close()
	}()

	received := 0
	for {
		_, _, err := ReadMessage(server)
		if err != nil {
			break
		}
		received++
	}
	if received != count {
		t.Errorf("received %d messages, want %d", received, count)
	}
}

// TestMessageTypeConstants ensures the constants have the expected wire values.
func TestMessageTypeConstants(t *testing.T) {
	if MsgHeartbeatReq != 0x01 {
		t.Errorf("MsgHeartbeatReq = 0x%02x, want 0x01", MsgHeartbeatReq)
	}
	if MsgHeartbeatResp != 0x02 {
		t.Errorf("MsgHeartbeatResp = 0x%02x, want 0x02", MsgHeartbeatResp)
	}
}

// TestWriteMessage_ClosedConn ensures WriteMessage returns an error on a closed connection.
func TestWriteMessage_ClosedConn(t *testing.T) {
	client, server := net.Pipe()
	server.Close()
	err := WriteMessage(client, MsgHeartbeatReq, HeartbeatPayload{})
	client.Close()
	if err == nil {
		t.Fatal("expected error writing to closed connection, got nil")
	}
}

// TestReadMessage_ClosedConn ensures ReadMessage returns an error on a closed connection.
func TestReadMessage_ClosedConn(t *testing.T) {
	client, server := net.Pipe()
	client.Close()
	_, _, err := ReadMessage(server)
	server.Close()
	if err == nil {
		t.Fatal("expected error reading from closed connection, got nil")
	}
}

// TestWriteMessage_WireFormat verifies the exact bytes on the wire.
func TestWriteMessage_WireFormat(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := HeartbeatPayload{ID: "1", Timestamp: 0, NodeName: "n"}

	var wireBytes []byte
	done := make(chan struct{})
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, server)
		wireBytes = buf.Bytes()
		close(done)
	}()

	WriteMessage(client, MsgHeartbeatReq, payload)
	client.Close()
	<-done

	if len(wireBytes) < 5 {
		t.Fatalf("too few bytes: %d", len(wireBytes))
	}
	if wireBytes[0] != byte(MsgHeartbeatReq) {
		t.Errorf("first byte = 0x%02x, want 0x%02x", wireBytes[0], MsgHeartbeatReq)
	}
	declaredLen := binary.BigEndian.Uint32(wireBytes[1:5])
	if int(declaredLen) != len(wireBytes)-5 {
		t.Errorf("declared len %d != actual body len %d", declaredLen, len(wireBytes)-5)
	}
}
