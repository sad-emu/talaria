package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
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

// TestHandleMessage_NewMessageTypes tests all new message types.
func TestHandleMessage_NewMessageTypes(t *testing.T) {
	tests := []struct {
		name     string
		msgType  MessageType
		payload  any
		handlers func(*Handlers, func(any))
	}{
		{
			name:    "metadata request",
			msgType: MsgMetaDataReq,
			payload: MetaDataReqPayload{
				ID:        "meta-req-1",
				Timestamp: 123,
				NodeName:  "client-a",
			},
			handlers: func(h *Handlers, set func(any)) {
				h.MetaDataReq = func(p MetaDataReqPayload) error {
					set(p)
					return nil
				}
			},
		},
		{
			name:    "metadata response",
			msgType: MsgMetaDataResp,
			payload: MetaDataRespPayload{
				ID:        "meta-resp-1",
				RequestID: "meta-req-1",
				Timestamp: 124,
				NodeName:  "server-a",
				Files: []MetaDataEntry{
					{
						UUID:          "file-1",
						Name:          "example.dat",
						Connector:     "outbound",
						NumAttributes: 1,
						Attributes: []MetaDataAttribute{
							{Key: "content-type", Value: "application/octet-stream"},
						},
					},
				},
			},
			handlers: func(h *Handlers, set func(any)) {
				h.MetaDataResp = func(p MetaDataRespPayload) error {
					set(p)
					return nil
				}
			},
		},
		{
			name:    "data request",
			msgType: MsgDataReq,
			payload: DataReqPayload{
				ID:        "data-req-1",
				RequestID: "meta-req-1",
				Timestamp: 125,
				NodeName:  "client-a",
				UUID:      "file-1",
				Offset:    1024,
				Length:    4096,
			},
			handlers: func(h *Handlers, set func(any)) {
				h.DataReq = func(p DataReqPayload) error {
					set(p)
					return nil
				}
			},
		},
		{
			name:    "data response",
			msgType: MsgDataResp,
			payload: DataRespPayload{
				ID:        "data-resp-1",
				RequestID: "data-req-1",
				Timestamp: 126,
				NodeName:  "server-a",
				UUID:      "file-1",
				Offset:    1024,
				Data:      []byte("chunk-data"),
			},
			handlers: func(h *Handlers, set func(any)) {
				h.DataResp = func(p DataRespPayload) error {
					set(p)
					return nil
				}
			},
		},
		{
			name:    "file status update",
			msgType: MsgFileStatusUpdate,
			payload: FileStatusUpdatePayload{
				ID:        "status-1",
				RequestID: "data-req-1",
				Timestamp: 127,
				NodeName:  "server-a",
				UUID:      "file-1",
				Status:    "PROGRESS",
				Message:   "50%",
			},
			handlers: func(h *Handlers, set func(any)) {
				h.FileStatusUpdate = func(p FileStatusUpdatePayload) error {
					set(p)
					return nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			writeErr := make(chan error, 1)
			go func() {
				writeErr <- WriteMessage(server, tt.msgType, tt.payload)
			}()

			msgType, body, err := ReadMessage(client)
			if err != nil {
				t.Fatalf("ReadMessage() error = %v", err)
			}
			if err := <-writeErr; err != nil {
				t.Fatalf("WriteMessage() error = %v", err)
			}
			if msgType != tt.msgType {
				t.Fatalf("ReadMessage() type = %v, want %v", msgType, tt.msgType)
			}

			var got any
			handlers := Handlers{}
			tt.handlers(&handlers, func(v any) { got = v })

			if err := HandleMessage(msgType, body, handlers); err != nil {
				t.Fatalf("HandleMessage() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.payload) {
				t.Fatalf("decoded payload = %#v, want %#v", got, tt.payload)
			}
		})
	}
}

func TestHandleMessage_UnsupportedType(t *testing.T) {
	err := HandleMessage(MessageType(0xff), []byte(`{}`), Handlers{})
	if err == nil {
		t.Fatal("HandleMessage() error = nil, want non-nil")
	}
}

func TestHandleMessage_InvalidJSON(t *testing.T) {
	err := HandleMessage(MsgDataReq, []byte(`{"id":`), Handlers{})
	if err == nil {
		t.Fatal("HandleMessage() error = nil, want non-nil")
	}
}

func TestHandleMessage_HandlerError(t *testing.T) {
	wantErr := errors.New("handler failed")

	err := HandleMessage(MsgFileStatusUpdate, []byte(`{
		"id":"status-1",
		"req_id":"data-req-1",
		"ts":127,
		"node":"server-a",
		"uuid":"file-1",
		"status":"ERROR",
		"message":"disk full"
	}`), Handlers{
		FileStatusUpdate: func(FileStatusUpdatePayload) error {
			return wantErr
		},
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleMessage() error = %v, want %v", err, wantErr)
	}
}

func TestHandleMessage_NilHandler(t *testing.T) {
	// Ensure nil handlers don't panic.
	err := HandleMessage(MsgMetaDataReq, []byte(`{
		"id":"meta-req-1",
		"ts":123,
		"node":"client-a"
	}`), Handlers{})
	if err != nil {
		t.Fatalf("HandleMessage() with nil handler error = %v, want nil", err)
	}
}
