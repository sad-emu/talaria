package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// MessageType identifies the kind of message on the wire.
type MessageType uint8

const (
	MsgHeartbeatReq  MessageType = 0x01
	MsgHeartbeatResp MessageType = 0x02
)

// maxMessageSize caps incoming messages to protect against malformed frames.
const maxMessageSize = 1 << 20 // 1 MiB

// HeartbeatPayload is the JSON body of both heartbeat request and response.
type HeartbeatPayload struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"ts"` // Unix nanoseconds
	NodeName  string `json:"node"`
}

// NewHeartbeatReq creates a fresh heartbeat request payload.
func NewHeartbeatReq(nodeName string) HeartbeatPayload {
	return HeartbeatPayload{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp: time.Now().UnixNano(),
		NodeName:  nodeName,
	}
}

// WriteMessage encodes and writes a framed message to conn.
//
// Wire format: [1 byte type][4 byte length, big-endian uint32][N byte JSON payload]
func WriteMessage(conn net.Conn, msgType MessageType, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("protocol: marshal: %w", err)
	}
	if len(body) > maxMessageSize {
		return fmt.Errorf("protocol: payload too large (%d bytes)", len(body))
	}
	header := [5]byte{}
	header[0] = byte(msgType)
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	if _, err := conn.Write(header[:]); err != nil {
		return fmt.Errorf("protocol: write header: %w", err)
	}
	if _, err := conn.Write(body); err != nil {
		return fmt.Errorf("protocol: write body: %w", err)
	}
	return nil
}

// ReadMessage reads and returns one framed message from conn.
func ReadMessage(conn net.Conn) (MessageType, []byte, error) {
	header := [5]byte{}
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, fmt.Errorf("protocol: read header: %w", err)
	}
	msgType := MessageType(header[0])
	length := binary.BigEndian.Uint32(header[1:])
	if length > maxMessageSize {
		return 0, nil, fmt.Errorf("protocol: declared payload size %d exceeds limit", length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil, fmt.Errorf("protocol: read body: %w", err)
	}
	return msgType, body, nil
}
