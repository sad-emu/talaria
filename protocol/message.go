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

	// Clients requests metadata from Servers
	MsgMetaDataReq MessageType = 0x03
	// Servers respond with the first n files they are waiting to serve
	MsgMetaDataResp MessageType = 0x04

	// Clients request part of a file from Server
	MsgDataReq MessageType = 0x05
	// Server responds with the requested file chunk, or an error if it cannot be served
	MsgDataResp MessageType = 0x06

	MsgFileStatusUpdate MessageType = 0x07 // Server or Client updates the other about the status of a file (e.g. "DONE", "ERROR", "PROGRESS")
)

// maxMessageSize caps incoming messages to protect against malformed frames.
const maxMessageSize = 1024 * 1024 * 10 // 10 MiB

// HeartbeatPayload is the JSON body of both heartbeat request and response.
type HeartbeatPayload struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"ts"` // Unix nanoseconds
	NodeName  string `json:"node"`
}

// MetaDataReqPayload is the JSON body of a metadata request.
type MetaDataReqPayload struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"ts"` // Unix nanoseconds
	NodeName  string `json:"node"`
}

type MetaDataAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MetaDataEntry struct {
	UUID          string              `json:"uuid"`
	Name          string              `json:"name"`
	Connector     string              `json:"connector"`
	NumAttributes int                 `json:"num_attributes"`
	Attributes    []MetaDataAttribute `json:"attributes"`
}

type MetaDataRespPayload struct {
	ID        string          `json:"id"`
	RequestID string          `json:"req_id"` // ID of the corresponding MetaDataReq
	Timestamp int64           `json:"ts"`     // Unix nanoseconds
	NodeName  string          `json:"node"`
	Files     []MetaDataEntry `json:"files"` // List of file paths the server is waiting to serve
}

type DataReqPayload struct {
	ID        string `json:"id"`
	RequestID string `json:"req_id"` // ID of the corresponding MetaDataReq
	Timestamp int64  `json:"ts"`     // Unix nanoseconds
	NodeName  string `json:"node"`
	UUID      string `json:"uuid"` // UUID of the file being requested
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
}

type DataRespPayload struct {
	ID        string `json:"id"`
	RequestID string `json:"req_id"` // ID of the corresponding DataReq
	Timestamp int64  `json:"ts"`     // Unix nanoseconds
	NodeName  string `json:"node"`
	UUID      string `json:"uuid"` // UUID of the file being served
	Offset    int64  `json:"offset"`
	Data      []byte `json:"data"` // File chunk data, or error message if serving failed
}

type FileStatusUpdatePayload struct {
	ID        string `json:"id"`
	RequestID string `json:"req_id"` // ID of the corresponding DataReq
	Timestamp int64  `json:"ts"`     // Unix nanoseconds
	NodeName  string `json:"node"`
	UUID      string `json:"uuid"`    // UUID of the file being updated
	Status    string `json:"status"`  // Status message (e.g. "DONE", "ERROR", "PROGRESS")
	Message   string `json:"message"` // Optional human-readable message (e.g. progress percentage or error details)
}

// Handlers contains per-message callbacks used by HandleMessage.
type Handlers struct {
	HeartbeatReq     func(HeartbeatPayload) error
	HeartbeatResp    func(HeartbeatPayload) error
	MetaDataReq      func(MetaDataReqPayload) error
	MetaDataResp     func(MetaDataRespPayload) error
	DataReq          func(DataReqPayload) error
	DataResp         func(DataRespPayload) error
	FileStatusUpdate func(FileStatusUpdatePayload) error
}

// NewHeartbeatReq creates a fresh heartbeat request payload.
func NewHeartbeatReq(nodeName string) HeartbeatPayload {
	return HeartbeatPayload{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp: time.Now().UnixNano(),
		NodeName:  nodeName,
	}
}

func decodeAndHandle[T any](body []byte, name string, handler func(T) error) error {
	var payload T
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("protocol: decode %s payload: %w", name, err)
	}
	if handler == nil {
		return nil
	}
	return handler(payload)
}

// HandleMessage decodes a message body and dispatches it to the matching handler.
func HandleMessage(msgType MessageType, body []byte, handlers Handlers) error {
	switch msgType {
	case MsgHeartbeatReq:
		return decodeAndHandle(body, "heartbeat request", handlers.HeartbeatReq)
	case MsgHeartbeatResp:
		return decodeAndHandle(body, "heartbeat response", handlers.HeartbeatResp)
	case MsgMetaDataReq:
		return decodeAndHandle(body, "metadata request", handlers.MetaDataReq)
	case MsgMetaDataResp:
		return decodeAndHandle(body, "metadata response", handlers.MetaDataResp)
	case MsgDataReq:
		return decodeAndHandle(body, "data request", handlers.DataReq)
	case MsgDataResp:
		return decodeAndHandle(body, "data response", handlers.DataResp)
	case MsgFileStatusUpdate:
		return decodeAndHandle(body, "file status update", handlers.FileStatusUpdate)
	default:
		return fmt.Errorf("protocol: unsupported message type 0x%02x", byte(msgType))
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
