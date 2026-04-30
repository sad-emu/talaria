package peer

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"talaria/protocol"
	"talaria/utils"
)

// PeerConn wraps a TLS connection to a single peer and runs a read loop.
type PeerConn struct {
	conn     net.Conn
	nodeName string // local node name, for logging

	closeOnce sync.Once
	closed    chan struct{}

	// onMessage is called for every inbound message.
	onMessage func(msgType protocol.MessageType, body []byte)
}

// newPeerConn creates a PeerConn.  onMessage is called from the read goroutine;
// it must be non-nil.
func newPeerConn(conn net.Conn, nodeName string, onMessage func(protocol.MessageType, []byte)) *PeerConn {
	return &PeerConn{
		conn:      conn,
		nodeName:  nodeName,
		closed:    make(chan struct{}),
		onMessage: onMessage,
	}
}

// Send encodes and writes a message to the peer.
func (pc *PeerConn) Send(msgType protocol.MessageType, payload any) error {
	return protocol.WriteMessage(pc.conn, msgType, payload)
}

// runReadLoop reads messages until the connection closes.  Call in a goroutine.
func (pc *PeerConn) runReadLoop() {
	defer pc.Close()
	for {
		msgType, body, err := protocol.ReadMessage(pc.conn)
		if err != nil {
			select {
			case <-pc.closed:
				// Closed intentionally — no noise.
			default:
				utils.Errorf("[%s] read error: %v", pc.nodeName, err)
			}
			return
		}
		pc.onMessage(msgType, body)
	}
}

// Close shuts down the connection idempotently.
func (pc *PeerConn) Close() {
	pc.closeOnce.Do(func() {
		close(pc.closed)
		pc.conn.Close()
	})
}

// RemoteAddr returns the peer's network address string.
func (pc *PeerConn) RemoteAddr() string {
	return pc.conn.RemoteAddr().String()
}

// IsClosed reports whether the connection has been closed.
func (pc *PeerConn) IsClosed() bool {
	select {
	case <-pc.closed:
		return true
	default:
		return false
	}
}

// handleMessage dispatches an inbound message to the correct handler.
func handleMessage(pc *PeerConn, localName string, msgType protocol.MessageType, body []byte) {
	switch msgType {
	case protocol.MsgHeartbeatReq:
		var req protocol.HeartbeatPayload
		if err := json.Unmarshal(body, &req); err != nil {
			utils.Errorf("[%s] bad heartbeat req from %s: %v", localName, pc.RemoteAddr(), err)
			return
		}
		rtt := time.Duration(time.Now().UnixNano()-req.Timestamp) * time.Nanosecond
		utils.Debugf("[%s] heartbeat req from %s (node=%s id=%s rtt=%v)", localName, pc.RemoteAddr(), req.NodeName, req.ID, rtt)
		resp := protocol.HeartbeatPayload{
			ID:        req.ID,
			Timestamp: time.Now().UnixNano(),
			NodeName:  localName,
		}
		if err := pc.Send(protocol.MsgHeartbeatResp, resp); err != nil {
			utils.Errorf("[%s] heartbeat resp send error: %v", localName, err)
		}

	case protocol.MsgHeartbeatResp:
		var resp protocol.HeartbeatPayload
		if err := json.Unmarshal(body, &resp); err != nil {
			utils.Errorf("[%s] bad heartbeat resp from %s: %v", localName, pc.RemoteAddr(), err)
			return
		}
		rtt := time.Duration(time.Now().UnixNano()-resp.Timestamp) * time.Nanosecond
		utils.Debugf("[%s] heartbeat resp from %s (node=%s id=%s rtt=%v)", localName, pc.RemoteAddr(), resp.NodeName, resp.ID, rtt)

	default:
		utils.Errorf("[%s] unknown message type 0x%02x from %s", localName, msgType, pc.RemoteAddr())
	}
}
