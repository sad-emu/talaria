package peer

import (
	"log"
	"time"

	"talaria/protocol"
)

// heartbeatSender sends periodic heartbeat requests over a PeerConn.
// It runs in its own goroutine and stops when the connection closes.
func heartbeatSender(pc *PeerConn, localName string, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-pc.closed:
			return
		case <-ticker.C:
			req := protocol.NewHeartbeatReq(localName)
			if err := pc.Send(protocol.MsgHeartbeatReq, req); err != nil {
				log.Printf("[%s] heartbeat send to %s failed: %v", localName, pc.RemoteAddr(), err)
				return
			}
			log.Printf("[%s] heartbeat sent to %s (id=%s)", localName, pc.RemoteAddr(), req.ID)
		}
	}
}
