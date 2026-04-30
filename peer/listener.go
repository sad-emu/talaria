package peer

import (
	"crypto/tls"
	"fmt"
	"net"

	"talaria/protocol"
	"talaria/utils"
)

// Listener accepts inbound TLS connections and hands them to the Manager.
type Listener struct {
	address   string
	tlsCfg    *tls.Config
	localName string
	manager   *Manager
}

// newListener creates a Listener.
func newListener(address string, tlsCfg *tls.Config, localName string, mgr *Manager) *Listener {
	return &Listener{
		address:   address,
		tlsCfg:    tlsCfg,
		localName: localName,
		manager:   mgr,
	}
}

// Listen starts accepting connections.  Blocks until the listener fails.
func (l *Listener) Listen() error {
	ln, err := tls.Listen("tcp", l.address, l.tlsCfg)
	if err != nil {
		return fmt.Errorf("listener: bind %s: %w", l.address, err)
	}
	utils.Infof("[%s] listening on %s", l.localName, l.address)
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("listener: accept: %w", err)
		}
		go l.handleInbound(conn)
	}
}

func (l *Listener) handleInbound(conn net.Conn) {
	utils.Infof("[%s] inbound connection from %s", l.localName, conn.RemoteAddr())
	var pc *PeerConn
	pc = newPeerConn(conn, l.localName, func(msgType protocol.MessageType, body []byte) {
		handleMessage(pc, l.localName, msgType, body)
	})
	l.manager.register(pc)
	defer l.manager.unregister(pc)
	pc.runReadLoop()
}
