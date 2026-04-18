package peer

import (
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"time"

	"talaria/config"
	"talaria/protocol"
)

const (
	dialInitialBackoff = 2 * time.Second
	dialMaxBackoff     = 5 * time.Minute
	dialBackoffFactor  = 2.0
	dialJitterFraction = 0.2
)

// Dialer manages an outbound connection to one configured peer.  It
// reconnects automatically with exponential backoff.
type Dialer struct {
	peerCfg   config.PeerConfig
	tlsCfg    *tls.Config
	localName string
	manager   *Manager
}

// newDialer creates a Dialer for the given peer.
func newDialer(peerCfg config.PeerConfig, tlsCfg *tls.Config, localName string, mgr *Manager) *Dialer {
	return &Dialer{
		peerCfg:   peerCfg,
		tlsCfg:    tlsCfg,
		localName: localName,
		manager:   mgr,
	}
}

// Run dials the peer and reconnects forever.  Call in a goroutine.
func (d *Dialer) Run() {
	backoff := dialInitialBackoff
	for {
		if err := d.connect(); err != nil {
			log.Printf("[%s] connection to %s lost: %v — retrying in %v",
				d.localName, d.peerCfg.Name, err, backoff)
		}
		time.Sleep(jitter(backoff))
		backoff = nextBackoff(backoff)
	}
}

func (d *Dialer) connect() error {
	addr := net.JoinHostPort(d.peerCfg.Address, fmt.Sprintf("%d", d.peerCfg.Port))
	// Clone tlsCfg so we can set ServerName per-peer without races.
	tc := d.tlsCfg.Clone()
	tc.ServerName = d.peerCfg.Address

	log.Printf("[%s] dialing %s (%s)", d.localName, d.peerCfg.Name, addr)
	rawConn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial tcp: %w", err)
	}
	tlsConn := tls.Client(rawConn, tc)
	if err := tlsConn.Handshake(); err != nil {
		rawConn.Close()
		return fmt.Errorf("TLS handshake: %w", err)
	}
	log.Printf("[%s] connected to %s (%s)", d.localName, d.peerCfg.Name, tlsConn.RemoteAddr())

	var pc *PeerConn
	pc = newPeerConn(tlsConn, d.localName, func(msgType protocol.MessageType, body []byte) {
		handleMessage(pc, d.localName, msgType, body)
	})
	d.manager.register(pc)
	defer d.manager.unregister(pc)

	// Send heartbeats from this side as the initiating peer.
	go heartbeatSender(pc, d.localName, d.peerCfg.HeartbeatInterval)
	pc.runReadLoop() // blocks until connection dies
	return nil
}

func nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * dialBackoffFactor)
	if next > dialMaxBackoff {
		next = dialMaxBackoff
	}
	return next
}

func jitter(d time.Duration) time.Duration {
	jitterRange := float64(d) * dialJitterFraction
	offset := time.Duration((rand.Float64()*2 - 1) * jitterRange)
	result := d + offset
	if result < time.Second {
		result = time.Second
	}
	return result
}

// resetBackoff returns the initial backoff value (used after a successful
// long-lived connection to avoid punishing quick reconnects after a real
// outage).
func resetBackoff() time.Duration {
	return dialInitialBackoff
}

// Ensure resetBackoff is used to avoid "declared and not used" — it will be
// called in a future version when we track connection uptime.
var _ = resetBackoff

// ensure math import is used
var _ = math.MaxFloat64
