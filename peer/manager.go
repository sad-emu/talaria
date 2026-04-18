package peer

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"

	"talaria/config"
)

// Manager owns the listener and all outbound dialers for this node.
type Manager struct {
	localName string

	mu    sync.Mutex
	conns map[*PeerConn]struct{}
}

// NewManager creates a Manager for the given node name.
func NewManager(localName string) *Manager {
	return &Manager{
		localName: localName,
		conns:     make(map[*PeerConn]struct{}),
	}
}

// Start binds the listener and launches all outbound dialers.  It blocks until
// the listener returns an error (which is fatal).
func (m *Manager) Start(cfg *config.Config, serverTLS *tls.Config, clientTLS *tls.Config) error {
	// Launch outbound dialers in background goroutines.
	for _, p := range cfg.Peers {
		d := newDialer(p, clientTLS, m.localName, m)
		log.Printf("[%s] starting dialer for peer %s (%s)", m.localName, p.Name, net.JoinHostPort(p.Address, fmt.Sprintf("%d", p.Port)))
		go d.Run()
	}

	// Start listener — blocks.
	addr := fmt.Sprintf("%s:%d", cfg.Node.ListenAddress, cfg.Node.ListenPort)
	ln := newListener(addr, serverTLS, m.localName, m)
	return ln.Listen()
}

// register adds a connection to the active set.
func (m *Manager) register(pc *PeerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[pc] = struct{}{}
	log.Printf("[%s] peer connected: %s (total active: %d)", m.localName, pc.RemoteAddr(), len(m.conns))
}

// unregister removes a connection from the active set.
func (m *Manager) unregister(pc *PeerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, pc)
	log.Printf("[%s] peer disconnected: %s (total active: %d)", m.localName, pc.RemoteAddr(), len(m.conns))
}

// ActiveCount returns the number of currently connected peers.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.conns)
}
