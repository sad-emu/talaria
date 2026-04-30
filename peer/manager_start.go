package peer

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"

	"talaria/config"
	"talaria/utils"
)

// Start launches the inbound listener and outbound dialers.
//
// It blocks while serving inbound connections.
func (m *Manager) Start(cfg *config.Config, serverTLS *tls.Config, clientTLS *tls.Config) error {
	if cfg == nil {
		return fmt.Errorf("peer manager start: config is required")
	}
	if serverTLS == nil {
		return fmt.Errorf("peer manager start: server TLS config is required")
	}
	if clientTLS == nil {
		return fmt.Errorf("peer manager start: client TLS config is required")
	}

	localName := strings.TrimSpace(cfg.Node.Name)
	listenHost := strings.TrimSpace(cfg.Node.ListenAddress)
	listenAddr := net.JoinHostPort(listenHost, fmt.Sprintf("%d", cfg.Node.ListenPort))

	for _, p := range cfg.Peers {
		d := newDialer(p, clientTLS, localName, m)
		go d.Run()
	}

	utils.Infof("[%s] peer manager listening on %s with %d configured peer(s)", localName, listenAddr, len(cfg.Peers))
	listener := newListener(listenAddr, serverTLS, localName, m)
	return listener.Listen()
}
