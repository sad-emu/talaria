package main

import (
	"talaria/config"
	"talaria/peer"
	talariaTLS "talaria/tls"
	"talaria/utils"
)

// Node wires together the configuration, TLS material, and peer manager.
type Node struct {
	cfg     *config.Config
	manager *peer.Manager
}

// NewNode validates config and builds TLS configs ready for use.
func NewNode(cfg *config.Config) (*Node, error) {
	return &Node{
		cfg:     cfg,
		manager: peer.NewManager(cfg.Node.Name),
	}, nil
}

// Start runs the node: it sets up logging, loads TLS material, and starts the
// peer manager (which blocks until the listener fails).
func (n *Node) Start() error {
	utils.SetupLogger(&n.cfg.GlobalLog)
	utils.Infof("[%s] talaria starting", n.cfg.Node.Name)

	serverTLS, err := talariaTLS.BuildServerTLSConfig(&n.cfg.TLS)
	if err != nil {
		return err
	}
	clientTLS, err := talariaTLS.BuildClientTLSConfig(&n.cfg.TLS)
	if err != nil {
		return err
	}

	return n.manager.Start(n.cfg, serverTLS, clientTLS)
}
