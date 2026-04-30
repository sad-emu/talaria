package peer

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"talaria/connector"
	"talaria/protocol"
	"talaria/utils"
)

type PeerContext struct {
	DN string
	IP net.IP
}

type Manager struct {
	mu    sync.RWMutex
	conns map[*PeerConn]struct{}

	name string

	// Connector access policies (loaded from YAML dir or set directly).
	Policies *connector.PolicySet

	ResolveFileConnector func(ctx context.Context, fileUUID string) (string, error)

	OnMetaReq func(ctx context.Context, peer PeerContext, p protocol.MetaReqPayload) error
	OnDataReq func(ctx context.Context, peer PeerContext, p protocol.DataReqPayload) error
}

// NewManager requires an explicit manager name.
func NewManager(name string) *Manager {
	return &Manager{
		conns: make(map[*PeerConn]struct{}),
		name:  strings.TrimSpace(name),
	}
}

func (m *Manager) Name() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.name
}

func (m *Manager) SetName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name = strings.TrimSpace(name)
}

func (m *Manager) register(pc *PeerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conns == nil {
		m.conns = make(map[*PeerConn]struct{})
	}
	m.conns[pc] = struct{}{}
}

func (m *Manager) unregister(pc *PeerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, pc)
}

// SetPolicies replaces the runtime policy set.
func (m *Manager) SetPolicies(ps *connector.PolicySet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Policies = ps
}

// LoadPoliciesFromDir loads per-connector YAML files and builds runtime policy set.
func (m *Manager) LoadPoliciesFromDir(dir string) error {
	cfgs, err := connector.LoadPoliciesFromDir(dir)
	if err != nil {
		return err
	}
	ps, err := connector.NewPolicySet(cfgs)
	if err != nil {
		return err
	}
	m.SetPolicies(ps)
	return nil
}

// AllowedConnectors returns connector names this peer is currently authorized to request.
func (m *Manager) AllowedConnectors(peer PeerContext) ([]string, error) {
	m.mu.RLock()
	ps := m.Policies
	m.mu.RUnlock()

	if ps == nil {
		return nil, fmt.Errorf("manager: policies not configured")
	}
	return ps.AllowedConnectors(peer.DN, peer.IP), nil
}

// AuthorizeConnector checks whether a peer can access a connector.
func (m *Manager) AuthorizeConnector(peer PeerContext, connectorName string) error {
	m.mu.RLock()
	ps := m.Policies
	m.mu.RUnlock()

	if ps == nil {
		return fmt.Errorf("manager: policies not configured")
	}

	connectorName = strings.TrimSpace(connectorName)
	if connectorName == "" {
		return fmt.Errorf("manager: empty connector name")
	}

	dec := ps.Evaluate(connectorName, peer.DN, peer.IP)
	if !dec.Allowed {
		return fmt.Errorf("manager: connector %q denied: %s", connectorName, dec.Reason)
	}
	return nil
}

// ProcessIncoming assumes protocol framing/syntax validation is done in protocol.HandleMessage.
// It applies semantic/authorization checks.
func (m *Manager) ProcessIncoming(
	ctx context.Context,
	peer PeerContext,
	msgType protocol.MessageType,
	body []byte,
) (any, error) {
	decoded, err := protocol.HandleMessage(msgType, body, protocol.Handlers{})
	if err != nil {
		return nil, err
	}

	switch p := decoded.(type) {
	case protocol.MetaReqPayload:
		if err := m.handleMetaReq(ctx, peer, p); err != nil {
			return nil, err
		}
	case protocol.DataReqPayload:
		if err := m.handleDataReq(ctx, peer, p); err != nil {
			return nil, err
		}
	}

	return decoded, nil
}

func (m *Manager) handleMetaReq(ctx context.Context, peer PeerContext, p protocol.MetaReqPayload) error {
	requestType := strings.ToUpper(strings.TrimSpace(p.RequestType))
	requestConnector := strings.TrimSpace(p.RequestConnector)

	switch requestType {
	case "FILES":
		// Optional for now: if specified, enforce connector access.
		if requestConnector != "" {
			if err := m.AuthorizeConnector(peer, requestConnector); err != nil {
				return fmt.Errorf("manager: meta request denied: %w", err)
			}
		}
	case "PERMISSIONS":
		// No connector argument required.
	default:
		return fmt.Errorf("manager: invalid metadata request_type %q", p.RequestType)
	}

	if m.OnMetaReq != nil {
		return m.OnMetaReq(ctx, peer, p)
	}
	return nil
}

func (m *Manager) handleDataReq(ctx context.Context, peer PeerContext, p protocol.DataReqPayload) error {
	if m.ResolveFileConnector == nil {
		return fmt.Errorf("manager: ResolveFileConnector not configured")
	}
	if strings.TrimSpace(p.UUID) == "" {
		return fmt.Errorf("manager: data request missing file uuid")
	}

	utils.Debugf("[%s] data transfer chunk start transfer_id=%q request_id=%q file_uuid=%q offset=%d length=%d peer_dn=%q",
		m.Name(), p.TransferId, p.RequestID, p.UUID, p.Offset, p.Length, peer.DN)

	connectorName, err := m.ResolveFileConnector(ctx, p.UUID)
	if err != nil {
		return fmt.Errorf("manager: resolve file connector for %q: %w", p.UUID, err)
	}
	if err := m.AuthorizeConnector(peer, connectorName); err != nil {
		return fmt.Errorf("manager: data request denied: %w", err)
	}

	if m.OnDataReq != nil {
		if err := m.OnDataReq(ctx, peer, p); err != nil {
			return err
		}
	}

	utils.Debugf("[%s] data transfer chunk finish transfer_id=%q request_id=%q file_uuid=%q offset=%d length=%d connector=%q",
		m.Name(), p.TransferId, p.RequestID, p.UUID, p.Offset, p.Length, connectorName)
	return nil
}
