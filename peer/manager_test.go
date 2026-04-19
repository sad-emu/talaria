package peer

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"talaria/connector"
	"talaria/protocol"
)

func mustPolicies(t *testing.T) *connector.PolicySet {
	t.Helper()

	ps, err := connector.NewPolicySet([]connector.ConnectorPolicy{
		{
			Name:         "fileTypeA",
			Enabled:      true,
			AllowedDNs:   []string{"CN=client-a,O=acme"},
			AllowedCIDRs: []string{"10.0.0.0/8"},
		},
		{
			Name:       "fileTypeB",
			Enabled:    true,
			AllowedDNs: []string{"CN=client-b,O=acme"},
		},
	})
	if err != nil {
		t.Fatalf("NewPolicySet() error = %v", err)
	}
	return ps
}

func makeTestPeerConn(t *testing.T) (*PeerConn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	pc := newPeerConn(server, "mgr-node", func(protocol.MessageType, []byte) {})
	return pc, client
}

func activeCount(m *Manager) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

func TestManager_ActiveCount_Empty(t *testing.T) {
	m := NewManager("test-mgr1")
	if got := activeCount(m); got != 0 {
		t.Errorf("activeCount = %d, want 0", got)
	}
}

func TestManager_Register_IncreasesCount(t *testing.T) {
	m := NewManager("test-mgr2")

	pc1, _ := makeTestPeerConn(t)
	pc2, _ := makeTestPeerConn(t)

	m.register(pc1)
	if got := activeCount(m); got != 1 {
		t.Errorf("activeCount after 1 register = %d, want 1", got)
	}

	m.register(pc2)
	if got := activeCount(m); got != 2 {
		t.Errorf("activeCount after 2 registers = %d, want 2", got)
	}
}

func TestManager_Unregister_DecreasesCount(t *testing.T) {
	m := NewManager("test-mgr3")

	pc, _ := makeTestPeerConn(t)
	m.register(pc)
	m.unregister(pc)

	if got := activeCount(m); got != 0 {
		t.Errorf("activeCount after unregister = %d, want 0", got)
	}
}

func TestManager_Unregister_NonRegistered(t *testing.T) {
	m := NewManager("test-mgr4")
	pc, _ := makeTestPeerConn(t)
	m.unregister(pc) // should not panic
}

func TestManager_MultipleRegisterUnregister(t *testing.T) {
	m := NewManager("test-mgr5")
	const n = 5
	pcs := make([]*PeerConn, n)
	for i := range pcs {
		pcs[i], _ = makeTestPeerConn(t)
		m.register(pcs[i])
	}
	if got := activeCount(m); got != n {
		t.Fatalf("activeCount = %d, want %d", got, n)
	}
	for _, pc := range pcs {
		m.unregister(pc)
	}
	if got := activeCount(m); got != 0 {
		t.Errorf("activeCount after all unregistered = %d, want 0", got)
	}
}

func TestManager_RegisterSamePeerTwice(t *testing.T) {
	m := NewManager("test-mgr6")
	pc, _ := makeTestPeerConn(t)
	m.register(pc)
	m.register(pc)
	if got := activeCount(m); got != 1 {
		t.Errorf("activeCount after double register = %d, want 1", got)
	}
}

func TestManager_MetaReq_RequestConnectorDenied(t *testing.T) {
	m := &Manager{Policies: mustPolicies(t)}
	peer := PeerContext{DN: "CN=client-b,O=acme", IP: net.ParseIP("10.1.2.3")}

	req := protocol.MetaReqPayload{
		ID:               "m1",
		RequestType:      "FILES",
		RequestConnector: "fileTypeA",
	}
	body, _ := json.Marshal(req)

	_, err := m.ProcessIncoming(context.Background(), peer, protocol.MsgMetaReq, body)
	if err == nil {
		t.Fatal("expected deny error, got nil")
	}
}

func TestManager_DataReq_MissingResolver(t *testing.T) {
	m := &Manager{Policies: mustPolicies(t)}
	peer := PeerContext{DN: "CN=client-a,O=acme", IP: net.ParseIP("10.2.3.4")}

	req := protocol.DataReqPayload{ID: "d3", UUID: "file-1"}
	body, _ := json.Marshal(req)

	_, err := m.ProcessIncoming(context.Background(), peer, protocol.MsgDataReq, body)
	if err == nil {
		t.Fatal("expected resolver error, got nil")
	}
}
