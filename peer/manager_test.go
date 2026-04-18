package peer

import (
	"net"
	"testing"

	"talaria/protocol"
)

func makeTestPeerConn(t *testing.T) (*PeerConn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	pc := newPeerConn(server, "mgr-node", func(protocol.MessageType, []byte) {})
	return pc, client
}

func TestManager_ActiveCount_Empty(t *testing.T) {
	m := NewManager("test-node")
	if got := m.ActiveCount(); got != 0 {
		t.Errorf("ActiveCount = %d, want 0", got)
	}
}

func TestManager_Register_IncreasesCount(t *testing.T) {
	m := NewManager("test-node")

	pc1, _ := makeTestPeerConn(t)
	pc2, _ := makeTestPeerConn(t)

	m.register(pc1)
	if got := m.ActiveCount(); got != 1 {
		t.Errorf("ActiveCount after 1 register = %d, want 1", got)
	}

	m.register(pc2)
	if got := m.ActiveCount(); got != 2 {
		t.Errorf("ActiveCount after 2 registers = %d, want 2", got)
	}
}

func TestManager_Unregister_DecreasesCount(t *testing.T) {
	m := NewManager("test-node")

	pc, _ := makeTestPeerConn(t)
	m.register(pc)
	m.unregister(pc)

	if got := m.ActiveCount(); got != 0 {
		t.Errorf("ActiveCount after unregister = %d, want 0", got)
	}
}

func TestManager_Unregister_NonRegistered(t *testing.T) {
	// Unregistering a connection that was never registered should not panic.
	m := NewManager("test-node")
	pc, _ := makeTestPeerConn(t)
	m.unregister(pc) // should not panic
}

func TestManager_MultipleRegisterUnregister(t *testing.T) {
	m := NewManager("test-node")
	const n = 5
	pcs := make([]*PeerConn, n)
	for i := range pcs {
		pcs[i], _ = makeTestPeerConn(t)
		m.register(pcs[i])
	}
	if got := m.ActiveCount(); got != n {
		t.Fatalf("ActiveCount = %d, want %d", got, n)
	}
	for _, pc := range pcs {
		m.unregister(pc)
	}
	if got := m.ActiveCount(); got != 0 {
		t.Errorf("ActiveCount after all unregistered = %d, want 0", got)
	}
}

func TestManager_RegisterSamePeerTwice(t *testing.T) {
	// A map keyed by pointer; registering the same pointer twice should keep count at 1.
	m := NewManager("test-node")
	pc, _ := makeTestPeerConn(t)
	m.register(pc)
	m.register(pc)
	if got := m.ActiveCount(); got != 1 {
		t.Errorf("ActiveCount after double register = %d, want 1", got)
	}
}
