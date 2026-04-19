package peer

import (
	"net"
	"reflect"
	"strings"
	"testing"

	"talaria/connector"
	"talaria/protocol"
)

func TestIntegration_TwoInstances_HeartbeatAndPermissionsFilter(t *testing.T) {
	// Simulated instance links.
	aConn, bConn := net.Pipe()
	defer aConn.Close()
	defer bConn.Close()

	// Instance A policy: connectors A/B/C exist, but instance B can only access B and C.
	ps, err := connector.NewPolicySet([]connector.ConnectorPolicy{
		{Name: "A", Enabled: true, AllowedDNs: []string{"CN=some-other-client,O=customer-x"}},
		{Name: "B", Enabled: true, AllowedDNs: []string{"CN=instance-b,O=customer-x"}},
		{Name: "C", Enabled: true, AllowedDNs: []string{"CN=instance-b,O=customer-x"}},
	})
	if err != nil {
		t.Fatalf("NewPolicySet() error = %v", err)
	}

	instanceA := NewManager("instance-a")
	instanceA.SetPolicies(ps)

	serverErr := make(chan error, 1)
	go func() {
		// 1) Receive heartbeat from instance B, reply heartbeat response.
		msgType, body, err := protocol.ReadMessage(aConn)
		if err != nil {
			serverErr <- err
			return
		}
		if msgType != protocol.MsgHeartbeatReq {
			serverErr <- errUnexpectedType(msgType, protocol.MsgHeartbeatReq)
			return
		}
		if _, err := protocol.HandleMessage(msgType, body, protocol.Handlers{}); err != nil {
			serverErr <- err
			return
		}
		if err := protocol.WriteMessage(aConn, protocol.MsgHeartbeatResp, protocol.NewHeartbeatReq("instance-a")); err != nil {
			serverErr <- err
			return
		}

		// 2) Receive permissions request and return filtered connectors.
		msgType, body, err = protocol.ReadMessage(aConn)
		if err != nil {
			serverErr <- err
			return
		}
		if msgType != protocol.MsgMetaReq {
			serverErr <- errUnexpectedType(msgType, protocol.MsgMetaReq)
			return
		}

		decoded, err := protocol.HandleMessage(msgType, body, protocol.Handlers{})
		if err != nil {
			serverErr <- err
			return
		}
		req := decoded.(protocol.MetaReqPayload)
		if !strings.EqualFold(req.RequestType, "PERMISSIONS") {
			serverErr <- errInvalidRequestType(req.RequestType)
			return
		}

		allowed, err := instanceA.AllowedConnectors(PeerContext{
			DN: "CN=instance-b,O=customer-x",
			IP: net.ParseIP("10.10.10.20"),
		})
		if err != nil {
			serverErr <- err
			return
		}

		resp := protocol.MetaPermissionsRespPayload{
			ID:                  "perm-resp-1",
			RequestID:           req.ID,
			Timestamp:           req.Timestamp + 1,
			NodeName:            "instance-a",
			AvailableConnectors: allowed,
		}
		if err := protocol.WriteMessage(aConn, protocol.MsgMetaPermResp, resp); err != nil {
			serverErr <- err
			return
		}

		serverErr <- nil
	}()

	// Instance B -> heartbeat req
	hbReq := protocol.NewHeartbeatReq("instance-b")
	if err := protocol.WriteMessage(bConn, protocol.MsgHeartbeatReq, hbReq); err != nil {
		t.Fatalf("WriteMessage heartbeat req: %v", err)
	}

	// Instance B <- heartbeat resp
	msgType, body, err := protocol.ReadMessage(bConn)
	if err != nil {
		t.Fatalf("ReadMessage heartbeat resp: %v", err)
	}
	if msgType != protocol.MsgHeartbeatResp {
		t.Fatalf("msgType = %v, want %v", msgType, protocol.MsgHeartbeatResp)
	}
	if _, err := protocol.HandleMessage(msgType, body, protocol.Handlers{}); err != nil {
		t.Fatalf("HandleMessage heartbeat resp: %v", err)
	}

	// Instance B -> MetaReq PERMISSIONS
	metaReq := protocol.MetaReqPayload{
		ID:          "meta-req-1",
		Timestamp:   hbReq.Timestamp + 10,
		NodeName:    "instance-b",
		RequestType: "PERMISSIONS",
	}
	if err := protocol.WriteMessage(bConn, protocol.MsgMetaReq, metaReq); err != nil {
		t.Fatalf("WriteMessage meta req: %v", err)
	}

	// Instance B <- MetaPermResp, must only contain B and C.
	msgType, body, err = protocol.ReadMessage(bConn)
	if err != nil {
		t.Fatalf("ReadMessage meta perm resp: %v", err)
	}
	if msgType != protocol.MsgMetaPermResp {
		t.Fatalf("msgType = %v, want %v", msgType, protocol.MsgMetaPermResp)
	}

	decoded, err := protocol.HandleMessage(msgType, body, protocol.Handlers{})
	if err != nil {
		t.Fatalf("HandleMessage meta perm resp: %v", err)
	}
	resp := decoded.(protocol.MetaPermissionsRespPayload)

	want := []string{"B", "C"}
	if !reflect.DeepEqual(resp.AvailableConnectors, want) {
		t.Fatalf("AvailableConnectors = %#v, want %#v", resp.AvailableConnectors, want)
	}

	if err := <-serverErr; err != nil {
		t.Fatalf("instance A loop error: %v", err)
	}
}

func errUnexpectedType(got, want protocol.MessageType) error {
	return &integrationErr{msg: "unexpected message type", gotType: got, wantType: want}
}

func errInvalidRequestType(got string) error {
	return &integrationErr{msg: "invalid request_type: " + got}
}

type integrationErr struct {
	msg      string
	gotType  protocol.MessageType
	wantType protocol.MessageType
}

func (e *integrationErr) Error() string {
	if e.wantType != 0 {
		return e.msg
	}
	return e.msg
}
