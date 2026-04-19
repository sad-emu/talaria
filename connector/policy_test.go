package connector

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestPolicySet_Evaluate(t *testing.T) {
	ps, err := NewPolicySet([]ConnectorPolicy{
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

	dec := ps.Evaluate("fileTypeA", "cn=client-a,o=acme", net.ParseIP("10.1.2.3"))
	if !dec.Allowed {
		t.Fatalf("expected allow, got deny: %s", dec.Reason)
	}

	dec = ps.Evaluate("fileTypeA", "cn=client-a,o=acme", net.ParseIP("192.168.1.10"))
	if dec.Allowed {
		t.Fatalf("expected deny for CIDR mismatch")
	}

	dec = ps.Evaluate("fileTypeB", "cn=client-a,o=acme", net.ParseIP("10.1.2.3"))
	if dec.Allowed {
		t.Fatalf("expected deny for DN mismatch")
	}
}

func TestLoadPoliciesFromDir(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "fileTypeA.yaml"), []byte(`
name: fileTypeA
enabled: true
allowed_dns:
  - "CN=client-a,O=acme"
allowed_cidrs:
  - "10.0.0.0/8"
`), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	policies, err := LoadPoliciesFromDir(dir)
	if err != nil {
		t.Fatalf("LoadPoliciesFromDir() error = %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("len(policies) = %d, want 1", len(policies))
	}

	ps, err := NewPolicySet(policies)
	if err != nil {
		t.Fatalf("NewPolicySet() error = %v", err)
	}
	allowed := ps.AllowedConnectors("cn=client-a,o=acme", net.ParseIP("10.9.9.9"))
	if len(allowed) != 1 || allowed[0] != "fileTypeA" {
		t.Fatalf("AllowedConnectors() = %#v, want [fileTypeA]", allowed)
	}
}
