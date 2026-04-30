package config

import (
	"os"
	"testing"
	"time"
)

// minimalValidYAML is a well-formed config that passes validation.
const minimalValidYAML = `
Node:
  Name: "test-node"
  ListenAddress: "0.0.0.0"
  ListenPort: 7000
TLS:
  CertFile: "cert.pem"
  KeyFile:  "key.pem"
  CAFile:   "ca.pem"
`

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "talaria-config-*.yml")
	if err != nil {
		t.Fatalf("create temp config: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestLoadConfig_ValidMinimal(t *testing.T) {
	path := writeTempConfig(t, minimalValidYAML)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Node.Name != "test-node" {
		t.Errorf("Node.Name = %q, want %q", cfg.Node.Name, "test-node")
	}
	if cfg.Node.ListenPort != 7000 {
		t.Errorf("Node.ListenPort = %d, want 7000", cfg.Node.ListenPort)
	}
	if cfg.TLS.CertFile != "cert.pem" {
		t.Errorf("TLS.CertFile = %q, want cert.pem", cfg.TLS.CertFile)
	}
}

func TestLoadConfig_WithPeers(t *testing.T) {
	yaml := minimalValidYAML + `
Peers:
  - Name: "peer-1"
    Address: "10.0.0.1"
    Port: 7001
    HeartbeatInterval: "10s"
`
	path := writeTempConfig(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Peers) != 1 {
		t.Fatalf("len(Peers) = %d, want 1", len(cfg.Peers))
	}
	p := cfg.Peers[0]
	if p.Name != "peer-1" {
		t.Errorf("Peers[0].Name = %q, want peer-1", p.Name)
	}
	if p.HeartbeatInterval != 10*time.Second {
		t.Errorf("Peers[0].HeartbeatInterval = %v, want 10s", p.HeartbeatInterval)
	}
}

func TestLoadConfig_DefaultHeartbeatInterval(t *testing.T) {
	yaml := minimalValidYAML + `
Peers:
  - Name: "peer-1"
    Address: "10.0.0.1"
    Port: 7001
`
	path := writeTempConfig(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Peers[0].HeartbeatInterval != 30*time.Second {
		t.Errorf("expected default HeartbeatInterval 30s, got %v", cfg.Peers[0].HeartbeatInterval)
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/talaria.yml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := writeTempConfig(t, "this: is: not: valid: yaml: }{")
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestValidate_MissingNodeName(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for missing Node.Name")
	}
}

func TestValidate_InvalidListenPort(t *testing.T) {
	for _, port := range []int{0, -1, 65536, 99999} {
		cfg := &Config{
			Node: NodeConfig{Name: "n", ListenPort: port},
			TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		}
		if err := validate(cfg); err == nil {
			t.Errorf("expected error for port %d", port)
		}
	}
}

func TestValidate_MissingTLSFields(t *testing.T) {
	base := NodeConfig{Name: "n", ListenPort: 7000}
	cases := []struct {
		name string
		tls  TLSConfig
	}{
		{"missing CertFile", TLSConfig{KeyFile: "k", CAFile: "ca"}},
		{"missing KeyFile", TLSConfig{CertFile: "c", CAFile: "ca"}},
		{"missing CAFile", TLSConfig{CertFile: "c", KeyFile: "k"}},
	}
	for _, tc := range cases {
		cfg := &Config{Node: base, TLS: tc.tls}
		if err := validate(cfg); err == nil {
			t.Errorf("expected error for %s", tc.name)
		}
	}
}

func TestValidate_PeerMissingAddress(t *testing.T) {
	cfg := &Config{
		Node:  NodeConfig{Name: "n", ListenPort: 7000},
		TLS:   TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		Peers: []PeerConfig{{Name: "p", Address: "", Port: 7001}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for peer missing Address")
	}
}

func TestValidate_PeerInvalidPort(t *testing.T) {
	cfg := &Config{
		Node:  NodeConfig{Name: "n", ListenPort: 7000},
		TLS:   TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		Peers: []PeerConfig{{Name: "p", Address: "10.0.0.1", Port: 0}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for peer invalid port")
	}
}

func TestLoadConfig_AllowedDNs(t *testing.T) {
	yaml := minimalValidYAML + `
  AllowedDNs:
    - "CN=node-2"
    - "CN=node-3"
`
	path := writeTempConfig(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.TLS.AllowedDNs) != 2 {
		t.Errorf("len(AllowedDNs) = %d, want 2", len(cfg.TLS.AllowedDNs))
	}
}

func TestValidate_DefaultsLogLevelToInfo(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "n", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
	}
	if err := validate(cfg); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
	if cfg.GlobalLog.Level != "INFO" {
		t.Fatalf("GlobalLog.Level = %q, want INFO", cfg.GlobalLog.Level)
	}
}

func TestValidate_RejectsInvalidLogLevel(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "n", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		GlobalLog: LogConfig{
			Level: "TRACE",
		},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for invalid GlobalLog.Level")
	}
}
