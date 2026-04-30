package config

import "testing"

func TestLoadConfig_WithHodosLocalToS3(t *testing.T) {
	yaml := minimalValidYAML + `
Hodos:
  - Name: "local-to-s3"
    Pickup:
      Type: "local"
      Local:
        Path: "/tmp/in"
        Recurse: true
        KeepFiles: true
    Dropoff:
      Type: "s3"
      S3:
        Bucket: "bucket"
        KeyPrefix: "uploads"
        Region: "us-east-1"
        AccessKeyID: "x"
        SecretAccessKey: "y"
`
	path := writeTempConfig(t, yaml)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Hodos) != 1 {
		t.Fatalf("len(Hodos) = %d, want 1", len(cfg.Hodos))
	}
	h := cfg.Hodos[0]
	if h.Name != "local-to-s3" {
		t.Fatalf("Hodos[0].Name = %q", h.Name)
	}
	if !h.EnabledValue() {
		t.Fatalf("EnabledValue() should default to true")
	}
	if cfg.Persistence.Backend != "sqlite" {
		t.Fatalf("Persistence.Backend = %q, want sqlite", cfg.Persistence.Backend)
	}
	if cfg.Persistence.SQLitePath != "talaria.db" {
		t.Fatalf("Persistence.SQLitePath = %q, want talaria.db", cfg.Persistence.SQLitePath)
	}
}

func TestValidate_HodosLocalRejectsNegativePickupDelayMs(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "n", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		Hodos: []HodosConfig{{
			Name: "bad-delay",
			Pickup: HodosEndpointConfig{
				Type: "local",
				Local: &HodosLocalConfig{
					Path:          "/tmp/in",
					PickupDelayMs: -1,
				},
			},
			Dropoff: HodosEndpointConfig{Type: "s3", S3: &HodosS3Config{
				Bucket: "b", ObjectKey: "k", Region: "r", AccessKeyID: "a", SecretAccessKey: "s",
			}},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatalf("expected error for negative PickupDelayMs")
	}
}

func TestValidate_HodosUnsupportedTypes(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "n", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		Hodos: []HodosConfig{{
			Name:   "bad",
			Pickup: HodosEndpointConfig{Type: "ftp"},
			Dropoff: HodosEndpointConfig{Type: "s3", S3: &HodosS3Config{
				Bucket: "b", ObjectKey: "k", Region: "r", AccessKeyID: "a", SecretAccessKey: "s",
			}},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatalf("expected error for unsupported pickup type")
	}
}

func TestValidate_HodosS3RequiresKeyOrPrefix(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Name: "n", ListenPort: 7000},
		TLS:  TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"},
		Hodos: []HodosConfig{{
			Name:   "bad-s3",
			Pickup: HodosEndpointConfig{Type: "local", Local: &HodosLocalConfig{Path: "/tmp"}},
			Dropoff: HodosEndpointConfig{Type: "s3", S3: &HodosS3Config{
				Bucket: "b", Region: "r", AccessKeyID: "a", SecretAccessKey: "s",
			}},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatalf("expected error for missing ObjectKey/KeyPrefix")
	}
}
