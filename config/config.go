package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level application configuration.
type Config struct {
	Node      NodeConfig    `yaml:"Node"`
	TLS       TLSConfig     `yaml:"TLS"`
	Peers     []PeerConfig  `yaml:"Peers"`
	Hodos     []HodosConfig `yaml:"Hodos"`
	GlobalLog LogConfig     `yaml:"GlobalLog"`
}

// NodeConfig holds the identity and listen address of this instance.
type NodeConfig struct {
	Name          string `yaml:"Name"`
	ListenAddress string `yaml:"ListenAddress"`
	ListenPort    int    `yaml:"ListenPort"`
}

// TLSConfig holds paths to TLS material and the DN allowlist.
type TLSConfig struct {
	CertFile   string   `yaml:"CertFile"`
	KeyFile    string   `yaml:"KeyFile"`
	CAFile     string   `yaml:"CAFile"`
	AllowedDNs []string `yaml:"AllowedDNs"`
}

// PeerConfig describes a remote talaria instance to connect to.
type PeerConfig struct {
	Name              string        `yaml:"Name"`
	Address           string        `yaml:"Address"`
	Port              int           `yaml:"Port"`
	HeartbeatInterval time.Duration `yaml:"HeartbeatInterval"`
}

// LogConfig mirrors lumberjack.Logger fields for log rotation.
type LogConfig struct {
	Filename   string `yaml:"Filename"`
	MaxSize    int    `yaml:"MaxSize"`
	MaxBackups int    `yaml:"MaxBackups"`
	MaxAge     int    `yaml:"MaxAge"`
	Compress   bool   `yaml:"Compress"`
}

// LoadConfig reads and parses a YAML config file at path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("config: invalid: %w", err)
	}
	return &cfg, nil
}

func validate(cfg *Config) error {
	if cfg.Node.Name == "" {
		return fmt.Errorf("Node.Name is required")
	}
	if cfg.Node.ListenPort <= 0 || cfg.Node.ListenPort > 65535 {
		return fmt.Errorf("Node.ListenPort must be 1-65535")
	}
	if cfg.TLS.CertFile == "" {
		return fmt.Errorf("TLS.CertFile is required")
	}
	if cfg.TLS.KeyFile == "" {
		return fmt.Errorf("TLS.KeyFile is required")
	}
	if cfg.TLS.CAFile == "" {
		return fmt.Errorf("TLS.CAFile is required")
	}
	for i, p := range cfg.Peers {
		if p.Address == "" {
			return fmt.Errorf("Peers[%d].Address is required", i)
		}
		if p.Port <= 0 || p.Port > 65535 {
			return fmt.Errorf("Peers[%d].Port must be 1-65535", i)
		}
		if p.HeartbeatInterval <= 0 {
			cfg.Peers[i].HeartbeatInterval = 30 * time.Second
		}
	}
	for i, h := range cfg.Hodos {
		if strings.TrimSpace(h.Name) == "" {
			return fmt.Errorf("Hodos[%d].Name is required", i)
		}

		pickupType := normalizeEndpointType(h.Pickup.Type)
		dropoffType := normalizeEndpointType(h.Dropoff.Type)

		switch pickupType {
		case "local":
			if h.Pickup.Local == nil || strings.TrimSpace(h.Pickup.Local.Path) == "" {
				return fmt.Errorf("Hodos[%d].Pickup.Local.Path is required for local pickup", i)
			}
		case "talaria":
			// Placeholder for upcoming talaria pickup mode.
		default:
			return fmt.Errorf("Hodos[%d].Pickup.Type %q is not supported", i, h.Pickup.Type)
		}

		switch dropoffType {
		case "s3":
			if h.Dropoff.S3 == nil {
				return fmt.Errorf("Hodos[%d].Dropoff.S3 is required for s3 dropoff", i)
			}
			s3cfg := h.Dropoff.S3
			if strings.TrimSpace(s3cfg.Bucket) == "" {
				return fmt.Errorf("Hodos[%d].Dropoff.S3.Bucket is required", i)
			}
			if strings.TrimSpace(s3cfg.ObjectKey) == "" && strings.TrimSpace(s3cfg.KeyPrefix) == "" {
				return fmt.Errorf("Hodos[%d].Dropoff.S3.ObjectKey or KeyPrefix is required", i)
			}
			if strings.TrimSpace(s3cfg.Region) == "" {
				return fmt.Errorf("Hodos[%d].Dropoff.S3.Region is required", i)
			}
			if strings.TrimSpace(s3cfg.AccessKeyID) == "" {
				return fmt.Errorf("Hodos[%d].Dropoff.S3.AccessKeyID is required", i)
			}
			if strings.TrimSpace(s3cfg.SecretAccessKey) == "" {
				return fmt.Errorf("Hodos[%d].Dropoff.S3.SecretAccessKey is required", i)
			}
		case "talaria":
			// Placeholder for upcoming talaria dropoff mode.
		default:
			return fmt.Errorf("Hodos[%d].Dropoff.Type %q is not supported", i, h.Dropoff.Type)
		}
	}
	return nil
}
