package config

import "strings"

// HodosConfig defines one pickup-to-dropoff flow.
type HodosConfig struct {
	Name    string `yaml:"Name"`
	Enabled *bool  `yaml:"Enabled,omitempty"`
	RunOnce *bool  `yaml:"RunOnce,omitempty"`

	Pickup  HodosEndpointConfig `yaml:"Pickup"`
	Dropoff HodosEndpointConfig `yaml:"Dropoff"`
}

// HodosEndpointConfig describes one side of a hodos flow.
type HodosEndpointConfig struct {
	Type string `yaml:"Type"`

	Local   *HodosLocalConfig   `yaml:"Local,omitempty"`
	S3      *HodosS3Config      `yaml:"S3,omitempty"`
	Talaria *HodosTalariaConfig `yaml:"Talaria,omitempty"`
}

// HodosLocalConfig configures local disk pickup/dropoff.
type HodosLocalConfig struct {
	Path      string `yaml:"Path"`
	Recurse   bool   `yaml:"Recurse"`
	KeepFiles bool   `yaml:"KeepFiles"`
}

// HodosS3Config configures S3 dropoff.
type HodosS3Config struct {
	Bucket            string `yaml:"Bucket"`
	ObjectKey         string `yaml:"ObjectKey,omitempty"`
	KeyPrefix         string `yaml:"KeyPrefix,omitempty"`
	Region            string `yaml:"Region"`
	Endpoint          string `yaml:"Endpoint,omitempty"`
	UsePathStyle      bool   `yaml:"UsePathStyle"`
	OverwriteExisting bool   `yaml:"OverwriteExisting"`
	AccessKeyID       string `yaml:"AccessKeyID"`
	SecretAccessKey   string `yaml:"SecretAccessKey"`
	SessionToken      string `yaml:"SessionToken,omitempty"`
}

// HodosTalariaConfig is a placeholder for talaria-native transfers.
type HodosTalariaConfig struct {
	PeerName  string `yaml:"PeerName"`
	Connector string `yaml:"Connector"`
}

func (h HodosConfig) EnabledValue() bool {
	return h.Enabled == nil || *h.Enabled
}

func (h HodosConfig) RunOnceValue() bool {
	return h.RunOnce == nil || *h.RunOnce
}

func normalizeEndpointType(t string) string {
	return strings.ToLower(strings.TrimSpace(t))
}
