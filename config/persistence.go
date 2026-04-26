package config

type PersistenceConfig struct {
	Backend    string `yaml:"Backend"`
	SQLitePath string `yaml:"SQLitePath"`
}
