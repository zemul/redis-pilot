package agent

import (
	"os"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Dir    string `yaml:"dir"`
	Stdout bool   `yaml:"stdout"`
}

type Config struct {
	Port       int       `yaml:"port"`
	Token      string    `yaml:"token"`
	DataDir    string    `yaml:"data_dir"`
	AnnounceIP string    `yaml:"announce_ip"`
	Log        LogConfig `yaml:"log"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:    8400,
		DataDir: "/data/redis",
		Log:     LogConfig{Stdout: true},
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	return cfg, yaml.Unmarshal(data, cfg)
}
