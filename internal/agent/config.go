package agent

import (
	"os"

	"gopkg.in/yaml.v3"
)

type LogConfig struct {
	Dir           string `yaml:"dir"`
	Stdout        bool   `yaml:"stdout"`
	RetentionDays int    `yaml:"retention_days"`
	Level         string `yaml:"level"` // debug/info/warn/error（默认 info）
}

type S3Config struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Region    string `yaml:"region"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type BackupConfig struct {
	S3 S3Config `yaml:"s3"`
}

type Config struct {
	Port       int          `yaml:"port"`
	Token      string       `yaml:"token"`
	DataDir    string       `yaml:"data_dir"`
	AnnounceIP string       `yaml:"announce_ip"`
	Log        LogConfig    `yaml:"log"`
	Backup     BackupConfig `yaml:"backup"`
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
