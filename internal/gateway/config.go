package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is deliberately limited to the G1 loopback-only skeleton.
// Forwarding, credentials and dynamic topology are not supported yet.
type Config struct {
	GatewayID       string        `yaml:"gateway_id"`
	ServiceID       string        `yaml:"service_id"`
	ShardID         string        `yaml:"shard_id"`
	Mode            string        `yaml:"mode"`
	Listen          string        `yaml:"listen"`
	AdminListen     string        `yaml:"admin_listen"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	Limits          Limits        `yaml:"limits"`
}

func DefaultConfig() Config {
	return Config{
		GatewayID: "gateway-local", ServiceID: "test", ShardID: "test-0",
		Mode: "skeleton", Listen: "127.0.0.1:17379", AdminListen: "127.0.0.1:19200",
		ShutdownTimeout: 10 * time.Second,
		Limits:          Limits{MaxBytes: 16 << 20, MaxElements: 4096, MaxDepth: 32},
	}
}

func LoadConfig(name string) (Config, error) {
	f, err := os.Open(name)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	return DecodeConfig(f)
}

func DecodeConfig(r io.Reader) (Config, error) {
	c := DefaultConfig()
	data, err := io.ReadAll(io.LimitReader(r, (64<<10)+1))
	if err != nil {
		return Config{}, err
	}
	if len(data) > 64<<10 {
		return Config{}, errors.New("gateway config exceeds 64KiB")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("gateway config: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("gateway config: expected one YAML document")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.GatewayID == "" || c.ServiceID == "" || c.ShardID == "" {
		return errors.New("gateway/service/shard IDs must not be empty")
	}
	if c.Mode != "skeleton" {
		return errors.New("only skeleton mode is implemented; forwarding is unavailable")
	}
	for _, addr := range []string{c.Listen, c.AdminListen} {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("invalid listen address: %w", err)
		}
		ip := net.ParseIP(host)
		n, err := strconv.Atoi(port)
		if ip == nil || !ip.IsLoopback() || err != nil || n < 0 || n > 65535 {
			return errors.New("G1 listeners require a literal loopback IP and valid port")
		}
	}
	if c.Listen == c.AdminListen {
		_, port, _ := net.SplitHostPort(c.Listen)
		if port != "0" {
			return errors.New("data and admin listeners must differ")
		}
	}
	if c.ShutdownTimeout <= 0 || c.ShutdownTimeout > time.Minute {
		return errors.New("shutdown_timeout must be positive and at most 1m")
	}
	return c.Limits.Validate()
}
