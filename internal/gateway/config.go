package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config describes the isolated single-backend development gateway.
type Config struct {
	GatewayID            string        `yaml:"gateway_id"`
	ServiceID            string        `yaml:"service_id"`
	ShardID              string        `yaml:"shard_id"`
	Mode                 string        `yaml:"mode"`
	Listen               string        `yaml:"listen"`
	AdminListen          string        `yaml:"admin_listen"`
	ShutdownTimeout      time.Duration `yaml:"shutdown_timeout"`
	Limits               Limits        `yaml:"limits"`
	RedisAddress         string        `yaml:"redis_address"`
	FrontendPasswordFile string        `yaml:"frontend_password_file"`
	BackendPasswordFile  string        `yaml:"backend_password_file"`
	AllowUnauthenticated bool          `yaml:"allow_unauthenticated"`
	ConnectTimeout       time.Duration `yaml:"connect_timeout"`
	RequestTimeout       time.Duration `yaml:"request_timeout"`
	ClientIdleTimeout    time.Duration `yaml:"client_idle_timeout"`
	MaxConnections       int           `yaml:"max_connections"`
	MaxPipeline          int           `yaml:"max_pipeline"`
	MaxBatchBytes        int           `yaml:"max_batch_bytes"`
	BufferBudgetBytes    int64         `yaml:"buffer_budget_bytes"`
}

func DefaultConfig() Config {
	return Config{
		GatewayID: "gateway-local", ServiceID: "test", ShardID: "test-0",
		Mode: "skeleton", Listen: "127.0.0.1:17379", AdminListen: "127.0.0.1:19200",
		ShutdownTimeout: 10 * time.Second,
		Limits:          Limits{MaxBytes: 16 << 20, MaxElements: 4096, MaxDepth: 32},
		ConnectTimeout:  3 * time.Second, RequestTimeout: 5 * time.Second,
		ClientIdleTimeout: time.Minute, MaxConnections: 16,
		MaxPipeline: 8, MaxBatchBytes: 16 << 20, BufferBudgetBytes: 1 << 30,
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
	if c.Mode != "skeleton" && c.Mode != "passthrough" {
		return errors.New("mode must be skeleton or passthrough; hybrid is unavailable")
	}
	if c.Mode == "passthrough" {
		if c.MaxPipeline < 1 || c.MaxPipeline > 64 || c.MaxBatchBytes < c.Limits.MaxBytes || c.MaxBatchBytes > 64<<20 || c.BufferBudgetBytes < c.workspaceBytes()+c.probeWorkspaceBytes() || c.BufferBudgetBytes > 8<<30 {
			return errors.New("invalid pipeline/batch/buffer limits; budget must fit one session workspace")
		}
		host, port, err := net.SplitHostPort(c.RedisAddress)
		n, pe := strconv.Atoi(port)
		if err != nil || strings.TrimSpace(host) == "" || pe != nil || n < 1 || n > 65535 {
			return errors.New("passthrough requires a valid redis_address")
		}
		if c.FrontendPasswordFile == "" && !c.AllowUnauthenticated {
			return errors.New("frontend_password_file or explicit allow_unauthenticated is required")
		}
		if c.ConnectTimeout <= 0 || c.ConnectTimeout > time.Minute || c.RequestTimeout <= 0 || c.RequestTimeout > time.Minute || c.ClientIdleTimeout <= 0 || c.ClientIdleTimeout > time.Hour || c.MaxConnections < 1 || c.MaxConnections > 256 {
			return errors.New("invalid gateway timeouts or max_connections")
		}
	}
	for _, addr := range []string{c.Listen, c.AdminListen} {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("invalid listen address: %w", err)
		}
		ip := net.ParseIP(host)
		n, err := strconv.Atoi(port)
		if ip == nil || !ip.IsLoopback() || err != nil || n < 0 || n > 65535 {
			return errors.New("development listeners require a literal loopback IP and valid port")
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

func (c Config) probeWorkspaceBytes() int64 {
	return 8*int64(c.Limits.MaxBytes) + 64*int64(c.Limits.MaxElements) + 16384
}

// Conservatively reserve a whole workspace before parsing any client input.
// Includes a batch, read-ahead/pending frames, response, argv and I/O buffers.
// This is an admission budget, not an RSS measurement or a heap hard limit.
func (c Config) workspaceBytes() int64 {
	return 8*(int64(c.MaxBatchBytes)+4*int64(c.Limits.MaxBytes)) + 64*int64(c.Limits.MaxElements)*int64(c.MaxPipeline+3) + 65536
}
