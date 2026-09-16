package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Disabled unless ServerEndpoint is set. Placement is declared observation,
// not a Server assignment, distributed lease, or permission to serve a shard.
type ControlPlaneConfig struct {
	ManageShards      bool          `yaml:"manage_shards"`
	ServerEndpoint    string        `yaml:"server_endpoint"`
	TokenFile         string        `yaml:"token_file"`
	NodeID            string        `yaml:"node_id"`
	ClusterID         string        `yaml:"cluster_id"`
	ShardIDs          []string      `yaml:"shard_ids"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	RequestTimeout    time.Duration `yaml:"request_timeout"`
}

func (c Config) validateControlPlane() error {
	p := c.ControlPlane
	if p.ManageShards && (p.ServerEndpoint == "" || c.Cache == nil || c.Cache.Sharding == nil || p.ClusterID != c.ServiceID) {
		return errors.New("manage_shards requires cache.sharding and control_plane.cluster_id equal to service_id")
	}
	if p.ManageShards {
		if c.BackendPasswordFile != "" {
			return errors.New("managed platform backends do not use database passwords")
		}
		for _, n := range c.Cache.Sharding.Nodes {
			if n.PasswordFile != "" {
				return errors.New("managed platform backends do not use database passwords")
			}
		}
	}
	if p.ServerEndpoint == "" {
		if p.TokenFile != "" || p.NodeID != "" || p.ClusterID != "" || len(p.ShardIDs) > 0 {
			return errors.New("control_plane requires server_endpoint")
		}
		return nil
	}
	u, err := url.Parse(p.ServerEndpoint)
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("control_plane requires an origin URL without userinfo, path, query or fragment")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || ip == nil || !ip.IsLoopback() {
			return errors.New("control_plane requires HTTPS, except for literal loopback HTTP")
		}
	}
	if !filepath.IsAbs(p.TokenFile) || p.HeartbeatInterval < time.Second || p.HeartbeatInterval > 15*time.Second || p.RequestTimeout <= 0 || p.RequestTimeout > 3*time.Second {
		return errors.New("control_plane requires absolute token_file, heartbeat_interval 1s..15s, request_timeout (0,3s]")
	}
	v := apitypes.GatewayHeartbeat{GatewayID: c.GatewayID, BootID: "validation", Sequence: 1, NodeID: p.NodeID, ClusterID: p.ClusterID, ShardIDs: p.ShardIDs, BuildVersion: c.BuildVersion, Mode: c.Mode, LocalConfigDigest: strings.Repeat("0", 64)}
	return v.Validate()
}

type heartbeatReporter struct {
	client   *http.Client
	endpoint string
	token    string
	interval time.Duration
	report   apitypes.GatewayHeartbeat // owned by one reporter goroutine
}

func newHeartbeatReporter(c Config) (*heartbeatReporter, error) {
	p := c.ControlPlane
	if p.ServerEndpoint == "" {
		return nil, nil
	}
	f, err := os.Open(p.TokenFile)
	if err != nil {
		return nil, errors.New("cannot open control-plane token file")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("control-plane token file must be regular and owner-only")
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	token := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if err != nil || len(b) > 4096 || len(token) < 16 {
		return nil, errors.New("invalid control-plane token file")
	}
	for _, ch := range token {
		if ch < 33 || ch > 126 {
			return nil, errors.New("invalid control-plane token file")
		}
	}
	// Digest of decoded non-secret config only; never hash credential contents.
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("cannot fingerprint gateway configuration")
	}
	sum := sha256.Sum256(raw)
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: p.RequestTimeout}).DialContext, TLSHandshakeTimeout: p.RequestTimeout, MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 8192}
	return &heartbeatReporter{
		client:   &http.Client{Transport: transport, Timeout: p.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		endpoint: strings.TrimRight(p.ServerEndpoint, "/") + "/api/v1/gateway/heartbeat/" + c.GatewayID,
		token:    token, interval: p.HeartbeatInterval,
		report: apitypes.GatewayHeartbeat{GatewayID: c.GatewayID, BootID: rand.Text(), NodeID: p.NodeID, ClusterID: p.ClusterID, ShardIDs: append([]string(nil), p.ShardIDs...), BuildVersion: c.BuildVersion, Mode: c.Mode, LocalConfigDigest: hex.EncodeToString(sum[:])},
	}, nil
}

// One bounded synchronous report at a time, no durable queue, no data-path locks.
// Lost acknowledgements are followed by a newer observation, never a data retry.
func (h *heartbeatReporter) send(ctx context.Context, s *Service) {
	h.report.Sequence++
	h.report.Serving = s.serving.Load()
	h.report.BackendReady = s.backendReady.Load()
	h.report.Draining = s.draining.Load()
	if h.report.Draining {
		h.report.Serving = false
	}
	result := "error"
	defer func() { s.heartbeats.WithLabelValues(result).Inc() }()
	raw, err := json.Marshal(h.report)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(raw))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(b) > 4096 {
		return
	}
	var ack struct {
		Success bool `json:"success"`
		Data    struct {
			Sequence int64 `json:"accepted_sequence"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &ack) != nil || !ack.Success || ack.Data.Sequence != h.report.Sequence {
		return
	}
	result = "acknowledged"
	s.lastHeartbeatAck.Set(float64(time.Now().Unix()))
}

func (h *heartbeatReporter) run(ctx context.Context, s *Service) {
	defer h.client.CloseIdleConnections()
	for {
		h.send(ctx, s)
		// Small bounded jitter prevents synchronized reconnect bursts.
		timer := time.NewTimer(h.interval + time.Duration(mathrand.Int64N(int64(h.interval/5)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			// Best-effort final draining observation; stale timeout is the fallback.
			if s.draining.Load() {
				final, cancel := context.WithTimeout(context.Background(), min(h.client.Timeout, s.cfg.ShutdownTimeout))
				h.send(final, s)
				cancel()
			}
			return
		case <-timer.C:
		}
	}
}
