package syncer

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"golang.org/x/sys/unix"
)

// Disabled by default. Reporting never authorizes task execution or transfers
// ownership; tasks still come solely from the operator-controlled local YAML.
type ControlPlaneConfig struct {
	ServerEndpoint    string        `yaml:"server_endpoint"`
	TokenFile         string        `yaml:"token_file"`
	SyncerID          string        `yaml:"syncer_id"`
	NodeID            string        `yaml:"node_id"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	RequestTimeout    time.Duration `yaml:"request_timeout"`
}

type ReportingStatus struct {
	Enabled    bool      `json:"enabled"`
	LastResult string    `json:"last_result"`
	LastAckAt  time.Time `json:"last_ack_at"`
}

func (c Config) validateControlPlane() error {
	p := c.ControlPlane
	if p.ServerEndpoint == "" {
		if p.TokenFile != "" || p.SyncerID != "" || p.NodeID != "" {
			return errors.New("syncer control_plane requires server_endpoint")
		}
		return nil
	}
	if len(c.Tasks) < 1 || !apitypes.ValidSyncIdentifier(p.SyncerID) || !apitypes.ValidSyncIdentifier(p.NodeID) {
		return errors.New("syncer reporting requires named tasks and stable syncer_id/node_id")
	}
	u, err := url.Parse(p.ServerEndpoint)
	if err != nil || u.Host == "" || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(p.ServerEndpoint, "#") || u.Opaque != "" {
		return errors.New("syncer server_endpoint requires an origin URL")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || ip == nil || !ip.IsLoopback() {
			return errors.New("syncer reporting requires HTTPS except literal loopback HTTP")
		}
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("syncer server_endpoint requires a valid port")
		}
	}
	if !filepath.IsAbs(p.TokenFile) || p.HeartbeatInterval < time.Second || p.HeartbeatInterval > 15*time.Second || p.RequestTimeout <= 0 || p.RequestTimeout > 3*time.Second {
		return errors.New("syncer reporting requires absolute token_file, interval 1s..15s and timeout (0,3s]")
	}
	if len(c.BuildVersion) < 1 || len(c.BuildVersion) > 128 {
		return errors.New("invalid syncer build version")
	}
	for _, ch := range c.BuildVersion {
		if ch < 33 || ch > 126 {
			return errors.New("invalid syncer build version")
		}
	}
	return nil
}

func readReportingToken(path string) (string, error) {
	invalid := errors.New("invalid syncer control-plane token file")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", invalid
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, err := f.Stat()
	var raw unix.Stat_t
	statErr := unix.Fstat(fd, &raw)
	if err != nil || statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || raw.Uid != uint32(os.Geteuid()) || raw.Nlink != 1 || st.Size() > 4096 {
		return "", invalid
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	token := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if err != nil || len(b) > 4096 || len(token) < 16 {
		return "", invalid
	}
	for _, ch := range token {
		if ch < 33 || ch > 126 {
			return "", invalid
		}
	}
	return token, nil
}

// Digest only non-secret task identity. Paths, resource budgets and passwords
// are deliberately excluded. Changing endpoint/task identity is not migration.
func taskIdentityDigest(task TaskConfig) string {
	source, _ := loopbackEndpoint(task.SourceAddress, false)
	cold, _ := loopbackEndpoint(task.ColdAddress, false)
	raw, _ := json.Marshal([]string{"sync-task-identity-v1", task.TaskID, source.String(), cold.String()})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type syncerReporter struct {
	client          *http.Client
	endpoint, token string
	interval        time.Duration
	report          apitypes.SyncerHeartbeat // owned exclusively by run goroutine
}

func newSyncerReporter(c Config) (*syncerReporter, error) {
	p := c.ControlPlane
	if p.ServerEndpoint == "" {
		return nil, nil
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	token, err := readReportingToken(p.TokenFile)
	if err != nil {
		return nil, err
	}
	report := apitypes.SyncerHeartbeat{SyncerID: p.SyncerID, NodeID: p.NodeID, BootID: rand.Text(), BuildVersion: c.BuildVersion}
	for _, task := range c.Tasks {
		report.Tasks = append(report.Tasks, apitypes.SyncTaskReport{TaskID: task.TaskID, IdentityDigest: taskIdentityDigest(task)})
	}
	sort.Slice(report.Tasks, func(i, j int) bool { return report.Tasks[i].TaskID < report.Tasks[j].TaskID })
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: p.RequestTimeout}).DialContext, TLSHandshakeTimeout: p.RequestTimeout, MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 8192}
	return &syncerReporter{
		client:   &http.Client{Transport: transport, Timeout: p.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		endpoint: strings.TrimRight(p.ServerEndpoint, "/") + "/api/v1/syncer/heartbeat/" + p.SyncerID,
		token:    token, interval: p.HeartbeatInterval, report: report,
	}, nil
}

func (s *Service) recordReport(result string) {
	s.mu.Lock()
	s.reporting.LastResult = result
	if result == "acknowledged" {
		s.reporting.LastAckAt = time.Now().UTC()
		s.lastReportAck.Set(float64(s.reporting.LastAckAt.Unix()))
	}
	s.mu.Unlock()
	s.reportCount.WithLabelValues(result).Inc()
}

func (h *syncerReporter) send(ctx context.Context, s *Service) {
	h.report.Sequence++
	status := s.Status()
	h.report.State, h.report.LastError, h.report.Running = status.State, status.LastError, status.Running
	for i := range h.report.Tasks {
		for _, task := range status.Tasks {
			if task.TaskID == h.report.Tasks[i].TaskID {
				h.report.Tasks[i].Progress = task.Progress
				h.report.Tasks[i].State, h.report.Tasks[i].LastError, h.report.Tasks[i].Running = task.State, task.LastError, task.Running
			}
		}
	}
	result := "error"
	defer func() { s.recordReport(result) }()
	if h.report.Validate() != nil {
		return
	}
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
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		result = "unauthorized"
		return
	case http.StatusConflict:
		result = "conflict"
		return
	case http.StatusOK:
	default:
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
}

// Reporting failure never controls data-plane task execution. No durable report
// queue or server-directed restart. Final reporting follows joined task cleanup.
func (h *syncerReporter) run(ctx context.Context, s *Service) {
	defer h.client.CloseIdleConnections()
	for {
		if ctx.Err() == nil {
			h.send(ctx, s)
		}
		timer := time.NewTimer(h.interval + time.Duration(mathrand.Int64N(int64(h.interval/5)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			final, cancel := context.WithTimeout(context.Background(), min(h.client.Timeout, s.cfg.ShutdownTimeout))
			h.send(final, s)
			cancel()
			return
		case <-timer.C:
		}
	}
}

func newReportingMetrics() (*prometheus.CounterVec, prometheus.Gauge) {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "redis_pilot_syncer_heartbeat_total", Help: "Control-plane observation attempts by fixed result; not replication health."}, []string{"result"}),
		prometheus.NewGauge(prometheus.GaugeOpts{Name: "redis_pilot_syncer_heartbeat_last_ack_timestamp_seconds", Help: "Last acknowledged observation; zero means never, not data freshness."})
}
