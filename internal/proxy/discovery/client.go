// Package discovery 从 Server 拉取 ProxySnapshot 并维护后端拓扑。
//
// 复用原 xds/client.go 的长轮询客户端（FetchProxySnapshot），已整体搬到此处。
// 原 internal/xds/ 已在迁移中删除。
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// serverClient 是到 Server 的 HTTP 客户端。
type serverClient struct {
	nodeID  string
	baseURL string
	token   string
	client  *http.Client
}

// ReportHeartbeat updates the Server's in-memory runtime view of this proxy.
func (c *serverClient) ReportHeartbeat(ctx context.Context, heartbeat apitypes.ProxyHeartbeat) error {
	body, err := json.Marshal(heartbeat)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/proxy/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server proxy heartbeat: %s", resp.Status)
	}
	return nil
}

func newServerClient(cfg ServerConfig, httpClient *http.Client) *serverClient {
	return &serverClient{
		baseURL: strings.TrimRight(cfg.Endpoint, "/"),
		nodeID:  cfg.NodeID,
		token:   cfg.Token,
		client:  httpClient,
	}
}

// FetchProxySnapshot 拉取代理快照。
//
// knownVersion 非空且 wait > 0 时使用长轮询：Server 在版本未变化时挂起请求，直到版本
// 变化或 wait 超时。超时返回 unchanged=true，此时 snapshot 为 nil。
//
// 兼容性：Server 若不认识这两个参数会立即返回完整快照，此时 unchanged=false，
// 调用方仍能正确工作（只是退化成短轮询），不会因此死循环--调用方按实际耗时补足间隔。
func (c *serverClient) FetchProxySnapshot(ctx context.Context, knownVersion string, wait time.Duration) (snapshot *apitypes.ProxySnapshot, unchanged bool, err error) {
	url := c.baseURL + "/api/v1/proxy/snapshot"
	params := neturl.Values{}
	if c.nodeID != "" {
		params.Set("node_id", c.nodeID)
	}
	if knownVersion != "" && wait > 0 {
		params.Set("known_version", knownVersion)
		params.Set("wait", wait.String())
	}
	if len(params) > 0 {
		url += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	// 304 的响应体是空的，不能拿去解 JSON
	if resp.StatusCode == http.StatusNotModified {
		return nil, true, nil
	}

	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
		Error   string          `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !envelope.Success {
		if envelope.Error == "" {
			envelope.Error = resp.Status
		}
		return nil, false, fmt.Errorf("server proxy snapshot: %s", envelope.Error)
	}

	var snap apitypes.ProxySnapshot
	if err := json.Unmarshal(envelope.Data, &snap); err != nil {
		return nil, false, err
	}
	if snap.Version == "" {
		return nil, false, fmt.Errorf("server proxy snapshot has empty version")
	}
	return &snap, false, nil
}
