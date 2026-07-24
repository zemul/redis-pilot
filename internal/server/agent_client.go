package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// agentHTTPClient 是 Server 调用各节点 Agent 的共享 HTTP 客户端。
// 必须设置超时：Agent 卡死或网络异常时，避免 handler 和状态锁被永久阻塞。
var agentHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

type agentClient struct {
	endpoint string
	token    string
}

func newAgentClient(server *apitypes.NodeServer) *agentClient {
	return &agentClient{
		endpoint: fmt.Sprintf("http://%s:%d", server.Endpoint, server.AgentPort),
		token:    server.AgentToken,
	}
}

func (c *agentClient) post(path string, body interface{}) ([]byte, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.endpoint+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := agentHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: %s", path, out)
	}
	return out, nil
}

func (c *agentClient) get(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, c.endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := agentHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: %s", path, out)
	}
	return out, nil
}
