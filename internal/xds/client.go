package xds

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type serverClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func newServerClient(cfg ServerConfig, httpClient *http.Client) *serverClient {
	return &serverClient{
		baseURL: strings.TrimRight(cfg.Endpoint, "/"),
		token:   cfg.Token,
		client:  httpClient,
	}
}

func (c *serverClient) FetchProxySnapshot(ctx context.Context) (*apitypes.ProxySnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/proxy/snapshot", nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
		Error   string          `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !envelope.Success {
		if envelope.Error == "" {
			envelope.Error = resp.Status
		}
		return nil, fmt.Errorf("server proxy snapshot: %s", envelope.Error)
	}

	var snapshot apitypes.ProxySnapshot
	if err := json.Unmarshal(envelope.Data, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version == "" {
		return nil, fmt.Errorf("server proxy snapshot has empty version")
	}
	return &snapshot, nil
}
