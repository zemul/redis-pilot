package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type Client struct {
	base     string
	token    string
	operator string
}

func NewClient(cfg *Config) *Client {
	return &Client{base: "http://" + cfg.Server, token: cfg.Token, operator: cfg.Operator}
}

func (c *Client) Get(path string) (*apitypes.APIResponse, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

func (c *Client) Post(path string, body interface{}) (*apitypes.APIResponse, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *Client) do(req *http.Request) (*apitypes.APIResponse, error) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.operator != "" {
		req.Header.Set("X-Operator", c.operator)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var r apitypes.APIResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return &r, nil
}
