package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

const heartbeatToken = "gateway-test-token-long-enough"

func serverHeartbeatFixture() apitypes.GatewayHeartbeat {
	return apitypes.GatewayHeartbeat{GatewayID: "gw", BootID: "boot", Sequence: 1, NodeID: "node", ClusterID: "cluster", ShardIDs: []string{"shard0"}, BuildVersion: "test", LocalConfigDigest: strings.Repeat("b", 64), Mode: "passthrough", Serving: true, BackendReady: true}
}

func TestGatewayHeartbeatAPI(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.GatewayTokens = map[string]string{"gw": heartbeatToken}
	r := s.Router()
	path := "/api/v1/gateway/heartbeat/gw"
	v := serverHeartbeatFixture()
	before, err := s.buildProxySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{"", "wrong", "admin"} {
		if w := doRequestWithAuth(r, "POST", path, tok, v); w.Code != 401 {
			t.Fatalf("auth %d", w.Code)
		}
	}
	if w := doAuthRequest(r, "POST", path, v); w.Code != 401 {
		t.Fatalf("JWT accepted as machine credential: %d", w.Code)
	}
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, v); w.Code != 200 {
		t.Fatalf("report %d %s", w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, v); w.Code != 200 {
		t.Fatal("idempotent report rejected")
	}
	v.Sequence = 2
	v.BackendReady = false
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, v); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	v.Sequence = 1
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, v); w.Code != 409 {
		t.Fatalf("old seq %d", w.Code)
	}
	if w := doRequestWithAuth(r, "POST", "/api/v1/gateway/heartbeat/other", heartbeatToken, v); w.Code != 401 {
		t.Fatalf("other gateway auth %d", w.Code)
	}
	v.GatewayID = "other"
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, v); w.Code != 400 {
		t.Fatal("body identity mismatch accepted")
	}
	for _, body := range []any{map[string]any{"password": "secret"}, map[string]any{"padding": strings.Repeat("x", 17000)}, nil} {
		if w := doRequestWithAuth(r, "POST", path, heartbeatToken, body); w.Code != 400 {
			t.Fatalf("invalid %d", w.Code)
		}
	}
	if w := doRequest(r, "GET", "/api/v1/gateway/runtime", nil); w.Code != 401 {
		t.Fatal("public observations")
	}
	viewer, err := s.jwtSign("viewer", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	w := doRequestWithAuth(r, "GET", "/api/v1/gateway/runtime", viewer, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"unready"`) || strings.Contains(w.Body.String(), heartbeatToken) {
		t.Fatalf("runtime %d %s", w.Code, w.Body.String())
	}
	after, err := s.buildProxySnapshot()
	if err != nil || after.Version != before.Version {
		t.Fatal("heartbeat changed proxy snapshot", err)
	}
	// Unlike the Proxy middleware, missing gateway credentials never bypass auth.
	s.cfg.GatewayTokens = nil
	if w := doRequestWithAuth(r, "POST", path, heartbeatToken, serverHeartbeatFixture()); w.Code != 401 {
		t.Fatal("empty config bypassed auth")
	}
}

func TestGatewayHeartbeatEndToEnd(t *testing.T) {
	s := newTestServer(t, "")
	s.cfg.GatewayTokens = map[string]string{"gateway-local": heartbeatToken}
	httpServer := httptest.NewServer(s.Router())
	defer httpServer.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(heartbeatToken), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := gateway.DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.AdminListen = "127.0.0.1:0"
	cfg.ControlPlane = gateway.ControlPlaneConfig{ServerEndpoint: httpServer.URL, TokenFile: tokenFile, NodeID: "node", ClusterID: "cluster", ShardIDs: []string{"shard0", "shard1"}, HeartbeatInterval: time.Second, RequestTimeout: time.Second}
	g, err := gateway.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, err := s.db.ListGatewayObservations()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 {
			if rows[0].Status != "unready" || len(rows[0].ShardIDs) != 2 || rows[0].BootID == "" || rows[0].AppliedRevision != "" {
				t.Fatalf("observation %+v", rows[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no registration")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway stop blocked")
	}
	w := doAuthRequest(s.Router(), http.MethodGet, "/api/v1/gateway/runtime", nil)
	var reply struct {
		Data struct {
			Observations []apitypes.GatewayObservation `json:"observations"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || len(reply.Data.Observations) != 1 || reply.Data.Observations[0].Status != "draining" {
		t.Fatalf("final report %s %v", w.Body.String(), err)
	}
}
