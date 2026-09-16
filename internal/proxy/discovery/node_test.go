package discovery

import (
	"context"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNodeScopedSnapshotRequest(t *testing.T) {
	if got := endpointAddr(apitypes.ProxyEndpoint{Address: "::1", Port: 17579}); got != "[::1]:17579" {
		t.Fatal(got)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("node_id") != "node-3" || r.URL.Query().Get("known_version") != "v+1" || r.URL.Query().Get("wait") != "1s" || r.Header.Get("Authorization") != "Bearer internal" {
			t.Error(r.URL, r.Header)
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer s.Close()
	c := newServerClient(ServerConfig{Endpoint: s.URL, Token: "internal", NodeID: "node-3"}, s.Client())
	_, unchanged, err := c.FetchProxySnapshot(context.Background(), "v+1", time.Second)
	if err != nil || !unchanged {
		t.Fatal(unchanged, err)
	}
}
