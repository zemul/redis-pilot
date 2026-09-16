package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/cli"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdCLIFileValidation(t *testing.T) {
	for _, body := range []string{`{"password":"not-supported"}`, `{} {}`, "{}" + strings.Repeat(" ", 64<<10)} {
		p := filepath.Join(t.TempDir(), "request.json")
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		var req apitypes.ColdClusterSpec
		if readColdRequest(p, &req) == nil {
			t.Fatal("invalid request accepted")
		}
	}
}
func TestColdCLICommands(t *testing.T) {
	var requests []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer api-token" {
			t.Error("missing token")
		}
		requests = append(requests, r.Method+" "+r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/tasks") {
			var req apitypes.ColdTaskRequest
			if json.NewDecoder(r.Body).Decode(&req) != nil || req.ID != "retry-safe" || req.ExpectedRevision != 1 {
				t.Error("task identity lost")
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true,"data":null}`))
	}))
	defer s.Close()
	previous := client
	defer func() { client = previous }()
	client = cli.NewClient(&cli.Config{Server: strings.TrimPrefix(s.URL, "http://"), Token: "api-token"})
	p := filepath.Join(t.TempDir(), "task.json")
	if err := os.WriteFile(p, []byte(`{"id":"retry-safe","expected_revision":1,"shards":[{"id":"a","instance":"a"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"list"}, {"get", "cluster"}, {"migrate", "cluster", "--file", p}, {"migrate", "cluster", "--file", p}} {
		cmd := newColdClusterCommand()
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 4 || requests[2] != "POST /api/v1/cold-clusters/cluster/tasks" || requests[2] != requests[3] {
		t.Fatal(requests)
	}
	if err := os.WriteFile(p, []byte(`{"gateway_address":"127.0.0.1:19555","password":"business"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"ingress", "cluster", "--file", p}, {"ingress", "cluster"}} {
		cmd := newColdClusterCommand()
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	if requests[4] != "POST /api/v1/cold-clusters/cluster/ingress" || requests[5] != "GET /api/v1/cold-clusters/cluster/ingress" {
		t.Fatal(requests)
	}
}
