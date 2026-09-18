package main

import (
	"bytes"
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

func TestColdCLIGatewayConfig(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "rejected"}[success], func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/custom-clusters/orders/gateway-config" || r.Method != "POST" {
					t.Error("wrong endpoint")
				}
				var req apitypes.GatewayBootstrapRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Listen != "127.0.0.1:17579" {
					t.Error("deployment choices lost", err)
				}
				json.NewEncoder(w).Encode(apitypes.APIResponse{Success: success, Error: "not ready", Data: apitypes.GatewayBootstrap{YAML: "mode: cache\n"}})
			}))
			defer s.Close()
			previous := client
			defer func() { client = previous }()
			client = cli.NewClient(&cli.Config{Server: strings.TrimPrefix(s.URL, "http://")})
			p := filepath.Join(t.TempDir(), "deployment.json")
			if err := os.WriteFile(p, []byte(`{"listen":"127.0.0.1:17579"}`), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			cmd := newColdClusterCommand()
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			cmd.SetOut(&out)
			cmd.SetArgs([]string{"gateway-config", "orders", "--file", p})
			err := cmd.Execute()
			if success && (err != nil || out.String() != "mode: cache\n") {
				t.Fatal("not raw YAML", err, out.String())
			}
			if !success && (err == nil || out.Len() != 0) {
				t.Fatal("rejection emitted configuration")
			}
		})
	}
}

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

func TestColdCLIProvisionSpec(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/custom-clusters/create" {
			t.Errorf("unexpected route: %s %s", r.Method, r.URL.Path)
		}
		var req apitypes.ClusterCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID != "orders" || req.Cold.Count != 2 || req.Cache.WritePolicy != "update_existing" {
			t.Errorf("spec lost: %+v, %v", req, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"success":true,"data":{"task_id":"parent","status":"pending"}}`))
	}))
	defer s.Close()
	previous := client
	defer func() { client = previous }()
	client = cli.NewClient(&cli.Config{Server: strings.TrimPrefix(s.URL, "http://")})
	p := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(p, []byte(`{"id":"orders","gateway_id":"gw","node_id":"node","cold":{"count":2,"memory":"2Gi"},"cache":{"memory":"4Gi","write_policy":"update_existing"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := newColdClusterCommand()
	cmd.SetArgs([]string{"provision", "--file", p})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
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
