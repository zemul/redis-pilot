package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/spf13/cobra"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func readColdRequest(path string, dst any) error {
	if path == "" {
		return errors.New("--file is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return err
	}
	if len(b) > 64<<10 {
		return errors.New("request exceeds 64KiB")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("expected one JSON request")
	}
	return nil
}

func newColdClusterCommand() *cobra.Command {
	root := &cobra.Command{Use: "cold-cluster", Aliases: []string{"custom-cluster"}, Short: "Manage Kvrocks custom clusters with optional Redis cache and migration tasks"}
	root.AddCommand(&cobra.Command{Use: "list", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return checkResp(client.Get("/api/v1/cold-clusters")) }})
	root.AddCommand(&cobra.Command{Use: "get NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return checkResp(client.Get("/api/v1/cold-clusters/" + url.PathEscape(args[0])))
	}})
	var createFile string
	create := &cobra.Command{Use: "create", Short: "Register existing dedicated instances (does not deploy or route Proxy)", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		var req apitypes.ColdClusterSpec
		if err := readColdRequest(createFile, &req); err != nil {
			return err
		}
		return checkResp(client.Post("/api/v1/cold-clusters", req))
	}}
	create.Flags().StringVar(&createFile, "file", "", "Cluster JSON specification")
	root.AddCommand(create)
	var provisionFile string
	provision := &cobra.Command{Use: "provision", Short: "Create all dedicated resources and a cluster task from one specification", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		var req apitypes.ClusterCreateRequest
		if err := readColdRequest(provisionFile, &req); err != nil {
			return err
		}
		return checkResp(client.Post("/api/v1/custom-clusters/create", req))
	}}
	provision.Flags().StringVar(&provisionFile, "file", "", "JSON with id, gateway_id, node_id, cold and optional cache")
	root.AddCommand(provision)
	var bootstrapFile string
	bootstrap := &cobra.Command{Use: "gateway-config NAME", Short: "Print initial Gateway YAML; does not deploy, initialize or publish ingress", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		var req apitypes.GatewayBootstrapRequest
		if err := readColdRequest(bootstrapFile, &req); err != nil {
			return err
		}
		resp, err := client.Post("/api/v1/custom-clusters/"+url.PathEscape(args[0])+"/gateway-config", req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("Gateway configuration rejected: %v", resp.Error)
		}
		raw, err := json.Marshal(resp.Data)
		if err != nil {
			return err
		}
		var config apitypes.GatewayBootstrap
		if err := json.Unmarshal(raw, &config); err != nil {
			return err
		}
		if config.YAML == "" {
			return errors.New("Server returned empty Gateway configuration")
		}
		_, err = fmt.Fprint(cmd.OutOrStdout(), config.YAML)
		return err
	}}
	bootstrap.Flags().StringVar(&bootstrapFile, "file", "", "Deployment JSON: server_endpoint, listen, admin_listen, state_dir, token_file")
	root.AddCommand(bootstrap)
	var taskFile string
	migrate := &cobra.Command{Use: "migrate NAME", Short: "Request expansion/shrink; reuse the same task ID on retries", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		var req apitypes.ColdTaskRequest
		if err := readColdRequest(taskFile, &req); err != nil {
			return err
		}
		return checkResp(client.Post("/api/v1/cold-clusters/"+url.PathEscape(args[0])+"/tasks", req))
	}}
	migrate.Flags().StringVar(&taskFile, "file", "", "JSON with id, expected_revision and target shards")
	root.AddCommand(migrate)
	var ingressFile string
	ingress := &cobra.Command{Use: "ingress NAME", Short: "Register immutable local Proxy ingress, or show it without --file", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		path := "/api/v1/cold-clusters/" + url.PathEscape(args[0]) + "/ingress"
		if ingressFile == "" {
			return checkResp(client.Get(path))
		}
		var req apitypes.ColdIngressRequest
		if err := readColdRequest(ingressFile, &req); err != nil {
			return err
		}
		return checkResp(client.Post(path, req))
	}}
	ingress.Flags().StringVar(&ingressFile, "file", "", "JSON with gateway_address and business Proxy password")
	root.AddCommand(ingress)
	return root
}
