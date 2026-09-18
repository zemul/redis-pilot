package apitypes

// GatewayBootstrapRequest contains deployment choices, never secret contents.
type GatewayBootstrapRequest struct {
	ServerEndpoint string `json:"server_endpoint"`
	Listen         string `json:"listen"`
	AdminListen    string `json:"admin_listen"`
	StateDir       string `json:"state_dir"`
	TokenFile      string `json:"token_file"`
}

type GatewayBootstrap struct {
	ClusterID string `json:"cluster_id"`
	NodeID    string `json:"node_id"`
	Revision  uint64 `json:"revision"`
	YAML      string `json:"yaml"`
}
