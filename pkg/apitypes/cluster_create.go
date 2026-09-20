package apitypes

// A placement is either an explicit node or an automatic selector. Resources
// remain dedicated storage groups behind a separately deployed Gateway.
type ClusterResourceSpec struct {
	Memory        string            `json:"memory"`
	CPUs          float64           `json:"cpus,omitempty"`
	Disk          string            `json:"disk,omitempty"`
	EngineVersion string            `json:"engine_version,omitempty"`
	NodeSelector  map[string]string `json:"node_selector,omitempty"`
}
type ClusterColdSpec struct {
	ClusterResourceSpec
	Count          int        `json:"count"`
	Servers        []string   `json:"servers,omitempty"`
	Replicas       int        `json:"replicas,omitempty"`
	ReplicaServers [][]string `json:"replica_servers,omitempty"`
}
type ClusterCacheSpec struct {
	ClusterResourceSpec
	Server      string `json:"server,omitempty"`
	WritePolicy string `json:"write_policy,omitempty"`
}
type ClusterCreateRequest struct {
	ID        string            `json:"id"`
	Engine    string            `json:"engine,omitempty"`
	GatewayID string            `json:"gateway_id"`
	NodeID    string            `json:"node_id"`
	Cold      ClusterColdSpec   `json:"cold"`
	Cache     *ClusterCacheSpec `json:"cache,omitempty"`
}
