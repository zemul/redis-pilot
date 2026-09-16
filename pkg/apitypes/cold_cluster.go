package apitypes

// Cold shards reference existing passwordless standalone instances. ID is the
// stable HRW identity, Instance is the platform resource, Address is resolved
// by Server and must match the Gateway's local endpoint registry.
type ColdShard struct {
	ID       string `json:"id"`
	Instance string `json:"instance"`
	Address  string `json:"address,omitempty"`
}

type ColdClusterSpec struct {
	ID          string      `json:"id"`
	GatewayID   string      `json:"gateway_id"`
	NodeID      string      `json:"node_id"`
	HotInstance string      `json:"hot_instance"`
	Shards      []ColdShard `json:"shards"`
}

type ColdCluster struct {
	ColdClusterSpec
	HotAddress string    `json:"hot_address"`
	Revision   uint64    `json:"revision"`
	Task       *ColdTask `json:"task,omitempty"`
}

type ColdTaskRequest struct {
	ID               string      `json:"id"`
	ExpectedRevision uint64      `json:"expected_revision"`
	Shards           []ColdShard `json:"shards"`
}

type ColdTask struct {
	ID           string      `json:"id"`
	ClusterID    string      `json:"cluster_id"`
	FromRevision uint64      `json:"from_revision"`
	ToRevision   uint64      `json:"to_revision"`
	Source       []ColdShard `json:"source"`
	Target       []ColdShard `json:"target"`
	Phase        string      `json:"phase"` // pending, active, published, complete
	ErrorCode    string      `json:"error_code,omitempty"`
	Pages        int64       `json:"pages"`
	Attempts     int64       `json:"attempts"`
}

type ColdTaskReport struct {
	TaskID     string `json:"task_id"`
	ToRevision uint64 `json:"to_revision"`
	Phase      string `json:"phase"`
	ErrorCode  string `json:"error_code,omitempty"`
	Pages      int64  `json:"pages"`
	Attempts   int64  `json:"attempts"`
}
