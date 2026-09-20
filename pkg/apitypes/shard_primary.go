package apitypes

// ShardPrimary identifies a Server-confirmed primary. Generation is persisted
// with the group topology, not derived from uptime or event arrival order.
type ShardPrimary struct {
	ShardID    string `json:"shard_id"`
	Group      string `json:"group"`
	Instance   string `json:"instance"`
	Address    string `json:"address"`
	Generation uint64 `json:"generation"`
	Replicas   int    `json:"replicas,omitempty"`
}
