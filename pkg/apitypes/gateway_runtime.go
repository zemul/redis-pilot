package apitypes

import (
	"encoding/hex"
	"errors"
	"regexp"
	"time"
)

const GatewayStaleAfter = 45 * time.Second

var gatewayIdentifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func ValidGatewayIdentifier(s string) bool { return gatewayIdentifier.MatchString(s) }

// Observations only: declared placement is not a routing assignment or lease.
// LocalConfigDigest is not an acknowledgement of a Server draft revision.
type GatewayHeartbeat struct {
	GatewayID         string   `json:"gateway_id"`
	BootID            string   `json:"boot_id"`
	Sequence          int64    `json:"sequence"`
	NodeID            string   `json:"node_id"`
	ClusterID         string   `json:"cluster_id"`
	ShardIDs          []string `json:"shard_ids"`
	BuildVersion      string   `json:"build_version"`
	LocalConfigDigest string   `json:"local_config_digest"`
	Mode              string   `json:"mode"`
	Serving           bool     `json:"serving"`
	BackendReady      bool     `json:"backend_ready"`
	Draining          bool     `json:"draining"`
}

func (v GatewayHeartbeat) Validate() error {
	invalid := errors.New("invalid gateway heartbeat")
	for _, id := range []string{v.GatewayID, v.BootID, v.NodeID, v.ClusterID} {
		if !ValidGatewayIdentifier(id) {
			return invalid
		}
	}
	if v.Sequence < 1 || len(v.ShardIDs) == 0 || len(v.ShardIDs) > 64 || len(v.BuildVersion) == 0 || len(v.BuildVersion) > 128 {
		return invalid
	}
	for _, c := range v.BuildVersion {
		if c < 32 || c > 126 {
			return invalid
		}
	}
	seen := make(map[string]bool)
	for _, id := range v.ShardIDs {
		if !ValidGatewayIdentifier(id) || seen[id] {
			return invalid
		}
		seen[id] = true
	}
	digest, err := hex.DecodeString(v.LocalConfigDigest)
	if err != nil || len(digest) != 32 || (v.Mode != "skeleton" && v.Mode != "passthrough") {
		return invalid
	}
	return nil
}

type GatewayObservation struct {
	GatewayHeartbeat
	ReceivedAt      string `json:"received_at"`
	Status          string `json:"status"`           // stale/conflict/draining/ready/unready, derived by Server
	AppliedRevision string `json:"applied_revision"` // Always empty until config publication exists.
	ConfigSource    string `json:"config_source"`    // local_file
}
