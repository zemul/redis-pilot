package apitypes

import (
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"
)

const SyncerStaleAfter = 45 * time.Second

var syncIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func ValidSyncIdentifier(s string) bool { return syncIdentifier.MatchString(s) }

// Observations of local-file tasks, not grants to execute or fencing tokens.
type SyncTaskReport struct {
	TaskID         string        `json:"task_id"`
	IdentityDigest string        `json:"identity_digest"`
	State          string        `json:"state"`
	LastError      string        `json:"last_error"`
	Running        bool          `json:"running"`
	Progress       *SyncProgress `json:"progress,omitempty"`
}

type SyncerHeartbeat struct {
	SyncerID     string           `json:"syncer_id"`
	BootID       string           `json:"boot_id"`
	NodeID       string           `json:"node_id"`
	Sequence     int64            `json:"sequence"`
	BuildVersion string           `json:"build_version"`
	State        string           `json:"state"`
	LastError    string           `json:"last_error"`
	Running      bool             `json:"running"`
	Tasks        []SyncTaskReport `json:"tasks"`
}

func ValidSyncTaskState(s string) bool {
	switch s {
	case "idle", "starting", "applying", "running", "retry_source", "retry_cold", "pressure", "draining", "drained", "stopped", "failed", "unknown":
		return true
	}
	return false
}

func ValidSyncTaskError(s string) bool {
	switch s {
	case "none", "retry_exhausted", "pressure_timeout", "history_gap", "cold_outcome_uncertain", "cold_state_mismatch", "journal_corrupt", "journal_capacity", "resource_pressure", "source_transport", "cold_connection", "unsupported_record", "canceled", "unclassified_failure", "shutdown_timeout", "shutdown_incomplete", "shutdown_aborted", "unknown", "credential_file", "journal_open", "journal_close", "task_identity", "task_failure", "task_exited":
		return true
	}
	return false
}

func ValidSyncServiceState(s string) bool {
	switch s {
	case "created", "starting", "running", "degraded", "stopping", "stopped", "failed":
		return true
	}
	return false
}

func ValidSyncServiceError(s string) bool {
	switch s {
	case "none", "run_once", "config", "credentials", "admin_listen", "admin_serve", "admin_shutdown", "task_failure", "task_exited", "unknown":
		return true
	}
	return false
}

func (v SyncerHeartbeat) Validate() error {
	invalid := errors.New("invalid syncer heartbeat")
	if !ValidSyncIdentifier(v.SyncerID) || !ValidSyncIdentifier(v.BootID) || !ValidSyncIdentifier(v.NodeID) || v.Sequence < 1 || len(v.Tasks) < 1 || len(v.Tasks) > 8 || !ValidSyncServiceState(v.State) || !ValidSyncServiceError(v.LastError) {
		return invalid
	}
	if len(v.BuildVersion) < 1 || len(v.BuildVersion) > 128 {
		return invalid
	}
	for _, ch := range v.BuildVersion {
		if ch < 33 || ch > 126 {
			return invalid
		}
	}
	last := ""
	terminal := v.State == "stopped" || v.State == "failed"
	if terminal && v.Running {
		return invalid
	}
	for _, task := range v.Tasks {
		if task.Progress.Validate() != nil {
			return invalid
		}
		if terminal && task.Running {
			return invalid
		}
		digest, err := hex.DecodeString(task.IdentityDigest)
		if !ValidSyncIdentifier(task.TaskID) || task.TaskID <= last || err != nil || len(digest) != 32 || strings.ToLower(task.IdentityDigest) != task.IdentityDigest || !ValidSyncTaskState(task.State) || !ValidSyncTaskError(task.LastError) {
			return invalid
		}
		last = task.TaskID // Require unique sorted membership, stable across reports.
	}
	return nil
}

type SyncTaskBinding struct {
	TaskID         string `json:"task_id"`
	SyncerID       string `json:"syncer_id"`
	NodeID         string `json:"node_id"`
	IdentityDigest string `json:"identity_digest"`
	RegisteredAt   string `json:"registered_at"`
}

type SyncerObservation struct {
	SyncerHeartbeat
	ReceivedAt   string                      `json:"received_at"`
	Status       string                      `json:"status"` // stale/conflict or reported service state; never readiness
	ConfigSource string                      `json:"config_source"`
	TaskProgress map[string]SyncProgressView `json:"task_progress,omitempty"`
}

type SyncerRuntime struct {
	Bindings          []SyncTaskBinding   `json:"bindings"`
	Observations      []SyncerObservation `json:"observations"`
	StaleAfterSeconds int                 `json:"stale_after_seconds"`
}
