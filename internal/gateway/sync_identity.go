package gateway

import (
	"errors"
	"net"
	"strconv"
)

var errSyncTaskIdentity = errors.New("replication journal task identity does not match configuration")

// This local binding prevents accidental reuse of another task's journal. It
// is not a lease, backend fencing token, or permission to fail over the source.
// Endpoint identities are stored only in the private journal, never in metrics.
type syncTaskIdentity struct {
	TaskID string `json:"task_id"`
	Source string `json:"source"`
	Cold   string `json:"cold"`
}

func syncCanonicalEndpoint(address string) string {
	ip, port, ok := syncLiteralEndpoint(address)
	if !ok {
		return ""
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(port))
}

func validSyncTaskIdentity(identity *syncTaskIdentity) bool {
	return identity != nil && ValidateIsolatedSyncTaskID(identity.TaskID) == nil &&
		syncCanonicalEndpoint(identity.Source) == identity.Source && identity.Source != "" &&
		syncCanonicalEndpoint(identity.Cold) == identity.Cold && identity.Cold != "" &&
		identity.Source != identity.Cold
}

func sameSyncTaskIdentity(a, b *syncTaskIdentity) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func cloneSyncTaskIdentity(identity *syncTaskIdentity) *syncTaskIdentity {
	if identity == nil {
		return nil
	}
	copy := *identity
	return &copy
}

func (o IsolatedSyncOptions) identity() *syncTaskIdentity {
	if o.TaskID == "" {
		return nil // preserve the existing unbound single-task format, not auto-adopt it
	}
	return &syncTaskIdentity{TaskID: o.TaskID, Source: syncCanonicalEndpoint(o.SourceAddress), Cold: syncCanonicalEndpoint(o.ColdAddress)}
}

func validSyncManifestIdentity(m syncSegmentManifest) bool {
	return (m.Version == 1 && m.Identity == nil) || (m.Version == 2 && validSyncTaskIdentity(m.Identity))
}

func (target syncColdTarget) checkJournalIdentity(j *syncSegments) error {
	if j == nil {
		return errSyncTaskIdentity
	}
	j.mu.Lock()
	identity := cloneSyncTaskIdentity(j.manifest.Identity)
	j.mu.Unlock()
	if identity == nil {
		if target.taskID == "" {
			return nil
		}
		return errSyncTaskIdentity
	}
	if !validSyncTaskIdentity(identity) || identity.TaskID != target.taskID || identity.Cold != syncCanonicalEndpoint(target.address) {
		return errSyncTaskIdentity
	}
	return nil
}
