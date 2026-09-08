package gateway

import (
	"errors"
	"strings"
)

const syncTaskIDLimit = 64

// ValidateIsolatedSyncTaskID validates an explicit task identity. Empty IDs are
// reserved for the legacy single-task adapter, not a named task identity.
func ValidateIsolatedSyncTaskID(id string) error {
	if len(id) == 0 || len(id) > syncTaskIDLimit {
		return errors.New("sync task ID must contain 1 to 64 ASCII letters, digits, underscores or hyphens")
	}
	for i := 0; i < len(id); i++ {
		b := id[i]
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-') {
			return errors.New("sync task ID must contain 1 to 64 ASCII letters, digits, underscores or hyphens")
		}
	}
	return nil
}

func (c *syncColdClient) metadataKey(suffix string) string {
	if c.taskID == "" {
		return syncMetadataPrefix + "{isolated}:" + suffix
	}
	return syncMetadataPrefix + "{task:" + c.taskID + "}:" + suffix
}

func (c *syncColdClient) pendingCheckpointKey() string { return c.metadataKey("v2-checkpoint") }
func (c *syncColdClient) pendingKey() string           { return c.metadataKey("v2-pending") }
func (c *syncColdClient) anchorKey() string            { return c.metadataKey("v2-anchor") }
func (c *syncColdClient) evictionReceiptKey() string   { return c.metadataKey("eviction-receipt-v1") }

func (c *syncColdClient) ownsMetadataKey(key string) bool {
	return key == c.pendingCheckpointKey() || key == c.pendingKey() || key == c.anchorKey() ||
		(c.evictionReceipt && c.taskID != "" && key == c.evictionReceiptKey()) ||
		(c.taskID == "" && key == syncCheckpointKey)
}

func (c *syncColdClient) metadataScanBudget() (int, int) {
	count := 3
	bytes := len(c.pendingCheckpointKey()) + len(c.pendingKey()) + len(c.anchorKey())
	if c.evictionReceipt && c.taskID != "" {
		count++
		bytes += len(c.evictionReceiptKey())
	}
	if c.taskID == "" {
		count++
		bytes += len(syncCheckpointKey)
	}
	return count, bytes
}

// A namespace is not permission to share a cold DB. Named tasks reject any
// foreign metadata at connection/reconnect, including otherwise valid task
// namespaces. Bootstrap still separately scans the entire DB for emptiness.
// This is a fail-closed sanity check, NOT a distributed ownership fence.
func (c *syncColdClient) checkMetadataOwnership() error {
	count, bytes := c.metadataScanBudget()
	_, err := c.scanKeys(syncMetadataPrefix+"*", count, bytes)
	return err
}

func (c *syncColdClient) rejectForeignMetadata(key string) bool {
	return strings.HasPrefix(key, syncMetadataPrefix) && !c.ownsMetadataKey(key)
}
