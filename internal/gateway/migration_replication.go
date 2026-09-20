package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"strconv"
)

// Replication acknowledgement is required only for ownership transitions, not
// ordinary business writes. WAIT is not a consensus or disk fsync guarantee.
// All eligible replicas must be platform-managed; membership changes during a
// migration are forbidden. Never infer success from a timeout or a short ACK.
func waitMigrationReplicas(ctx context.Context, c migrationCaller, replicas int) error {
	if replicas == 0 {
		return nil
	}
	if replicas < 0 || replicas > 2 {
		return errors.New("invalid migration replica requirement")
	}
	b, err := c.call(ctx, "WAIT", strconv.Itoa(replicas), "1000")
	if err != nil || len(b) < 4 || b[0] != ':' {
		return errors.New("migration replication confirmation unavailable")
	}
	n, err := strconv.Atoi(string(b[1 : len(b)-2]))
	if err != nil || n < replicas {
		return errors.New("migration replicas have not acknowledged handoff")
	}
	return nil
}

// A fresh, guarded write establishes THIS connection's replication offset even
// when the preceding transition was an idempotent retry on another connection.
func (f *migrationFence) replicated(ctx context.Context, phase, attempt string) error {
	if f.replicas == 0 {
		return nil
	}
	if err := f.ack(ctx, migrationReplicationScript, phase, attempt, rand.Text()); err != nil {
		return err
	}
	return waitMigrationReplicas(ctx, f.c, f.replicas)
}

const migrationReplicationScript = `
if not bound() or phase()~=ARGV[3] then return reject() end
if ARGV[4]~='' and redis.call('HGET',KEYS[2],'attempt')~=ARGV[4] then return reject() end
redis.call('HSET',KEYS[2],'replication_barrier',ARGV[5]);return 1`
