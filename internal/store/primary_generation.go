package store

import (
	"database/sql"
	"fmt"
	"net/netip"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Triggers cover Sentinel, manual topology changes and snapshot writers in the
// SAME transaction as CurrentMaster. Tombstones prevent group-name ABA reuse.
func createPrimaryGenerationSchema(tx *sql.Tx) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS group_primary_generations (grp TEXT PRIMARY KEY, generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>0))`,
		`INSERT OR IGNORE INTO group_primary_generations SELECT name,1 FROM instance_groups`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_insert AFTER INSERT ON instance_groups BEGIN INSERT INTO group_primary_generations VALUES(NEW.name,1) ON CONFLICT(grp) DO UPDATE SET generation=generation+1; END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_update AFTER UPDATE ON instance_groups WHEN NEW.current_master!=OLD.current_master BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp=NEW.name; END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_delete AFTER DELETE ON instance_groups BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp=OLD.name; END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_instance AFTER UPDATE ON instances WHEN NEW.server!=OLD.server OR NEW.port!=OLD.port BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp IN (SELECT name FROM instance_groups WHERE current_master=NEW.name); END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_instance_delete AFTER DELETE ON instances BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp IN (SELECT name FROM instance_groups WHERE current_master=OLD.name); END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_host AFTER UPDATE ON servers WHEN NEW.endpoint!=OLD.endpoint BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp IN (SELECT g.name FROM instance_groups g JOIN instances i ON i.name=g.current_master WHERE i.server=NEW.name); END`,
		`CREATE TRIGGER IF NOT EXISTS primary_generation_host_delete AFTER DELETE ON servers BEGIN UPDATE group_primary_generations SET generation=generation+1 WHERE grp IN (SELECT g.name FROM instance_groups g JOIN instances i ON i.name=g.current_master WHERE i.server=OLD.name); END`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// GroupPrimaryVersion reads identity and version from one database snapshot.
// Observations and lock changes do not count as a new primary.
func (d *DB) GroupPrimaryVersion(group string) (string, uint64, error) {
	var master string
	var generation uint64
	err := d.sql.QueryRow(`SELECT g.current_master,v.generation FROM instance_groups g JOIN group_primary_generations v ON v.grp=g.name WHERE g.name=?`, group).Scan(&master, &generation)
	return master, generation, err
}

// ColdAssignment takes one SQLite snapshot, so an address cannot be paired with
// a generation from another election. Only the owning machine may receive it.
// This does not grant direct backend access or bypass group ownership guards.
func (d *DB) ColdAssignment(gateway, id string) (*apitypes.ColdCluster, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	c, err := loadColdCluster(tx, id)
	if err != nil {
		return nil, err
	}
	if c.GatewayID != gateway {
		return nil, ErrColdConflict
	}
	shards := append([]apitypes.ColdShard(nil), c.Shards...)
	if c.Task != nil && c.Task.Phase != "complete" {
		shards = append(shards, c.Task.Source...)
		shards = append(shards, c.Task.Target...)
	}
	c.Primaries = nil
	seen, groups, addresses := map[string]string{}, map[string]bool{}, map[string]bool{}
	for _, shard := range shards {
		if instance, ok := seen[shard.ID]; ok {
			if instance != shard.Instance {
				return nil, ErrColdConflict
			}
			continue
		}
		seen[shard.ID] = shard.Instance
		p := apitypes.ShardPrimary{ShardID: shard.ID}
		var host, endpoint, engine, role, desired string
		var port int
		err = tx.QueryRow(`SELECT g.name,i.name,v.generation,i.server,i.port,n.endpoint,g.engine,i.role,i.desired_state
		 FROM instances anchor JOIN instance_groups g ON g.name=anchor.grp
		 JOIN group_primary_generations v ON v.grp=g.name
		 JOIN instances i ON i.name=CASE WHEN g.type='standalone' AND g.current_master='' THEN anchor.name ELSE g.current_master END AND i.grp=g.name
		 JOIN servers n ON n.name=i.server WHERE anchor.name=?`, shard.Instance).
			Scan(&p.Group, &p.Instance, &p.Generation, &host, &port, &endpoint, &engine, &role, &desired)
		if err != nil {
			return nil, fmt.Errorf("resolve shard primary: %w", err)
		}
		if engine != c.StorageEngine() || role != "master" || desired != "running" || port < 1 || port > 65535 || p.Generation == 0 || groups[p.Group] {
			return nil, ErrColdConflict
		}
		ip := netip.MustParseAddr("127.0.0.1")
		if host != c.NodeID {
			ip, err = netip.ParseAddr(endpoint)
			if err != nil || !ip.Unmap().IsPrivate() || ip.Zone() != "" {
				return nil, ErrColdConflict
			}
		}
		p.Address = netip.AddrPortFrom(ip.Unmap(), uint16(port)).String()
		if err := tx.QueryRow(`SELECT count(*) FROM instances WHERE grp=? AND name!=?`, p.Group, p.Instance).Scan(&p.Replicas); err != nil {
			return nil, err
		}
		if addresses[p.Address] {
			return nil, ErrColdConflict
		}
		groups[p.Group], addresses[p.Address] = true, true
		c.Primaries = append(c.Primaries, p)
	}
	return c, tx.Commit()
}
