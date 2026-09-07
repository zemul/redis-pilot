package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

var (
	ErrGatewayDraftInvalid  = errors.New("invalid gateway draft")
	ErrGatewayDraftNotFound = errors.New("gateway draft not found")
	ErrGatewayDraftConflict = errors.New("gateway draft already exists or revision changed")
	gatewayDraftName        = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
)

// Drafts do not change topology generation or Proxy snapshots, reserve ports,
// deploy processes, or prevent ordinary node/group deletion. Deployment must
// revalidate references, resources, credentials and current primary separately.
func (d *DB) validateGatewayDraft(name string, spec *apitypes.GatewayDraftSpec) error {
	invalid := func(reason string) error { return fmt.Errorf("%w: %s", ErrGatewayDraftInvalid, reason) }
	if !gatewayDraftName.MatchString(name) {
		return invalid("name must be 1-64 ASCII letters, digits, underscores or hyphens, starting with a letter or digit")
	}
	if spec.Mode == "" {
		spec.Mode = "passthrough"
	}
	if spec.Mode != "passthrough" {
		return invalid("only passthrough drafts are supported")
	}
	if spec.Node == "" || spec.Group == "" || len(spec.Node) > 128 || len(spec.Group) > 128 || strings.TrimSpace(spec.Node) != spec.Node || strings.TrimSpace(spec.Group) != spec.Group {
		return invalid("node and group are required, at most 128 bytes without surrounding whitespace")
	}
	if spec.DataPort < 1024 || spec.DataPort > 65535 || spec.AdminPort < 1024 || spec.AdminPort > 65535 || spec.DataPort == spec.AdminPort {
		return invalid("distinct data/admin ports in 1024..65535 are required")
	}
	var exists bool
	if err := d.sql.QueryRow(`SELECT EXISTS(SELECT 1 FROM servers WHERE name=?)`, spec.Node).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return invalid("node does not exist")
	}
	var engine, kind string
	err := d.sql.QueryRow(`SELECT engine,type FROM instance_groups WHERE name=?`, spec.Group).Scan(&engine, &kind)
	if errors.Is(err, sql.ErrNoRows) {
		return invalid("instance group does not exist")
	}
	if err != nil {
		return err
	}
	if engine != "redis" || (kind != "standalone" && kind != "replication") {
		return invalid("backend must be a Redis standalone or replication group")
	}
	return nil
}

func (d *DB) ListGatewayDrafts() ([]apitypes.GatewayDraft, error) {
	rows, err := d.sql.Query(`SELECT name,spec,revision,created_at,updated_at FROM gateway_drafts ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]apitypes.GatewayDraft, 0)
	for rows.Next() {
		v, err := scanGatewayDraft(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *v)
	}
	return result, rows.Err()
}

func scanGatewayDraft(row interface{ Scan(...any) error }) (*apitypes.GatewayDraft, error) {
	v := &apitypes.GatewayDraft{Status: "draft"}
	var raw string
	if err := row.Scan(&v.Name, &raw, &v.Revision, &v.CreatedAt, &v.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrGatewayDraftNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &v.Spec); err != nil {
		return nil, err
	}
	return v, nil
}

func (d *DB) GetGatewayDraft(name string) (*apitypes.GatewayDraft, error) {
	return scanGatewayDraft(d.sql.QueryRow(`SELECT name,spec,revision,created_at,updated_at FROM gateway_drafts WHERE name=?`, name))
}

// expected="" means create-only; updates require the exact current revision.
func (d *DB) SaveGatewayDraft(name string, spec apitypes.GatewayDraftSpec, expected string) (*apitypes.GatewayDraft, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.validateGatewayDraft(name, &spec); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	v := &apitypes.GatewayDraft{Name: name, Spec: spec, Revision: rand.Text(), Status: "draft", CreatedAt: now, UpdatedAt: now}
	var result sql.Result
	if expected == "" {
		result, err = d.sql.Exec(`INSERT INTO gateway_drafts(name,spec,revision,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(name) DO NOTHING`, name, string(raw), v.Revision, now, now)
	} else {
		old, e := d.GetGatewayDraft(name)
		if e != nil {
			return nil, e
		}
		v.CreatedAt = old.CreatedAt
		result, err = d.sql.Exec(`UPDATE gateway_drafts SET spec=?,revision=?,updated_at=? WHERE name=? AND revision=?`, string(raw), v.Revision, now, name, expected)
	}
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrGatewayDraftConflict
	}
	return v, nil
}

func (d *DB) DeleteGatewayDraft(name, expected string) error {
	if expected == "" {
		return fmt.Errorf("%w: revision is required", ErrGatewayDraftInvalid)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.GetGatewayDraft(name); err != nil {
		return err
	}
	result, err := d.sql.Exec(`DELETE FROM gateway_drafts WHERE name=? AND revision=?`, name, expected)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrGatewayDraftConflict
	}
	return nil
}
