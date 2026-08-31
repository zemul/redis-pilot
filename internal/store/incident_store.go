package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Incident 是一次异常的完整生命周期记录。一行从 open 开始，恢复后写入 resolved_at
// 并转为 resolved，不再改动。设计见 docs/architecture/health-incidents.md。
type Incident struct {
	ID              string `json:"id"`
	Fingerprint     string `json:"fingerprint"`
	IncidentType    string `json:"incident_type"`
	Status          string `json:"status"`
	Severity        string `json:"severity"`
	Source          string `json:"source"`
	ObjectType      string `json:"object_type"`
	ObjectID        string `json:"object_id"`
	GroupName       string `json:"group_name,omitempty"`
	InstanceName    string `json:"instance_name,omitempty"`
	NodeName        string `json:"node_name,omitempty"`
	Summary         string `json:"summary"`
	Reason          string `json:"reason,omitempty"`
	Evidence        string `json:"evidence,omitempty"`
	FirstObservedAt string `json:"first_observed_at"`
	ResolvedAt      string `json:"resolved_at,omitempty"`
	LastSeenAt      string `json:"last_seen_at"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
	// DurationMs 由查询时计算注入，不落库。open 用 now-first_observed，resolved 用差值。
	DurationMs int64 `json:"duration_ms"`
}

// IncidentFilter 是列表查询的筛选条件。
type IncidentFilter struct {
	Status     string // open|resolved（空=全部公开状态）
	Severity   string // warning|critical
	Type       string
	Source     string
	ObjectType string
	ObjectID   string
	Group      string
	Instance   string
	Node       string
	From       string // RFC3339，按 first_observed_at 过滤
	To         string
	Limit      int
	Offset     int
}

const incidentCols = `id,fingerprint,incident_type,status,severity,source,object_type,object_id,
	group_name,instance_name,node_name,summary,reason,evidence,
	first_observed_at,resolved_at,last_seen_at,created_at,updated_at`

func scanIncident(row interface{ Scan(...interface{}) error }) (*Incident, error) {
	var i Incident
	err := row.Scan(&i.ID, &i.Fingerprint, &i.IncidentType, &i.Status, &i.Severity, &i.Source,
		&i.ObjectType, &i.ObjectID, &i.GroupName, &i.InstanceName, &i.NodeName,
		&i.Summary, &i.Reason, &i.Evidence,
		&i.FirstObservedAt, &i.ResolvedAt, &i.LastSeenAt, &i.CreatedAt, &i.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &i, nil
}

// CreateIncident 插入一条新异常记录。依赖 active fingerprint 唯一索引兜底：
// 若同 fingerprint 已有 open 记录，插入会因唯一约束失败——调用方据此判断并发。
func (d *DB) CreateIncident(i Incident) error {
	now := time.Now().Format(time.RFC3339)
	if i.CreatedAt == "" {
		i.CreatedAt = now
	}
	if i.UpdatedAt == "" {
		i.UpdatedAt = now
	}
	if i.FirstObservedAt == "" {
		i.FirstObservedAt = now
	}
	if i.LastSeenAt == "" {
		i.LastSeenAt = i.FirstObservedAt
	}
	if i.Status == "" {
		i.Status = "open"
	}
	if i.Evidence == "" {
		i.Evidence = "{}"
	}
	return d.execWrite(`INSERT INTO health_incidents(`+incidentCols+`)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		i.ID, i.Fingerprint, i.IncidentType, i.Status, i.Severity, i.Source,
		i.ObjectType, i.ObjectID, i.GroupName, i.InstanceName, i.NodeName,
		i.Summary, i.Reason, i.Evidence,
		i.FirstObservedAt, i.ResolvedAt, i.LastSeenAt, i.CreatedAt, i.UpdatedAt)
}

// GetActiveIncident 返回指定 fingerprint 当前未恢复(open)的记录，无则返回 (nil,nil)。
func (d *DB) GetActiveIncident(fingerprint string) (*Incident, error) {
	row := d.sql.QueryRow(`SELECT `+incidentCols+` FROM health_incidents
		WHERE fingerprint=? AND status='open' LIMIT 1`, fingerprint)
	i, err := scanIncident(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return i, nil
}

// TouchIncident 刷新一条 open 记录的最近观测时间/证据/级别（异常仍在持续）。
func (d *DB) TouchIncident(fingerprint, lastSeenAt, evidence, severity string) error {
	if evidence == "" {
		evidence = "{}"
	}
	return d.execWrite(`UPDATE health_incidents
		SET last_seen_at=?, evidence=?, severity=?, updated_at=?
		WHERE fingerprint=? AND status='open'`,
		lastSeenAt, evidence, severity, time.Now().Format(time.RFC3339), fingerprint)
}

// ResolveIncident 把指定 fingerprint 的 open 记录标记为已恢复。
// 返回是否有记录被更新（false 表示当时没有 open 记录）。
func (d *DB) ResolveIncident(fingerprint, resolvedAt string) (bool, error) {
	now := time.Now().Format(time.RFC3339)
	r, err := d.sql.Exec(`UPDATE health_incidents
		SET status='resolved', resolved_at=?, last_seen_at=CASE WHEN last_seen_at<? THEN last_seen_at ELSE last_seen_at END, updated_at=?
		WHERE fingerprint=? AND status='open'`,
		resolvedAt, resolvedAt, now, fingerprint)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err == nil && n > 0 {
		d.bumpGeneration()
	}
	return n > 0, err
}

// ListOpenIncidents 返回所有未恢复记录，供 Dashboard 摘要与重启后重建 fingerprint 映射。
func (d *DB) ListOpenIncidents() ([]Incident, error) {
	rows, err := d.sql.Query(`SELECT ` + incidentCols + ` FROM health_incidents
		WHERE status='open' ORDER BY first_observed_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Incident
	for rows.Next() {
		i, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *i)
	}
	return out, rows.Err()
}

// GetIncident 按 ID 返回单条记录。
func (d *DB) GetIncident(id string) (*Incident, error) {
	row := d.sql.QueryRow(`SELECT `+incidentCols+` FROM health_incidents WHERE id=?`, id)
	i, err := scanIncident(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fillDuration(i)
	return i, nil
}

// QueryIncidents 返回分页后的异常列表与总数。默认不返回 observing（本表本就没有该态）。
// 排序：未指定 status 时 open 优先、再按发生时间倒序；指定 status 时仅按时间倒序。
func (d *DB) QueryIncidents(f IncidentFilter) ([]Incident, int, error) {
	if f.Limit <= 0 {
		f.Limit = 30
	}
	where := []string{"1=1"}
	args := []interface{}{}
	add := func(clause string, val string) {
		if val != "" {
			where = append(where, clause)
			args = append(args, val)
		}
	}
	add("status = ?", f.Status)
	add("severity = ?", f.Severity)
	add("incident_type = ?", f.Type)
	add("source = ?", f.Source)
	add("object_type = ?", f.ObjectType)
	add("object_id = ?", f.ObjectID)
	add("group_name = ?", f.Group)
	add("instance_name = ?", f.Instance)
	add("node_name = ?", f.Node)
	add("first_observed_at >= ?", f.From)
	add("first_observed_at <= ?", f.To)
	whereClause := strings.Join(where, " AND ")

	var total int
	if err := d.sql.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM health_incidents WHERE %s`, whereClause), args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	order := "first_observed_at DESC, id DESC"
	if f.Status == "" {
		// open 优先(0)，resolved 其后(1)
		order = "CASE status WHEN 'open' THEN 0 ELSE 1 END, first_observed_at DESC, id DESC"
	}
	queryArgs := append(append([]interface{}{}, args...), f.Limit, f.Offset)
	rows, err := d.sql.Query(fmt.Sprintf(`SELECT %s FROM health_incidents WHERE %s ORDER BY %s LIMIT ? OFFSET ?`,
		incidentCols, whereClause, order), queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Incident
	for rows.Next() {
		i, err := scanIncident(rows)
		if err != nil {
			return nil, 0, err
		}
		fillDuration(i)
		out = append(out, *i)
	}
	return out, total, rows.Err()
}

// CleanupResolvedIncidents 删除 resolved_at 早于 before 的历史记录，单批限制 limit 条，
// 返回删除数量。分批调用避免 SQLite 长事务。
func (d *DB) CleanupResolvedIncidents(before string, limit int) (int, error) {
	if limit <= 0 {
		limit = 500
	}
	r, err := d.sql.Exec(`DELETE FROM health_incidents
		WHERE id IN (SELECT id FROM health_incidents WHERE status='resolved' AND resolved_at<>'' AND resolved_at < ? LIMIT ?)`,
		before, limit)
	if err != nil {
		return 0, err
	}
	n, err := r.RowsAffected()
	if err == nil && n > 0 {
		d.bumpGeneration()
	}
	return int(n), err
}

// fillDuration 按状态计算持续时间(毫秒)。open 用 now-first_observed，resolved 用差值。
func fillDuration(i *Incident) {
	start, err := time.Parse(time.RFC3339, i.FirstObservedAt)
	if err != nil {
		return
	}
	end := time.Now()
	if i.Status == "resolved" && i.ResolvedAt != "" {
		if t, err := time.Parse(time.RFC3339, i.ResolvedAt); err == nil {
			end = t
		}
	}
	if ms := end.Sub(start).Milliseconds(); ms > 0 {
		i.DurationMs = ms
	}
}
