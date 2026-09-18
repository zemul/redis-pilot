package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Task records the durable state of a long-running operation coordinated by Server.
type Task struct {
	ID              string `json:"id"`
	Kind            string `json:"kind"`
	Status          string `json:"status"`
	Phase           string `json:"phase,omitempty"`
	Progress        int64  `json:"progress"`
	Message         string `json:"message,omitempty"`
	Cancelable      bool   `json:"cancelable"`
	CancelRequested bool   `json:"cancel_requested"`
	Payload         string `json:"-"`
	Result          string `json:"result,omitempty"`
	Error           string `json:"error,omitempty"`
	Operator        string `json:"operator"`
	Source          string `json:"source"`
	Trigger         string `json:"trigger"`
	GroupName       string `json:"group_name,omitempty"`
	InstanceName    string `json:"instance_name,omitempty"`
	ServerName      string `json:"server_name,omitempty"`
	CreatedAt       string `json:"created_at"`
	StartedAt       string `json:"started_at,omitempty"`
	FinishedAt      string `json:"finished_at,omitempty"`
	UpdatedAt       string `json:"updated_at"`
}

type TaskFilter struct {
	Status   string
	Kind     string
	Operator string
	Instance string
	Query    string
	Limit    int
	Offset   int
}

func (d *DB) CreateTask(t Task) error {
	err := insertTask(t, d.sql.Exec)
	if err == nil {
		d.bumpGeneration()
	}
	return err
}

func insertTask(t Task, exec func(string, ...any) (sql.Result, error)) error {
	now := time.Now().Format(time.RFC3339)
	if t.CreatedAt == "" {
		t.CreatedAt = now
	}
	if t.UpdatedAt == "" {
		t.UpdatedAt = now
	}
	if t.Status == "running" && t.StartedAt == "" {
		t.StartedAt = now
	}
	if (t.Status == "success" || t.Status == "failed" || t.Status == "interrupted" || t.Status == "canceled") && t.FinishedAt == "" {
		t.FinishedAt = now
	}
	if t.Operator == "" {
		t.Operator = "system"
	}
	if t.Source == "" {
		t.Source = "system"
	}
	if t.Trigger == "" {
		t.Trigger = "automatic"
	}
	_, err := exec(`INSERT INTO tasks(id,kind,status,phase,progress,message,cancelable,cancel_requested,payload,result,error,operator,source,trigger,
		group_name,instance_name,server_name,created_at,started_at,finished_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, t.ID, t.Kind, t.Status, t.Phase, t.Progress, t.Message, t.Cancelable, t.CancelRequested, t.Payload, t.Result, t.Error,
		t.Operator, t.Source, t.Trigger, t.GroupName, t.InstanceName, t.ServerName,
		t.CreatedAt, t.StartedAt, t.FinishedAt, t.UpdatedAt)
	return err
}

func (d *DB) GetTask(id string) (*Task, error) {
	var t Task
	err := d.sql.QueryRow(`SELECT id,kind,status,phase,progress,message,cancelable,cancel_requested,payload,result,error,operator,source,trigger,
		group_name,instance_name,server_name,created_at,started_at,finished_at,updated_at FROM tasks WHERE id=?`, id).
		Scan(&t.ID, &t.Kind, &t.Status, &t.Phase, &t.Progress, &t.Message, &t.Cancelable, &t.CancelRequested, &t.Payload, &t.Result, &t.Error, &t.Operator, &t.Source, &t.Trigger,
			&t.GroupName, &t.InstanceName, &t.ServerName, &t.CreatedAt, &t.StartedAt, &t.FinishedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// QueryTasks returns task summaries ordered newest first. Payload is deliberately
// not exposed by Task's JSON shape because it may contain operation parameters.
func (d *DB) QueryTasks(f TaskFilter) ([]Task, int, error) {
	if f.Limit <= 0 {
		f.Limit = 30
	}
	where := []string{"1=1"}
	args := []interface{}{}
	if f.Status == "active" {
		where = append(where, "status IN ('pending','accepted','running')")
	} else if f.Status == "failed" {
		where = append(where, "status IN ('failed','interrupted')")
	} else if f.Status != "" {
		where = append(where, "status = ?")
		args = append(args, f.Status)
	}
	if f.Kind != "" {
		where = append(where, "kind = ?")
		args = append(args, f.Kind)
	}
	if f.Operator != "" {
		where = append(where, "operator = ?")
		args = append(args, f.Operator)
	}
	if f.Instance != "" {
		where = append(where, "instance_name = ?")
		args = append(args, f.Instance)
	}
	if f.Query != "" {
		where = append(where, "(id LIKE ? OR group_name LIKE ? OR instance_name LIKE ? OR server_name LIKE ? OR operator LIKE ?)")
		like := "%" + f.Query + "%"
		args = append(args, like, like, like, like, like)
	}
	whereClause := strings.Join(where, " AND ")
	var total int
	if err := d.sql.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM tasks WHERE %s`, whereClause), args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]interface{}{}, args...), f.Limit, f.Offset)
	rows, err := d.sql.Query(fmt.Sprintf(`SELECT id,kind,status,phase,progress,message,cancelable,cancel_requested,payload,result,error,operator,source,trigger,
		group_name,instance_name,server_name,created_at,started_at,finished_at,updated_at
		FROM tasks WHERE %s ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`, whereClause), queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Kind, &t.Status, &t.Phase, &t.Progress, &t.Message, &t.Cancelable, &t.CancelRequested, &t.Payload, &t.Result, &t.Error, &t.Operator, &t.Source, &t.Trigger,
			&t.GroupName, &t.InstanceName, &t.ServerName, &t.CreatedAt, &t.StartedAt, &t.FinishedAt, &t.UpdatedAt); err != nil {
			return nil, 0, err
		}
		tasks = append(tasks, t)
	}
	return tasks, total, rows.Err()
}

func (d *DB) ListActiveTasks() ([]Task, error) {
	rows, err := d.sql.Query(`SELECT id,kind,status,phase,progress,message,cancelable,cancel_requested,payload,result,error,operator,source,trigger,
		group_name,instance_name,server_name,created_at,started_at,finished_at,updated_at
		FROM tasks WHERE status IN ('pending','accepted','running') ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Kind, &t.Status, &t.Phase, &t.Progress, &t.Message, &t.Cancelable, &t.CancelRequested, &t.Payload, &t.Result, &t.Error, &t.Operator, &t.Source, &t.Trigger,
			&t.GroupName, &t.InstanceName, &t.ServerName, &t.CreatedAt, &t.StartedAt, &t.FinishedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (d *DB) UpdateTaskStatus(id, status, result, errMsg string) error {
	now := time.Now().Format(time.RFC3339)
	return d.execWrite(`UPDATE tasks SET status=?,result=?,error=?,
		started_at=CASE WHEN ?='running' AND started_at='' THEN ? ELSE started_at END,
		finished_at=CASE WHEN ? IN ('success','failed','interrupted','canceled') THEN ? ELSE finished_at END,
		cancelable=CASE WHEN ? IN ('success','failed','interrupted','canceled') THEN 0 ELSE cancelable END,
		updated_at=? WHERE id=?`, status, result, errMsg, status, now, status, now, status, now, id)
}

func (d *DB) UpdateTaskProgress(id, phase string, progress int64, message, result string, cancelable, cancelRequested bool) error {
	return d.execWrite(`UPDATE tasks SET phase=?,progress=?,message=?,result=?,cancelable=?,cancel_requested=?,updated_at=?
		WHERE id=? AND status IN ('pending','accepted','running')`, phase, progress, message, result, cancelable, cancelRequested, time.Now().Format(time.RFC3339), id)
}

func (d *DB) MarkTaskCancelRequested(id string) (bool, error) {
	r, err := d.sql.Exec(`UPDATE tasks SET cancel_requested=1,message='正在取消',updated_at=?
		WHERE id=? AND status IN ('pending','accepted','running') AND cancelable=1`, time.Now().Format(time.RFC3339), id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err == nil && n > 0 {
		d.bumpGeneration()
	}
	return n > 0, err
}

// CompleteTask transitions an active task to a terminal state. changed is false if
// another poller already completed it, which keeps final audit logging idempotent.
func (d *DB) CompleteTask(id, status, result, errMsg string) (changed bool, err error) {
	now := time.Now().Format(time.RFC3339)
	r, err := d.sql.Exec(`UPDATE tasks SET status=?,result=?,error=?,cancelable=0,finished_at=?,updated_at=?
		WHERE id=? AND status IN ('pending','accepted','running')`, status, result, errMsg, now, now, id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err == nil && n > 0 {
		d.bumpGeneration()
	}
	return n > 0, err
}
