package store

import "time"

// Task records the durable state of a long-running operation coordinated by Server.
type Task struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	Payload   string `json:"-"`
	Result    string `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (d *DB) CreateTask(t Task) error {
	now := time.Now().Format(time.RFC3339)
	if t.CreatedAt == "" {
		t.CreatedAt = now
	}
	if t.UpdatedAt == "" {
		t.UpdatedAt = now
	}
	return d.execWrite(`INSERT INTO tasks(id,kind,status,payload,result,error,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?)`, t.ID, t.Kind, t.Status, t.Payload, t.Result, t.Error, t.CreatedAt, t.UpdatedAt)
}

func (d *DB) GetTask(id string) (*Task, error) {
	var t Task
	err := d.sql.QueryRow(`SELECT id,kind,status,payload,result,error,created_at,updated_at FROM tasks WHERE id=?`, id).
		Scan(&t.ID, &t.Kind, &t.Status, &t.Payload, &t.Result, &t.Error, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (d *DB) ListActiveTasks() ([]Task, error) {
	rows, err := d.sql.Query(`SELECT id,kind,status,payload,result,error,created_at,updated_at
		FROM tasks WHERE status IN ('pending','accepted','running') ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Kind, &t.Status, &t.Payload, &t.Result, &t.Error, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (d *DB) UpdateTaskStatus(id, status, result, errMsg string) error {
	return d.execWrite(`UPDATE tasks SET status=?,result=?,error=?,updated_at=? WHERE id=?`,
		status, result, errMsg, time.Now().Format(time.RFC3339), id)
}

// CompleteTask transitions an active task to a terminal state. changed is false if
// another poller already completed it, which keeps final audit logging idempotent.
func (d *DB) CompleteTask(id, status, result, errMsg string) (changed bool, err error) {
	r, err := d.sql.Exec(`UPDATE tasks SET status=?,result=?,error=?,updated_at=?
		WHERE id=? AND status IN ('pending','accepted','running')`, status, result, errMsg, time.Now().Format(time.RFC3339), id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err == nil && n > 0 {
		d.bumpGeneration()
	}
	return n > 0, err
}
