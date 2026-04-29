package audit

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Level 审计级别
type Level string

const (
	LevelNormal   Level = "normal"
	LevelImportant Level = "important"
	LevelCritical Level = "critical"
)

// Record 审计日志记录
type Record struct {
	ID        string                 `json:"id"`
	Timestamp string                 `json:"timestamp"`
	Operator  string                 `json:"operator"`
	Action    string                 `json:"action"`
	Level     Level                  `json:"level"`
	Target    map[string]interface{} `json:"target,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
	Result    string                 `json:"result"` // success | failed
	Duration  int64                  `json:"duration_ms,omitempty"`
	Detail    string                 `json:"detail,omitempty"`
}

// ChecksumRecord 每日校验记录
type ChecksumRecord struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Date        string `json:"date"`
	RecordCount int    `json:"record_count"`
	SHA256      string `json:"sha256"`
	GeneratedAt string `json:"generated_at"`
}

// Logger 审计日志写入器
type Logger struct {
	dir     string
	mu      sync.Mutex
	seq     atomic.Int64
	curDate string
	curFile *os.File
}

// New 创建审计日志写入器
func New(dataDir string) *Logger {
	dir := filepath.Join(dataDir, "audit")
	os.MkdirAll(dir, 0755)
	return &Logger{dir: dir}
}

// Log 写入一条审计记录
func (l *Logger) Log(r Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	today := now.Format("20060102")

	if r.Timestamp == "" {
		r.Timestamp = now.Format(time.RFC3339)
	}
	if r.ID == "" {
		seq := l.seq.Add(1)
		r.ID = fmt.Sprintf("audit-%s-%04d", today, seq)
	}

	f, err := l.getFile(today)
	if err != nil {
		return err
	}

	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	return err
}

// Verify 校验指定日期的审计日志完整性
func (l *Logger) Verify(date string) (bool, error) {
	path := filepath.Join(l.dir, "audit-"+date+".jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	// 找到最后一行校验记录，与前面所有记录的哈希比对
	lines := splitLines(data)
	if len(lines) < 2 {
		return false, fmt.Errorf("no checksum record found")
	}

	lastLine := lines[len(lines)-1]
	var cs ChecksumRecord
	if err := json.Unmarshal(lastLine, &cs); err != nil || cs.Type != "daily_checksum" {
		return false, fmt.Errorf("last line is not a checksum record")
	}

	// 计算前面所有行的 SHA256
	var content []byte
	for _, line := range lines[:len(lines)-1] {
		content = append(content, line...)
		content = append(content, '\n')
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	return hash == cs.SHA256, nil
}

// GenerateDailyChecksum 生成指定日期的校验和记录
func (l *Logger) GenerateDailyChecksum(date string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	path := filepath.Join(l.dir, "audit-"+date+".jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	lines := splitLines(data)
	// 跳过已有的 checksum 行
	var records [][]byte
	for _, line := range lines {
		var check struct{ Type string }
		json.Unmarshal(line, &check)
		if check.Type != "daily_checksum" {
			records = append(records, line)
		}
	}

	var content []byte
	for _, line := range records {
		content = append(content, line...)
		content = append(content, '\n')
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(content))

	cs := ChecksumRecord{
		ID:          "audit-" + date + "-checksum",
		Type:        "daily_checksum",
		Date:        date,
		RecordCount: len(records),
		SHA256:      hash,
		GeneratedAt: time.Now().Format(time.RFC3339),
	}
	csData, _ := json.Marshal(cs)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(csData, '\n'))
	return err
}

func (l *Logger) getFile(date string) (*os.File, error) {
	if l.curDate == date && l.curFile != nil {
		return l.curFile, nil
	}
	if l.curFile != nil {
		l.curFile.Close()
	}
	path := filepath.Join(l.dir, "audit-"+date+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	l.curDate = date
	l.curFile = f
	l.seq.Store(0)
	return f, nil
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			line := data[start:i]
			if len(line) > 0 {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
