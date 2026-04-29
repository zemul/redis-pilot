package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Logger struct {
	dir    string
	stdout bool
}

type Entry struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

func New(dir string, stdout bool) *Logger {
	return &Logger{dir: dir, stdout: stdout}
}

func (l *Logger) log(level, msg string) {
	entry := Entry{
		Time:    time.Now().Format(time.RFC3339),
		Level:   level,
		Message: msg,
	}
	line, _ := json.Marshal(entry)
	line = append(line, '\n')

	var writers []io.Writer
	if l.stdout {
		writers = append(writers, os.Stdout)
	}
	if l.dir != "" {
		if f := l.dailyFile(); f != nil {
			defer f.Close()
			writers = append(writers, f)
		}
	}
	for _, w := range writers {
		w.Write(line)
	}
}

func (l *Logger) dailyFile() *os.File {
	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return nil
	}
	name := filepath.Join(l.dir, fmt.Sprintf("%s.jsonl", time.Now().Format("2006-01-02")))
	f, _ := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	return f
}

func (l *Logger) Info(msg string)  { l.log("info", msg) }
func (l *Logger) Error(msg string) { l.log("error", msg) }
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}
