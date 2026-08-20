// Package logger 提供统一的日志基础设施，支持按天切割、自动轮转、指定目录。
// 输出格式: 2026-08-19 11:05:32.176 [component] LEVEL [category] file:line - message key=value ...
package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Config 日志配置。
type Config struct {
	Dir           string // 日志目录，为空则不写文件
	Stdout        bool   // 是否同时输出到 stdout
	RetentionDays int    // 日志保留天数，0 = 不清理
	Component     string // 组件名，如 server/agent/proxy
	Level         string // 日志级别：debug/info/warn/error（默认 info）
}

// Logger 兼容旧接口（Server/Agent 使用），同时设置 slog 全局 default。
type Logger struct {
	slog   *slog.Logger
	writer *dailyWriter
}

// Setup 初始化全局日志，返回 Logger 实例（兼容 Server/Agent 的 s.log 用法）。
// 同时将 slog.Default() 设置为同一个 handler（Proxy 直接用 slog 包级函数）。
func Setup(cfg Config) *Logger {
	var writers []io.Writer

	if cfg.Stdout {
		writers = append(writers, os.Stdout)
	}

	var dw *dailyWriter
	if cfg.Dir != "" {
		dw = newDailyWriter(cfg.Dir)
		writers = append(writers, dw)
	}

	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	var w io.Writer
	if len(writers) == 1 {
		w = writers[0]
	} else {
		w = io.MultiWriter(writers...)
	}

	h := &customHandler{
		w:         w,
		component: cfg.Component,
		level:     parseLevel(cfg.Level),
	}
	sl := slog.New(h)
	slog.SetDefault(sl)

	l := &Logger{slog: sl, writer: dw}

	// 启动轮转清理
	if dw != nil && cfg.RetentionDays > 0 {
		go dw.cleanLoop(cfg.RetentionDays)
	}

	return l
}

// Info 兼容旧接口。
func (l *Logger) Info(msg string) {
	l.logAtLevel(slog.LevelInfo, msg)
}

// Error 兼容旧接口。
func (l *Logger) Error(msg string) {
	l.logAtLevel(slog.LevelError, msg)
}

// Infof 兼容旧接口。
func (l *Logger) Infof(format string, args ...interface{}) {
	l.logAtLevel(slog.LevelInfo, fmt.Sprintf(format, args...))
}

// Errorf 兼容旧接口。
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logAtLevel(slog.LevelError, fmt.Sprintf(format, args...))
}

// logAtLevel 用正确的 caller 深度记录日志。
func (l *Logger) logAtLevel(level slog.Level, msg string) {
	if !l.slog.Enabled(context.Background(), level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip: Callers, logAtLevel, Info/Error/Infof/Errorf
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	l.slog.Handler().Handle(context.Background(), r)
}

// Slog 返回底层 slog.Logger，供需要结构化日志的场景使用。
func (l *Logger) Slog() *slog.Logger { return l.slog }

// parseLevel 将字符串转为 slog.Level，默认 Info。
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Close 关闭日志文件（如果有）。
func (l *Logger) Close() {
	if l.writer != nil {
		l.writer.close()
	}
}

// --- customHandler: 自定义格式的 slog.Handler ---
// 输出格式: 2026-08-19 11:05:32.176 [proxy] INFO file.go:42 - message key=value ...

type customHandler struct {
	w         io.Writer
	component string
	level     slog.Level
	attrs     []slog.Attr
	groups    []string
}

func (h *customHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *customHandler) Handle(_ context.Context, r slog.Record) error {
	var buf strings.Builder

	// 时间
	buf.WriteString(r.Time.Format("2006-01-02 15:04:05.000"))

	// 组件
	if h.component != "" {
		buf.WriteString(" [")
		buf.WriteString(h.component)
		buf.WriteString("]")
	}

	// 级别
	buf.WriteString(" ")
	buf.WriteString(r.Level.String())

	// 文件:行号
	if r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		if f.File != "" {
			buf.WriteString(" ")
			buf.WriteString(filepath.Base(f.File))
			buf.WriteString(":")
			buf.WriteString(fmt.Sprintf("%d", f.Line))
		}
	}

	// 分隔符 + 消息
	buf.WriteString(" - ")
	buf.WriteString(r.Message)

	// 预设属性
	for _, a := range h.attrs {
		buf.WriteString(" ")
		buf.WriteString(a.Key)
		buf.WriteString("=")
		buf.WriteString(fmt.Sprintf("%v", a.Value.Any()))
	}

	// 本次记录的属性
	r.Attrs(func(a slog.Attr) bool {
		buf.WriteString(" ")
		buf.WriteString(a.Key)
		buf.WriteString("=")
		buf.WriteString(fmt.Sprintf("%v", a.Value.Any()))
		return true
	})

	buf.WriteString("\n")
	_, err := io.WriteString(h.w, buf.String())
	return err
}

func (h *customHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &customHandler{
		w:         h.w,
		component: h.component,
		level:     h.level,
		attrs:     append(append([]slog.Attr{}, h.attrs...), attrs...),
		groups:    h.groups,
	}
}

func (h *customHandler) WithGroup(name string) slog.Handler {
	return &customHandler{
		w:         h.w,
		component: h.component,
		level:     h.level,
		attrs:     h.attrs,
		groups:    append(append([]string{}, h.groups...), name),
	}
}

// --- dailyWriter: 按天切割的 io.Writer ---

type dailyWriter struct {
	dir string

	mu      sync.Mutex
	current *os.File
	curDate string
}

func newDailyWriter(dir string) *dailyWriter {
	os.MkdirAll(dir, 0755)
	return &dailyWriter{dir: dir}
}

func (dw *dailyWriter) Write(p []byte) (n int, err error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	today := time.Now().Format("2006-01-02")
	if dw.current == nil || dw.curDate != today {
		dw.rotate(today)
	}
	if dw.current == nil {
		return 0, fmt.Errorf("logger: no open file")
	}
	return dw.current.Write(p)
}

func (dw *dailyWriter) rotate(date string) {
	if dw.current != nil {
		dw.current.Close()
	}
	name := filepath.Join(dw.dir, date+".log")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		dw.current = nil
		return
	}
	dw.current = f
	dw.curDate = date
}

func (dw *dailyWriter) close() {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	if dw.current != nil {
		dw.current.Close()
		dw.current = nil
	}
}

// cleanLoop 每小时检查一次，删除超过 retention 天的日志文件。
func (dw *dailyWriter) cleanLoop(retentionDays int) {
	// 启动时立即清理一次
	dw.cleanOnce(retentionDays)

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		dw.cleanOnce(retentionDays)
	}
}

func (dw *dailyWriter) cleanOnce(retentionDays int) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).Format("2006-01-02")

	entries, err := os.ReadDir(dw.dir)
	if err != nil {
		return
	}

	var logFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		dateStr := strings.TrimSuffix(name, ".log")
		if len(dateStr) != 10 {
			continue
		}
		logFiles = append(logFiles, name)
	}

	sort.Strings(logFiles)
	for _, name := range logFiles {
		dateStr := strings.TrimSuffix(name, ".log")
		if dateStr < cutoff {
			os.Remove(filepath.Join(dw.dir, name))
		}
	}
}

// New 兼容旧签名（过渡用，后续可以删除）。
// Deprecated: 使用 Setup(Config{...}) 代替。
func New(dir string, stdout bool) *Logger {
	return Setup(Config{Dir: dir, Stdout: stdout})
}

// Nop 返回一个不输出任何内容的 Logger，用于测试。
func Nop() *Logger {
	h := &customHandler{w: io.Discard, level: slog.LevelDebug}
	return &Logger{slog: slog.New(h)}
}

// For 返回带有额外属性的子 Logger（供未来扩展）。
func (l *Logger) For(ctx context.Context, attrs ...slog.Attr) *Logger {
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return &Logger{slog: l.slog.With(args...), writer: l.writer}
}
