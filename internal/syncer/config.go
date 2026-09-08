// Package syncer hosts bounded isolated synchronization tasks in a standalone process.
// Optional control-plane reporting observes local tasks, not production ownership.
package syncer

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const (
	maxConfigBytes             = 64 << 10
	maxTasks                   = 8
	maxTotalJournalBytes int64 = 1 << 30
)

// TaskConfig owns an exclusive source, cold database and durable journal. IDs
// are stable operational identifiers, not discovered Redis node names.
type TaskConfig struct {
	TaskID               string `yaml:"task_id"`
	ConfirmExclusiveCold bool   `yaml:"confirm_exclusive_cold"`
	SourceAddress        string `yaml:"source_address"`
	SourcePasswordFile   string `yaml:"source_password_file"`
	ColdAddress          string `yaml:"cold_address"`
	ColdPasswordFile     string `yaml:"cold_password_file"`
	JournalDir           string `yaml:"journal_dir"`
	AllowUnauthenticated bool   `yaml:"allow_unauthenticated"`
	SegmentBytes         int64  `yaml:"segment_bytes"`
	JournalBytes         int64  `yaml:"journal_bytes"`
}

// DefaultTaskConfig supplies per-task budgets without granting backend access.
func DefaultTaskConfig() TaskConfig {
	return TaskConfig{SegmentBytes: 4 << 20, JournalBytes: 128 << 20}
}

// Config deliberately has no cluster discovery, task assignment, or inline secrets.
type Config struct {
	Mode                 string             `yaml:"mode"`
	ConfirmExclusiveCold bool               `yaml:"confirm_exclusive_cold"`
	SourceAddress        string             `yaml:"source_address"`
	SourcePasswordFile   string             `yaml:"source_password_file"`
	ColdAddress          string             `yaml:"cold_address"`
	ColdPasswordFile     string             `yaml:"cold_password_file"`
	JournalDir           string             `yaml:"journal_dir"`
	AllowUnauthenticated bool               `yaml:"allow_unauthenticated"`
	IOTimeout            time.Duration      `yaml:"io_timeout"`
	ShutdownTimeout      time.Duration      `yaml:"shutdown_timeout"`
	SegmentBytes         int64              `yaml:"segment_bytes"`
	JournalBytes         int64              `yaml:"journal_bytes"`
	AdminListen          string             `yaml:"admin_listen"`
	Tasks                []TaskConfig       `yaml:"tasks"`
	ControlPlane         ControlPlaneConfig `yaml:"control_plane"`
	Progress             ProgressConfig     `yaml:"progress"`
	BuildVersion         string             `yaml:"-"`
}

// DefaultConfig is intentionally not runnable without explicit isolated backends,
// durable storage, credentials (or an unauthenticated opt-in), and cold DB consent.
func DefaultConfig() Config {
	return Config{
		Mode: "isolated", IOTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second,
		SegmentBytes: 4 << 20, JournalBytes: 128 << 20, AdminListen: "127.0.0.1:19300",
		BuildVersion: "dev",
		ControlPlane: ControlPlaneConfig{HeartbeatInterval: 10 * time.Second, RequestTimeout: 3 * time.Second},
		Progress:     ProgressConfig{Interval: 5 * time.Second, Timeout: time.Second},
	}
}

func LoadConfig(name string) (Config, error) {
	// A mistaken FIFO/device path must not hang -check-config. Inspect the opened
	// descriptor rather than a pre-open stat, and do not follow a final symlink.
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return Config{}, errors.New("syncer config cannot be opened")
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return Config{}, errors.New("syncer config must be a regular file")
	}
	return DecodeConfig(f)
}

// DecodeConfig rejects unknown fields and extra documents. Decoder details must
// not escape: malformed YAML may contain credentials or other sensitive values.
func DecodeConfig(r io.Reader) (Config, error) {
	if r == nil {
		return Config{}, errors.New("syncer config cannot be read")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxConfigBytes+1))
	if err != nil {
		return Config{}, errors.New("syncer config cannot be read")
	}
	if len(data) > maxConfigBytes {
		return Config{}, errors.New("syncer config exceeds 64KiB")
	}
	c := DefaultConfig()
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return Config{}, errors.New("syncer config contains invalid YAML or unsupported fields")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("syncer config requires exactly one YAML document")
	}
	// Inspect field presence separately so explicit zero/false legacy values do
	// not silently disappear when tasks is selected. The typed strict decode
	// above remains responsible for unknown fields and duplicate keys.
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) != 1 {
		return Config{}, errors.New("syncer config contains invalid YAML")
	}
	if hasYAMLAliasOrMerge(&document) {
		return Config{}, errors.New("syncer config does not allow YAML aliases or merges")
	}
	fields := mappingFields(document.Content[0])
	if taskNode, present := fields["tasks"]; present {
		for _, key := range legacyConfigFields {
			if _, mixed := fields[key]; mixed {
				return Config{}, errors.New("syncer tasks cannot be mixed with legacy task fields")
			}
		}
		if taskNode.Kind != yaml.SequenceNode || len(c.Tasks) == 0 {
			return Config{}, errors.New("syncer tasks requires one to eight tasks")
		}
		for i, node := range taskNode.Content {
			taskFields := mappingFields(node)
			if _, present := taskFields["segment_bytes"]; !present {
				c.Tasks[i].SegmentBytes = DefaultTaskConfig().SegmentBytes
			}
			if _, present := taskFields["journal_bytes"]; !present {
				c.Tasks[i].JournalBytes = DefaultTaskConfig().JournalBytes
			}
		}
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

var legacyConfigFields = []string{"confirm_exclusive_cold", "source_address", "source_password_file", "cold_address", "cold_password_file", "journal_dir", "allow_unauthenticated", "segment_bytes", "journal_bytes"}

func mappingFields(node *yaml.Node) map[string]*yaml.Node {
	fields := make(map[string]*yaml.Node)
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			fields[node.Content[i].Value] = node.Content[i+1]
		}
	}
	return fields
}

func hasYAMLAliasOrMerge(node *yaml.Node) bool {
	if node.Kind == yaml.AliasNode || node.Tag == "!!merge" {
		return true
	}
	for _, child := range node.Content {
		if hasYAMLAliasOrMerge(child) {
			return true
		}
	}
	return false
}

// Validate performs only read-only path inspection, never creating directories,
// opening credentials, binding listeners, or contacting backends.
func (c Config) Validate() error {
	if err := c.validateProgress(); err != nil {
		return err
	}
	if err := c.validateControlPlane(); err != nil {
		return err
	}
	if c.Mode != "isolated" {
		return errors.New("syncer mode must be isolated")
	}
	admin, ok := loopbackEndpoint(c.AdminListen, true)
	if !ok {
		return errors.New("syncer admin_listen requires a literal loopback IP and valid port")
	}
	if c.Tasks != nil {
		if len(c.Tasks) == 0 || len(c.Tasks) > maxTasks {
			return errors.New("syncer tasks requires one to eight tasks")
		}
		defaults := DefaultConfig()
		if c.ConfirmExclusiveCold || c.SourceAddress != "" || c.SourcePasswordFile != "" || c.ColdAddress != "" || c.ColdPasswordFile != "" || c.JournalDir != "" || c.AllowUnauthenticated || (c.SegmentBytes != 0 && c.SegmentBytes != defaults.SegmentBytes) || (c.JournalBytes != 0 && c.JournalBytes != defaults.JournalBytes) {
			return errors.New("syncer tasks cannot be mixed with legacy task fields")
		}
	}
	ids := make(map[string]bool)
	endpoints := make(map[netip.AddrPort]bool)
	var paths []string
	var total int64
	for _, task := range c.taskConfigs() {
		if c.Tasks != nil {
			if gateway.ValidateIsolatedSyncTaskID(task.TaskID) != nil || ids[task.TaskID] {
				return errors.New("syncer task IDs must be unique safe identifiers")
			}
			ids[task.TaskID] = true
		}
		if err := c.validateTask(task, admin); err != nil {
			return err
		}
		for _, address := range []string{task.SourceAddress, task.ColdAddress} {
			endpoint, _ := loopbackEndpoint(address, false)
			if endpoints[endpoint] {
				return errors.New("syncer all source and cold endpoints must be distinct")
			}
			endpoints[endpoint] = true
		}
		canonical, err := canonicalJournalPath(task.JournalDir)
		if err != nil {
			return err
		}
		if filepath.Dir(canonical) == canonical {
			return errors.New("syncer journal must not resolve to filesystem root")
		}
		for _, other := range paths {
			if journalPathsConflict(canonical, other) {
				return errors.New("syncer journal directories must not overlap or alias")
			}
		}
		paths = append(paths, canonical)
		if task.JournalBytes > maxTotalJournalBytes-total {
			return errors.New("syncer total declared journal capacity exceeds 1GiB")
		}
		total += task.JournalBytes
	}
	return nil
}

func (c Config) validateTask(task TaskConfig, admin netip.AddrPort) error {
	if !task.ConfirmExclusiveCold {
		return errors.New("syncer requires confirm_exclusive_cold for a dedicated cold database")
	}
	source, ok := loopbackEndpoint(task.SourceAddress, false)
	if !ok {
		return errors.New("syncer source_address requires a literal loopback IP and nonzero port")
	}
	cold, ok := loopbackEndpoint(task.ColdAddress, false)
	if !ok {
		return errors.New("syncer cold_address requires a literal loopback IP and nonzero port")
	}
	if source == cold {
		return errors.New("syncer source and cold endpoints must differ")
	}
	if admin == source || admin == cold {
		return errors.New("syncer admin listener must differ from both backend endpoints")
	}
	if !filepath.IsAbs(task.JournalDir) || filepath.Clean(task.JournalDir) != task.JournalDir || task.JournalDir == string(filepath.Separator) {
		return errors.New("syncer journal_dir must be a clean absolute non-root path")
	}
	for _, name := range []string{task.SourcePasswordFile, task.ColdPasswordFile} {
		if name != "" && !filepath.IsAbs(name) {
			return errors.New("syncer password file paths must be absolute")
		}
		if name == "" && !task.AllowUnauthenticated {
			return errors.New("syncer missing password files require explicit allow_unauthenticated")
		}
	}
	if err := gateway.ValidateIsolatedSyncOptions(task.options(c)); err != nil {
		return errors.New("syncer isolated task options are invalid")
	}
	return nil
}

func (c Config) taskConfigs() []TaskConfig {
	if c.Tasks != nil {
		return c.Tasks
	}
	return []TaskConfig{{ConfirmExclusiveCold: c.ConfirmExclusiveCold,
		SourceAddress: c.SourceAddress, SourcePasswordFile: c.SourcePasswordFile,
		ColdAddress: c.ColdAddress, ColdPasswordFile: c.ColdPasswordFile,
		JournalDir: c.JournalDir, AllowUnauthenticated: c.AllowUnauthenticated,
		SegmentBytes: c.SegmentBytes, JournalBytes: c.JournalBytes}}
}

func (c Config) taskOptions() gateway.IsolatedSyncOptions {
	return c.taskConfigs()[0].options(c)
}

func (t TaskConfig) options(c Config) gateway.IsolatedSyncOptions {
	return gateway.IsolatedSyncOptions{
		TaskID:        t.TaskID,
		SourceAddress: t.SourceAddress, SourcePasswordFile: t.SourcePasswordFile,
		ColdAddress: t.ColdAddress, ColdPasswordFile: t.ColdPasswordFile,
		JournalDir: t.JournalDir, AllowUnauthenticated: t.AllowUnauthenticated,
		IOTimeout: c.IOTimeout, ShutdownTimeout: c.ShutdownTimeout,
		SegmentBytes: t.SegmentBytes, JournalBytes: t.JournalBytes,
	}
}

// Resolve existing ancestors without creating the journal. This handles host
// aliases (including /var on macOS) and rejects dangling links/special files.
func canonicalJournalPath(name string) (string, error) {
	current := name
	var suffix []string
	for {
		info, err := os.Lstat(current)
		if err == nil {
			resolved, resolveErr := filepath.EvalSymlinks(current)
			if resolveErr != nil {
				return "", errors.New("syncer journal ancestor cannot be resolved")
			}
			if info.Mode()&os.ModeSymlink != 0 {
				info, err = os.Stat(resolved)
			}
			if err != nil || !info.IsDir() {
				return "", errors.New("syncer journal ancestor must be a directory")
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("syncer journal ancestor cannot be inspected")
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("syncer journal ancestor cannot be resolved")
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func pathContains(parent, child string) bool {
	return parent == child || strings.HasPrefix(child, parent+string(filepath.Separator))
}

func journalPathsConflict(first, second string) bool {
	if pathContains(first, second) || pathContains(second, first) {
		return true
	}
	firstInfo, firstErr := os.Stat(first)
	secondInfo, secondErr := os.Stat(second)
	return firstErr == nil && secondErr == nil && os.SameFile(firstInfo, secondInfo)
}

// Canonicalize equivalent IP spellings and ports before checking collisions.
func loopbackEndpoint(address string, allowZero bool) (netip.AddrPort, bool) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return netip.AddrPort{}, false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || !ip.IsLoopback() {
		return netip.AddrPort{}, false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 || (!allowZero && n == 0) {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(n)), true
}
