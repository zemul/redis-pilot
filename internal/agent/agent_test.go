package agent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// fakeRuntime 记录调用但不执行真实命令
type fakeRuntime struct {
	mu         sync.Mutex
	calls      []string // 记录调用，如 "Create:redis:redis-myinst"
	containers []podman.ContainerStatus
}

var _ podman.ContainerRuntime = (*fakeRuntime)(nil)

func (f *fakeRuntime) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeRuntime) Create(engine, name string, port int, memory string, cpus int, dataDir string) (string, error) {
	f.record("Create:" + engine + ":" + name)
	return "fake-container-id", nil
}
func (f *fakeRuntime) Start(name string) error  { f.record("Start:" + name); return nil }
func (f *fakeRuntime) Stop(name string) error   { f.record("Stop:" + name); return nil }
func (f *fakeRuntime) Remove(name string) error { f.record("Remove:" + name); return nil }
func (f *fakeRuntime) Run(args ...string) (string, error) {
	f.record("Run:" + strings.Join(args, " "))
	return "", nil
}
func (f *fakeRuntime) ListAll() ([]podman.ContainerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.containers, nil
}

func (f *fakeRuntime) hasCalled(prefix string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func newTestAgentWithFake(t *testing.T) (*Agent, *fakeRuntime) {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:        8400,
		DataDir:     dir,
		SentinelDir: filepath.Join(dir, "sentinel"),
		Log:         LogConfig{Dir: dir + "/log", Stdout: false},
	}
	a := New(cfg)
	fake := &fakeRuntime{}
	a.runtime = fake
	return a, fake
}

func newTestAgent(t *testing.T) *Agent {
	a, _ := newTestAgentWithFake(t)
	return a
}

func doAgentRequest(router http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func parseAgentResponse(t *testing.T, w *httptest.ResponseRecorder) apitypes.APIResponse {
	t.Helper()
	var resp apitypes.APIResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v, body: %s", err, w.Body.String())
	}
	return resp
}

// ---------- 配置模板渲染测试 ----------

func TestWriteRedisConfig_Cache(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Password:        "mypass",
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	checks := []string{
		"requirepass mypass",
		"maxmemory 4294967296",
		"maxmemory-policy allkeys-lru",
		"appendonly no",
	}
	for _, c := range checks {
		if !strings.Contains(content, c) {
			t.Errorf("expected %q in config", c)
		}
	}
	if strings.Contains(content, "replicaof") {
		t.Error("cache config should not contain replicaof")
	}
}

func TestWriteRedisConfig_Persistent(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Password:        "pass",
		Memory:          "8Gi",
		MaxmemoryPolicy: "noeviction",
		Appendonly:      "yes",
		ReplicaOf:       "10.0.0.1 6379",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	if !strings.Contains(content, "replicaof 10.0.0.1 6379") {
		t.Error("expected replicaof in config")
	}
	if !strings.Contains(content, "appendonly yes") {
		t.Error("expected appendonly yes")
	}
}

func TestWriteRedisConfig_WithOverrides(t *testing.T) {
	dir := t.TempDir()
	writeRedisConfig(dir, RedisConfigParams{
		Password:        "pass",
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
		ConfigOverrides: "hz 100\ntcp-backlog 511\n",
	})
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	if !strings.Contains(content, "hz 100") || !strings.Contains(content, "tcp-backlog 511") {
		t.Error("expected config overrides in output")
	}
}

func TestWriteKvrocksConfig(t *testing.T) {
	dir := t.TempDir()
	err := writeKvrocksConfig(dir, KvrocksConfigParams{
		Password:  "kvpass",
		Memory:    "4Gi",
		ReplicaOf: "10.0.0.2 6666",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "kvrocks.conf"))
	content := string(data)
	if !strings.Contains(content, "requirepass kvpass") {
		t.Error("expected requirepass")
	}
	if !strings.Contains(content, "max-db-size 4294967296") {
		t.Error("expected max-db-size")
	}
	if !strings.Contains(content, "slaveof 10.0.0.2 6666") {
		t.Error("expected slaveof")
	}
	if !strings.Contains(content, "rocksdb.compression snappy") {
		t.Error("expected rocksdb defaults")
	}
}

// ---------- 工具函数测试 ----------

func TestTrimPrefix(t *testing.T) {
	cases := []struct{ in, want string }{
		{"redis-myinst", "myinst"},
		{"kvrocks-myinst", "myinst"},
		{"other-myinst", "other-myinst"},
	}
	for _, tc := range cases {
		got := trimPrefix(tc.in)
		if got != tc.want {
			t.Errorf("trimPrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCleanupBackups(t *testing.T) {
	dir := t.TempDir()
	// 创建 5 个备份文件
	for _, name := range []string{"2024-01-01.rdb", "2024-01-02.rdb", "2024-01-03.rdb", "2024-01-04.rdb", "2024-01-05.rdb"} {
		os.WriteFile(filepath.Join(dir, name), []byte("data"), 0644)
	}
	// 保留 3 份
	cleanupBackups(dir, 3)
	entries, _ := os.ReadDir(dir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 backups, got %d", len(entries))
	}
	// 应保留最新的 3 个
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	for _, want := range []string{"2024-01-03.rdb", "2024-01-04.rdb", "2024-01-05.rdb"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected %s to be retained, got %v", want, names)
		}
	}
}

func TestCleanupBackups_SkipCheckpoint(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "checkpoint"), 0755)
	os.WriteFile(filepath.Join(dir, "2024-01-01.rdb"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(dir, "2024-01-02.rdb"), []byte("data"), 0644)
	cleanupBackups(dir, 1)
	// checkpoint 目录不应被计入也不应被删除
	if _, err := os.Stat(filepath.Join(dir, "checkpoint")); os.IsNotExist(err) {
		t.Error("checkpoint dir should not be deleted")
	}
	entries, _ := os.ReadDir(dir)
	// checkpoint + 1 个备份文件
	fileCount := 0
	for _, e := range entries {
		if e.Name() != "checkpoint" {
			fileCount++
		}
	}
	if fileCount != 1 {
		t.Fatalf("expected 1 backup file, got %d", fileCount)
	}
}

// ---------- Sentinel 管理测试 ----------

func TestSentinelSync_WritesConfigForRunningContainer(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	fake.containers = []podman.ContainerStatus{{Name: "redis-sentinel", Running: true}}
	r := a.Router()
	w := doAgentRequest(r, "POST", "/sentinel/sync", map[string]interface{}{
		"port":   26379,
		"quorum": 2,
		"masters": []map[string]interface{}{
			{
				"group":                   "order-master",
				"host":                    "10.0.1.10",
				"port":                    6379,
				"password":                "secret",
				"down_after_milliseconds": 5000,
				"failover_timeout":        30000,
				"parallel_syncs":          1,
			},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	data, err := os.ReadFile(filepath.Join(a.cfg.SentinelDir, "conf", "sentinel.conf"))
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	for _, want := range []string{
		"port 26379",
		"sentinel monitor order-master 10.0.1.10 6379 2",
		"sentinel auth-pass order-master secret",
		"sentinel down-after-milliseconds order-master 5000",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("expected %q in sentinel.conf", want)
		}
	}
	if fake.hasCalled("Run:run") || fake.hasCalled("Run:rm -f redis-sentinel") {
		t.Fatalf("sentinel sync must not create or recreate containers, calls=%v", fake.calls)
	}
}

func TestSentinelSync_RequiresRunningContainer(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	w := doAgentRequest(a.Router(), "POST", "/sentinel/sync", map[string]interface{}{
		"port":    26379,
		"quorum":  2,
		"masters": []map[string]interface{}{},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 when sentinel is not running, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSentinelRemoveMaster_RemovesConfigLines(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	if _, err := a.writeSentinelConfig(apitypes.SentinelSyncRequest{
		Port:   26379,
		Quorum: 2,
		Masters: []apitypes.SentinelMaster{
			{Group: "order-master", Host: "10.0.1.10", Port: 6379, Password: "secret", DownAfterMilliseconds: 5000, FailoverTimeout: 30000, ParallelSyncs: 1},
			{Group: "user-master", Host: "10.0.1.11", Port: 6379, DownAfterMilliseconds: 5000, FailoverTimeout: 30000, ParallelSyncs: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	r := a.Router()
	w := doAgentRequest(r, "POST", "/sentinel/remove-master", map[string]interface{}{"group": "order-master"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	data, _ := os.ReadFile(filepath.Join(a.cfg.SentinelDir, "conf", "sentinel.conf"))
	content := string(data)
	if strings.Contains(content, "order-master") {
		t.Fatalf("order-master should be removed from config:\n%s", content)
	}
	if !strings.Contains(content, "user-master") {
		t.Fatalf("user-master should remain in config:\n%s", content)
	}
	if !fake.hasCalled("Run:exec redis-sentinel redis-cli -p 26379 SENTINEL REMOVE order-master") {
		t.Fatalf("expected SENTINEL REMOVE call, calls=%v", fake.calls)
	}
}

func TestSentinelStatus(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	fake.containers = []podman.ContainerStatus{{Name: "redis-sentinel", Running: true}}
	if _, err := a.writeSentinelConfig(apitypes.SentinelSyncRequest{
		Port:   26379,
		Quorum: 2,
		Masters: []apitypes.SentinelMaster{
			{Group: "order-master", Host: "10.0.1.10", Port: 6379, DownAfterMilliseconds: 5000, FailoverTimeout: 30000, ParallelSyncs: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	w := doAgentRequest(a.Router(), "GET", "/sentinel/status", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	resp := parseAgentResponse(t, w)
	data, _ := json.Marshal(resp.Data)
	var status apitypes.SentinelStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	if !status.Running {
		t.Fatal("expected running sentinel")
	}
	if len(status.Masters) != 1 || status.Masters[0] != "order-master" {
		t.Fatalf("unexpected masters: %#v", status.Masters)
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	os.WriteFile(src, []byte("hello"), 0644)
	if err := copyFile(src, dst); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dst)
	if string(data) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(data))
	}
}

func TestConfigOverridesString(t *testing.T) {
	m := map[string]string{"hz": "100", "timeout": "0"}
	s := configOverridesString(m)
	if !strings.Contains(s, "hz 100") || !strings.Contains(s, "timeout 0") {
		t.Errorf("unexpected output: %s", s)
	}
}

// ---------- Handler 测试（不依赖外部命令） ----------

func TestHostHealth(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "GET", "/host/health", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := parseAgentResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success: %s", resp.Error)
	}
}

func TestHostResources(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "GET", "/host/resources", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestInstanceBackups_Empty(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	// 创建备份目录
	os.MkdirAll(filepath.Join(a.cfg.DataDir, "redis-1", "backup"), 0755)
	w := doAgentRequest(r, "GET", "/instance/backups?name=redis-1", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestInstanceBackups_MissingName(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "GET", "/instance/backups", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceBackups_WithFiles(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	backupDir := filepath.Join(a.cfg.DataDir, "redis-1", "backup")
	os.MkdirAll(backupDir, 0755)
	os.WriteFile(filepath.Join(backupDir, "2024-01-01T00:00:00.rdb"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(backupDir, "2024-01-02T00:00:00.rdb"), []byte("data"), 0644)

	w := doAgentRequest(r, "GET", "/instance/backups?name=redis-1", nil)
	resp := parseAgentResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success: %s", resp.Error)
	}
	data, _ := json.Marshal(resp.Data)
	var result struct {
		Backups []string `json:"backups"`
	}
	json.Unmarshal(data, &result)
	if len(result.Backups) != 2 {
		t.Fatalf("expected 2 backups, got %d", len(result.Backups))
	}
}

func TestInstanceStatus_MissingName(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "GET", "/instance/status", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// ---------- Handler 参数校验测试 ----------

func TestInstanceCreate_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	// 缺少必填字段
	w := doAgentRequest(r, "POST", "/instance/create", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceStart_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/start", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceStop_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/stop", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceDelete_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/delete", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceConfig_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/config", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstancePromote_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/promote", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceReplicate_BadRequest(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/replicate", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestInstanceReplicate_InvalidFormat(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/replicate", map[string]string{
		"name":       "redis-1",
		"replica_of": "invalid-no-port",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid replica_of format, got %d", w.Code)
	}
}

func TestAgentAuth_Unauthorized(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Port:    8400,
		Token:   "secret",
		DataDir: dir,
		Log:     LogConfig{Dir: dir + "/log", Stdout: false},
	}
	a := New(cfg)
	a.runtime = &fakeRuntime{}
	r := a.Router()
	w := doAgentRequest(r, "GET", "/host/health", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

// ---------- 完整流程测试（使用 fakeRuntime） ----------

func TestInstanceCreate_Redis_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "myinst",
		Category: "cache",
		Engine:   "redis",
		Type:     "standalone",
		Server:   "srv1",
		Port:     6379,
		Memory:   "4Gi",
		CPUs:     2,
		Password: "pass123",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证目录创建
	for _, sub := range []string{"conf", "data", "backup"} {
		dir := filepath.Join(a.cfg.DataDir, "myinst", sub)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("expected dir %s to exist", dir)
		}
	}

	// 验证配置文件
	conf, _ := os.ReadFile(filepath.Join(a.cfg.DataDir, "myinst", "conf", "redis.conf"))
	content := string(conf)
	if !strings.Contains(content, "requirepass pass123") {
		t.Error("expected password in config")
	}
	if !strings.Contains(content, "maxmemory-policy allkeys-lru") {
		t.Error("cache should use allkeys-lru")
	}
	if !strings.Contains(content, "appendonly no") {
		t.Error("cache should disable AOF")
	}

	// 验证 runtime 调用
	if !fake.hasCalled("Create:redis:redis-myinst") {
		t.Errorf("expected Create call, got %v", fake.calls)
	}
}

func TestInstanceCreate_Kvrocks_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "kvinst",
		Category: "persistent",
		Engine:   "kvrocks",
		Type:     "standalone",
		Server:   "srv1",
		Port:     6666,
		Memory:   "8Gi",
		CPUs:     4,
		Password: "kvpass",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证 kvrocks 配置
	conf, _ := os.ReadFile(filepath.Join(a.cfg.DataDir, "kvinst", "conf", "kvrocks.conf"))
	if !strings.Contains(string(conf), "requirepass kvpass") {
		t.Error("expected password in kvrocks config")
	}

	if !fake.hasCalled("Create:kvrocks:kvrocks-kvinst") {
		t.Errorf("expected Create call, got %v", fake.calls)
	}
}

func TestInstanceCreate_Persistent_FullFlow(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:     "persist",
		Category: "persistent",
		Engine:   "redis",
		Type:     "standalone",
		Server:   "srv1",
		Port:     6380,
		Memory:   "8Gi",
		CPUs:     4,
		Password: "pass",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	conf, _ := os.ReadFile(filepath.Join(a.cfg.DataDir, "persist", "conf", "redis.conf"))
	content := string(conf)
	if !strings.Contains(content, "maxmemory-policy noeviction") {
		t.Error("persistent should use noeviction")
	}
	if !strings.Contains(content, "appendonly yes") {
		t.Error("persistent should enable AOF")
	}
}

func TestInstanceStart_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/start", map[string]string{
		"name": "myinst", "engine": "redis",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !fake.hasCalled("Start:redis-myinst") {
		t.Errorf("expected Start call, got %v", fake.calls)
	}
}

func TestInstanceStop_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/stop", map[string]string{
		"name": "myinst", "engine": "redis",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !fake.hasCalled("Stop:redis-myinst") {
		t.Errorf("expected Stop call, got %v", fake.calls)
	}
}

func TestInstanceDelete_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()

	// 创建数据目录
	dataDir := filepath.Join(a.cfg.DataDir, "myinst")
	os.MkdirAll(filepath.Join(dataDir, "data"), 0755)

	w := doAgentRequest(r, "POST", "/instance/delete", map[string]interface{}{
		"name": "myinst", "engine": "redis", "clean_data": true,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !fake.hasCalled("Remove:redis-myinst") {
		t.Errorf("expected Remove call, got %v", fake.calls)
	}
	// clean_data=true 应删除数据目录
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Error("expected data dir to be removed")
	}
}

func TestInstanceDelete_KeepData(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()

	dataDir := filepath.Join(a.cfg.DataDir, "myinst", "data")
	os.MkdirAll(dataDir, 0755)

	doAgentRequest(r, "POST", "/instance/delete", map[string]interface{}{
		"name": "myinst", "engine": "redis", "clean_data": false,
	})
	// clean_data=false 应保留数据目录
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Error("expected data dir to be kept")
	}
}

func TestInstanceList_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	fake.containers = []podman.ContainerStatus{
		{Name: "redis-myinst", Running: true},
		{Name: "kvrocks-other", Running: false},
	}
	r := a.Router()
	w := doAgentRequest(r, "GET", "/instance/list", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := parseAgentResponse(t, w)
	if !resp.Success {
		t.Fatalf("expected success: %s", resp.Error)
	}
}
