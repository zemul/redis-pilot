package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	images     []string
	containers []podman.ContainerStatus
}

var _ podman.ContainerRuntime = (*fakeRuntime)(nil)

func (f *fakeRuntime) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeRuntime) Create(engine, name, image string, port int, memory string, cpus float64, dataDir string) (string, error) {
	f.mu.Lock()
	f.images = append(f.images, image)
	f.mu.Unlock()
	f.record("Create:" + engine + ":" + name)
	return "fake-container-id", nil
}
func (f *fakeRuntime) Start(name string) error  { f.record("Start:" + name); return nil }
func (f *fakeRuntime) Stop(name string) error   { f.record("Stop:" + name); return nil }
func (f *fakeRuntime) Remove(name string) error { f.record("Remove:" + name); return nil }
func (f *fakeRuntime) Update(name, memory string, cpus float64) error {
	f.record(fmt.Sprintf("Update:%s:mem=%s:cpus=%g", name, memory, cpus))
	return nil
}
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

func (f *fakeRuntime) hasImage(image string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, got := range f.images {
		if got == image {
			return true
		}
	}
	return false
}

func newTestAgentWithFake(t *testing.T) (*Agent, *fakeRuntime) {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		Port:    8400,
		DataDir: dir,
		Log:     LogConfig{Dir: dir + "/log", Stdout: false},
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

func TestValidateInstanceName(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"my-cache", false},
		{"order_01", false},
		{"a", false},
		{strings.Repeat("a", 64), false},
		{strings.Repeat("a", 65), true}, // 超长
		{"", true},                      // 空
		{"-privileged", true},           // 以 "-" 开头，防止参数注入
		{"--privileged", true},          // 以 "-" 开头，防止参数注入
		{"../etc", true},                // 路径穿越字符
		{"foo bar", true},               // 含空格
		{"foo;rm -rf", true},            // 含 shell 元字符
	}
	for _, tc := range cases {
		err := validateInstanceName(tc.name)
		if tc.wantErr && err == nil {
			t.Errorf("validateInstanceName(%q) = nil, want error", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("validateInstanceName(%q) = %v, want nil", tc.name, err)
		}
	}
}

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
		"maxmemory 4294967296",
		"maxmemory-policy allkeys-lru",
		"appendonly no",
	}
	for _, c := range checks {
		if !strings.Contains(content, c) {
			t.Errorf("expected %q in config", c)
		}
	}
	// 实例无密码：鉴权只在 Proxy 前端，配置文件不写 requirepass/masterauth
	if strings.Contains(content, "requirepass") {
		t.Error("instance config must not contain requirepass (auth terminates at Proxy front)")
	}
	if strings.Contains(content, "masterauth") {
		t.Error("instance config must not contain masterauth")
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
	// 实例无密码：master 无 requirepass，replica 也无需 masterauth
	if strings.Contains(content, "masterauth") {
		t.Error("replica config must not contain masterauth (master has no requirepass)")
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

func TestWriteRedisConfig_Version5(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Version:         "5",
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	// Redis 5 应该用多行 save
	if !strings.Contains(content, "save 900 1") {
		t.Error("expected multi-line save for redis 5")
	}
	if strings.Contains(content, "save 3600 1 300 100 60 10000") {
		t.Error("redis 5 should not use single-line save syntax")
	}
	// Redis 5 不应有 lazyfree-lazy-user-del
	if strings.Contains(content, "lazyfree-lazy-user-del") {
		t.Error("redis 5 should not have lazyfree-lazy-user-del")
	}
	// 但应有基础 lazyfree 选项
	if !strings.Contains(content, "lazyfree-lazy-eviction") {
		t.Error("redis 5 should have lazyfree-lazy-eviction")
	}
}

func TestWriteRedisConfig_Version62(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Version:         "6.2",
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	// Redis 6.2 应该用多行 save
	if !strings.Contains(content, "save 900 1") {
		t.Error("expected multi-line save for redis 6.2")
	}
	// Redis 6.2 应有 lazyfree-lazy-user-del
	if !strings.Contains(content, "lazyfree-lazy-user-del") {
		t.Error("redis 6.2 should have lazyfree-lazy-user-del")
	}
	if !strings.Contains(content, "lazyfree-lazy-user-flush") {
		t.Error("redis 6.2 should have lazyfree-lazy-user-flush")
	}
}

func TestWriteRedisConfig_Version7(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Version:         "7",
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	// Redis 7 应该用单行 save
	if !strings.Contains(content, "save 3600 1 300 100 60 10000") {
		t.Error("expected single-line save for redis 7")
	}
	// Redis 7 应有 lazyfree-lazy-user-del
	if !strings.Contains(content, "lazyfree-lazy-user-del") {
		t.Error("redis 7 should have lazyfree-lazy-user-del")
	}
}

func TestWriteRedisConfig_DefaultVersion(t *testing.T) {
	dir := t.TempDir()
	// 不传 Version，应该默认为 7
	err := writeRedisConfig(dir, RedisConfigParams{
		Memory:          "4Gi",
		MaxmemoryPolicy: "allkeys-lru",
		Appendonly:      "no",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	if !strings.Contains(content, "save 3600 1 300 100 60 10000") {
		t.Error("default version should produce redis 7 single-line save")
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
	// 实例无密码：鉴权只在 Proxy 前端，配置文件不写 requirepass/masterauth
	if strings.Contains(content, "requirepass") {
		t.Error("kvrocks config must not contain requirepass (auth terminates at Proxy front)")
	}
	if !strings.Contains(content, "slaveof 10.0.0.2 6666") {
		t.Error("expected slaveof in config")
	}
	if strings.Contains(content, "masterauth") {
		t.Error("kvrocks replica config must not contain masterauth (master has no requirepass)")
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
	m := map[string]string{"timeout": "0", "hz": "100"}
	s, err := configOverridesString(m)
	if err != nil {
		t.Fatalf("configOverridesString: %v", err)
	}
	// 按 key 排序输出：同样的输入必须渲染出同样的文件，否则每次重写配置都是一份新内容
	if s != "hz 100\ntimeout 0\n" {
		t.Errorf("期望按 key 排序的确定性输出，实际得到 %q", s)
	}
}

// TestConfigOverridesString_RejectsInjection 验证渲染函数本身就是校验的收口点。
//
// 值里的换行会在配置文件中另起一行，而 config_overrides 渲染在模板最末尾，
// Redis 配置又是后者覆盖前者——注入的指令因此能推翻平台设定的任何一项。
func TestConfigOverridesString_RejectsInjection(t *testing.T) {
	cases := []struct {
		name      string
		overrides map[string]string
	}{
		{"换行注入 replicaof", map[string]string{"hz": "100\nreplicaof 203.0.113.1 6379"}},
		{"回车注入", map[string]string{"hz": "100\rreplicaof 203.0.113.1 6379"}},
		{"注释掉行尾", map[string]string{"hz": "100 # x"}},
		{"保留字 replicaof", map[string]string{"replicaof": "203.0.113.1 6379"}},
		{"保留字 requirepass", map[string]string{"requirepass": "hacked"}},
		{"保留字 dir", map[string]string{"dir": "/etc"}},
		{"保留字 min-replicas-to-write", map[string]string{"min-replicas-to-write": "0"}},
		{"保留字 rename-command", map[string]string{"rename-command": "FLUSHALL FLUSHALL"}},
		{"非法 key 字符", map[string]string{"hz\nreplicaof 1.2.3.4 6379": "100"}},
		{"空值", map[string]string{"hz": "  "}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := configOverridesString(c.overrides); err == nil {
				t.Errorf("应当被拒绝，实际通过了：%v", c.overrides)
			}
		})
	}
}

func TestConfigOverridesString_AcceptsLegitimateValues(t *testing.T) {
	// 带空格的值是合法的，不能因为要防注入就一并禁掉
	ok := map[string]string{
		"save":                      "3600 1 300 100",
		"maxmemory-policy":          "allkeys-lru",
		"hz":                        "100",
		"appendfsync":               "everysec",
		"auto-aof-rewrite-min-size": "64mb",
	}
	if _, err := configOverridesString(ok); err != nil {
		t.Errorf("合法配置被拒绝：%v", err)
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

func TestInstanceBackups_NotConfigured(t *testing.T) {
	a := newTestAgent(t)
	r := a.Router()
	os.MkdirAll(filepath.Join(a.cfg.DataDir, "redis-1", "backup"), 0755)
	w := doAgentRequest(r, "GET", "/instance/backups?name=redis-1", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (s3 not configured), got %d", w.Code)
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

// TestIsRedisErrReply 验证 RESP 错误回复的识别逻辑。
// 回归背景：redis-cli 收到服务端 -ERR 错误回复时，进程本身仍以退出码 0 正常结束，
// 错误只体现在输出内容里。曾经因为只检查 exec.Command 的 err 而非输出内容，
// 导致 instanceReplicate 里 Kvrocks 返回 "ERR can't replicate your own replicas"
// 时被误判为成功，旧 master 从未真正完成降级（脑裂）。
func TestIsRedisErrReply(t *testing.T) {
	cases := []struct {
		out  string
		want bool
	}{
		{"OK", false},
		{"", false},
		{"PONG", false},
		{"ERR can't replicate your own replicas", true},
		{"-ERR can't replicate your own replicas", true},
		{"ERR wrong number of arguments", true},
	}
	for _, c := range cases {
		if got := isRedisErrReply(c.out); got != c.want {
			t.Errorf("isRedisErrReply(%q) = %v, want %v", c.out, got, c.want)
		}
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
		Name:        "myinst",
		Category:    "cache",
		Engine:      "redis",
		EngineImage: "docker.io/redis:7",
		Type:        "standalone",
		Server:      "srv1",
		Port:        6379,
		Memory:      "4Gi",
		CPUs:        2,
		Password:    "pass123",
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
	// 实例无密码：鉴权只在 Proxy 前端，配置文件不写 requirepass
	if strings.Contains(content, "requirepass") {
		t.Error("instance config must not contain requirepass (auth terminates at Proxy front)")
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
	if !fake.hasImage("docker.io/redis:7") {
		t.Errorf("expected default redis image, got %v", fake.images)
	}
}

func TestInstanceCreate_Redis_WithEngineVersion(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:          "myinst",
		Category:      "cache",
		Engine:        "redis",
		EngineVersion: "6.2",
		EngineImage:   "docker.io/redis:6.2",
		Type:          "standalone",
		Server:        "srv1",
		Port:          6379,
		Memory:        "4Gi",
		CPUs:          2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !fake.hasImage("docker.io/redis:6.2") {
		t.Errorf("expected redis 6.2 image, got %v", fake.images)
	}
}

func TestInstanceCreate_MissingEngineImage(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:          "myinst",
		Category:      "cache",
		Engine:        "redis",
		EngineVersion: "6.2",
		Type:          "standalone",
		Server:        "srv1",
		Port:          6379,
		Memory:        "4Gi",
		CPUs:          2,
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstanceCreate_Kvrocks_FullFlow(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:        "kvinst",
		Category:    "persistent",
		Engine:      "kvrocks",
		EngineImage: "docker.io/apache/kvrocks:2.15.0",
		Type:        "standalone",
		Server:      "srv1",
		Port:        6666,
		Memory:      "8Gi",
		CPUs:        4,
		Password:    "kvpass",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// 验证 kvrocks 配置
	conf, _ := os.ReadFile(filepath.Join(a.cfg.DataDir, "kvinst", "conf", "kvrocks.conf"))
	if strings.Contains(string(conf), "requirepass") {
		t.Error("kvrocks config must not contain requirepass (auth terminates at Proxy front)")
	}

	if !fake.hasCalled("Create:kvrocks:kvrocks-kvinst") {
		t.Errorf("expected Create call, got %v", fake.calls)
	}
}

func TestInstanceCreate_Persistent_FullFlow(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/create", apitypes.CreateInstanceRequest{
		Name:        "persist",
		Category:    "persistent",
		Engine:      "redis",
		EngineImage: "docker.io/redis:7",
		Type:        "standalone",
		Server:      "srv1",
		Port:        6380,
		Memory:      "8Gi",
		CPUs:        4,
		Password:    "pass",
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

// CPU-only 变配：应调用 runtime.Update 且不触发 redis-cli（无 memory）。
func TestInstanceResize_CPUOnly(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "myinst", "engine": "redis", "cpus": 2.0,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !fake.hasCalled("Update:redis-myinst:mem=:cpus=2") {
		t.Errorf("expected Update call with cpus=2 and empty mem, got %v", fake.calls)
	}
}

// 既不传 memory 也不传 cpus → 400。
func TestInstanceResize_NoResourceFields(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()
	w := doAgentRequest(r, "POST", "/instance/resize", map[string]interface{}{
		"name": "myinst", "engine": "redis",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
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

// TestWriteRedisConfig_NoAuthWritten 验证实例配置在无密码方案下不写任何鉴权指令。
//
// 背景：改为"仅 Proxy 前端鉴权"后，后端实例一律无密码——鉴权只在客户端→Proxy 这一段
// 由 Proxy 校验前端密码完成。实例本身不写 requirepass/masterauth，Sentinel 与主从复制
// 都以无密码方式直连实例（依赖网络隔离作为边界）。因此即便传入 Password，模板也不应把
// 它落进配置文件。
func TestWriteRedisConfig_NoAuthWritten(t *testing.T) {
	dir := t.TempDir()
	err := writeRedisConfig(dir, RedisConfigParams{
		Password:        "pass",
		Memory:          "8Gi",
		MaxmemoryPolicy: "noeviction",
		Appendonly:      "yes",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "redis.conf"))
	content := string(data)
	if strings.Contains(content, "requirepass") {
		t.Error("instance config must not contain requirepass (auth terminates at Proxy front)")
	}
	if strings.Contains(content, "masterauth") {
		t.Error("instance config must not contain masterauth (backend is passwordless)")
	}
}

func TestWriteKvrocksConfig_NoAuthWritten(t *testing.T) {
	dir := t.TempDir()
	err := writeKvrocksConfig(dir, KvrocksConfigParams{
		Password: "kvpass",
		Memory:   "4Gi",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "conf", "kvrocks.conf"))
	content := string(data)
	if strings.Contains(content, "requirepass") {
		t.Error("kvrocks config must not contain requirepass (auth terminates at Proxy front)")
	}
	if strings.Contains(content, "masterauth") {
		t.Error("kvrocks config must not contain masterauth (backend is passwordless)")
	}
}
