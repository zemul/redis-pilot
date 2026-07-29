package agent

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
)

// TestParseReplicaOfDirective 覆盖配置文件里角色指令的各种写法。
//
// 归一化为 ip:port 是为了能直接和 Server 侧解析出的地址比较——模板写入时把 ip:port
// 的冒号换成了空格（config_template.go:157），这里要换回来。
func TestParseReplicaOfDirective(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"redis replicaof", "port 6379\nreplicaof 10.0.1.10 6379\n", "10.0.1.10:6379"},
		{"kvrocks slaveof", "port 6666\nslaveof 10.0.1.11 6666\n", "10.0.1.11:6666"},
		{"大写指令", "REPLICAOF 10.0.1.10 6379\n", "10.0.1.10:6379"},
		{"无指令即 master", "port 6379\nbind 0.0.0.0\n", ""},
		{"replicaof no one 即 master", "replicaof no one\n", ""},
		{"注释掉的指令不算", "# replicaof 10.0.1.10 6379\n", ""},
		{"前导空白", "   replicaof 10.0.1.10 6379\n", "10.0.1.10:6379"},
		{"空内容", "", ""},
		// 畸形配置原样返回，让 Server 判定为漂移并重写。这正是 instanceConfig 传实例名
		// 而非地址造成的产物：replicaof order-master 缺端口，Redis 会拒绝加载。
		{"畸形：缺端口", "replicaof order-master\n", "order-master"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseReplicaOfDirective(c.content); got != c.want {
				t.Errorf("parseReplicaOfDirective() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestReadConfiguredReplicaOf(t *testing.T) {
	dataDir := t.TempDir()

	t.Run("redis.conf", func(t *testing.T) {
		writeFile(t, filepath.Join(dataDir, "inst-a", "conf", "redis.conf"),
			"port 6379\nreplicaof 10.0.1.10 6379\n")
		got, err := readConfiguredReplicaOf(dataDir, "inst-a")
		if err != nil {
			t.Fatal(err)
		}
		if got != "10.0.1.10:6379" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("kvrocks.conf", func(t *testing.T) {
		writeFile(t, filepath.Join(dataDir, "inst-b", "conf", "kvrocks.conf"),
			"port 6666\nslaveof 10.0.1.11 6666\n")
		got, err := readConfiguredReplicaOf(dataDir, "inst-b")
		if err != nil {
			t.Fatal(err)
		}
		if got != "10.0.1.11:6666" {
			t.Errorf("got %q", got)
		}
	})

	// 配置文件不存在不是错误：实例可能刚创建还没写配置，或已被删除。
	// 报错会让 reconcile 把它当成需要介入的漂移。
	t.Run("配置文件不存在", func(t *testing.T) {
		got, err := readConfiguredReplicaOf(dataDir, "does-not-exist")
		if err != nil {
			t.Errorf("配置文件缺失不应报错，实际：%v", err)
		}
		if got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})
}

// TestInstanceConfig_RewriteOnlyDoesNotTouchContainer 是 Wave 1 的核心约束。
//
// 角色落盘的目的就是避免重启，所以这个模式绝不能碰容器。如果它顺手重启了容器，
// 就等于把"防止重启导致角色回退"变成了"每次落盘都主动触发一次重启"。
func TestInstanceConfig_RewriteOnlyDoesNotTouchContainer(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()

	w := doAgentRequest(r, "POST", "/instance/config", map[string]interface{}{
		"name":                "order-replica",
		"engine":              "redis",
		"type":                "replication",
		"category":            "persistent",
		"password":            "pw",
		"memory":              "1Gi",
		"port":                6380,
		"replica_of":          "10.0.1.10:6379",
		"rewrite_config_only": true,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}

	if fake.hasCalled("Stop:") {
		t.Error("rewrite_config_only 模式不得停止容器")
	}
	if fake.hasCalled("Start:") {
		t.Error("rewrite_config_only 模式不得启动容器")
	}

	// 配置文件应当已被写出，且角色正确
	got, err := readConfiguredReplicaOf(a.cfg.DataDir, "order-replica")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.0.1.10:6379" {
		t.Errorf("配置文件中的角色 = %q, want 10.0.1.10:6379", got)
	}
}

// TestInstanceConfig_RewriteOnlyCanPromoteToMaster 验证 replica_of 为空时写成 master。
// 这是故障转移后新 master 需要的落盘方向。
func TestInstanceConfig_RewriteOnlyCanPromoteToMaster(t *testing.T) {
	a, _ := newTestAgentWithFake(t)
	r := a.Router()

	// 先落一份 replica 配置
	doAgentRequest(r, "POST", "/instance/config", map[string]interface{}{
		"name": "order-1", "engine": "redis", "type": "replication",
		"password": "pw", "memory": "1Gi", "port": 6379,
		"replica_of": "10.0.1.10:6379", "rewrite_config_only": true,
	})
	if got, _ := readConfiguredReplicaOf(a.cfg.DataDir, "order-1"); got == "" {
		t.Fatal("前置条件失败：应先写成 replica")
	}

	// 再落盘为 master
	w := doAgentRequest(r, "POST", "/instance/config", map[string]interface{}{
		"name": "order-1", "engine": "redis", "type": "replication",
		"password": "pw", "memory": "1Gi", "port": 6379,
		"replica_of": "", "rewrite_config_only": true,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}

	got, err := readConfiguredReplicaOf(a.cfg.DataDir, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("落盘为 master 后配置文件仍有 replicaof = %q", got)
	}

	// master 必须带上 min-replicas-to-write 1，这是 replication 组 master 的写保护
	conf, err := os.ReadFile(filepath.Join(a.cfg.DataDir, "order-1", "conf", "redis.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(conf), "min-replicas-to-write 1") {
		t.Error("replication 组的 master 应当写入 min-replicas-to-write 1")
	}
}

// TestInstanceConfig_RestartStillRestarts 确认既有行为没被改坏。
func TestInstanceConfig_RestartStillRestarts(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	r := a.Router()

	w := doAgentRequest(r, "POST", "/instance/config", map[string]interface{}{
		"name": "order-1", "engine": "redis", "type": "standalone",
		"password": "pw", "memory": "1Gi", "port": 6379,
		"config_overrides": map[string]string{"maxmemory-policy": "noeviction"},
		"restart":          true,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if !fake.hasCalled("Stop:redis-order-1") {
		t.Error("restart 模式应当停止容器")
	}
	if !fake.hasCalled("Start:redis-order-1") {
		t.Error("restart 模式应当启动容器")
	}
}

// TestInstanceList_ReportsConfiguredReplicaOf 验证 Agent 上报磁盘角色，
// 这是 Server 侧 reconcile 检测漂移的输入。
func TestInstanceList_ReportsConfiguredReplicaOf(t *testing.T) {
	a, fake := newTestAgentWithFake(t)
	fake.containers = []podman.ContainerStatus{
		{Name: "redis-order-master", Running: true},
		{Name: "redis-order-replica", Running: true},
	}
	writeFile(t, filepath.Join(a.cfg.DataDir, "order-master", "conf", "redis.conf"),
		"port 6379\n")
	writeFile(t, filepath.Join(a.cfg.DataDir, "order-replica", "conf", "redis.conf"),
		"port 6380\nreplicaof 10.0.1.10 6379\n")

	w := doAgentRequest(a.Router(), "GET", "/instance/list", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	resp := parseAgentResponse(t, w)
	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected data shape: %#v", resp.Data)
	}
	configured, ok := data["configured_replica_of"].(map[string]interface{})
	if !ok {
		t.Fatalf("响应中缺少 configured_replica_of: %#v", data)
	}
	if got := configured["order-master"]; got != "" {
		t.Errorf("order-master 应当报告为 master（空串），实际 %v", got)
	}
	if got := configured["order-replica"]; got != "10.0.1.10:6379" {
		t.Errorf("order-replica = %v, want 10.0.1.10:6379", got)
	}
}
