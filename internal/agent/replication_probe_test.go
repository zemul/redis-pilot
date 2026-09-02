package agent

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
	"time"
)

// bulkReply 把一段文本包成 RESP 批量字符串回复。
func bulkReply(payload string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
}

// ---------------------------------------------------------------------------
// INFO replication 解析
// ---------------------------------------------------------------------------

func TestParseReplicationInfo(t *testing.T) {
	t.Run("master", func(t *testing.T) {
		st, err := parseReplicationInfo("# Replication\r\nrole:master\r\nconnected_slaves:1\r\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st.Role != "master" {
			t.Errorf("Role = %q, want master", st.Role)
		}
		if st.ReplicaOf != "" {
			t.Errorf("master 的 ReplicaOf 必须是空串，实际 %q", st.ReplicaOf)
		}
	})

	t.Run("replica 带 master 地址", func(t *testing.T) {
		st, err := parseReplicationInfo(
			"# Replication\r\nrole:slave\r\nmaster_host:10.0.1.11\r\nmaster_port:6380\r\nmaster_link_status:up\r\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st.ReplicaOf != "10.0.1.11:6380" {
			t.Errorf("ReplicaOf = %q, want 10.0.1.11:6380", st.ReplicaOf)
		}
	})

	t.Run("复制链路断开时仍报告配置的目标", func(t *testing.T) {
		// 链路断了不等于没有复制目标。这里必须照实上报，否则 Server 会以为它是
		// master，转而走"不提升、只告警"的分支，看不到它其实挂错了 master。
		st, err := parseReplicationInfo(
			"role:slave\r\nmaster_host:10.0.1.99\r\nmaster_port:6379\r\nmaster_link_status:down\r\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st.ReplicaOf != "10.0.1.99:6379" {
			t.Errorf("ReplicaOf = %q, want 10.0.1.99:6379", st.ReplicaOf)
		}
	})

	t.Run("没有 role 字段时报错而不是给出空结论", func(t *testing.T) {
		// 这个结果会上报给 Server 当作下发 REPLICAOF 的判据，"没读到"和"是 master"
		// 必须区分开——混同的方向是对一个正常的 replica 动手。
		if _, err := parseReplicationInfo("# Replication\r\nconnected_slaves:0\r\n"); err == nil {
			t.Fatal("缺 role 字段应当返回错误")
		}
	})

	t.Run("role 是 slave 但缺 master 地址时报错", func(t *testing.T) {
		if _, err := parseReplicationInfo("role:slave\r\nmaster_host:10.0.1.11\r\n"); err == nil {
			t.Fatal("缺 master_port 应当返回错误")
		}
	})
}

// ---------------------------------------------------------------------------
// RESP 批量字符串读取
// ---------------------------------------------------------------------------

func TestReadRESPBulkString(t *testing.T) {
	t.Run("正常读取", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader(bulkReply("role:master\r\n")))
		got, err := readRESPBulkString(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "role:master\r\n" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("空 payload", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader(bulkReply("")))
		got, err := readRESPBulkString(r)
		if err != nil || got != "" {
			t.Errorf("got %q, err %v", got, err)
		}
	})

	t.Run("nil 回复", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("$-1\r\n"))
		if _, err := readRESPBulkString(r); err == nil {
			t.Fatal("$-1 应当返回错误")
		}
	})

	t.Run("非批量字符串回复", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("+OK\r\n"))
		if _, err := readRESPBulkString(r); err == nil {
			t.Fatal("+OK 不是批量字符串，应当返回错误")
		}
	})

	t.Run("超长声明被拒绝而不是照着分配内存", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("$999999999\r\n"))
		if _, err := readRESPBulkString(r); err == nil {
			t.Fatal("超过上限的长度声明应当返回错误")
		}
	})

	t.Run("长度不是数字", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("$abc\r\n"))
		if _, err := readRESPBulkString(r); err == nil {
			t.Fatal("非法长度应当返回错误")
		}
	})
}

// ---------------------------------------------------------------------------
// probeInstance：一条连接上同时拿到健康分类和运行时复制关系
// ---------------------------------------------------------------------------

func TestProbeInstance_ReturnsRuntimeReplicationState(t *testing.T) {
	f := newFakeRedis(t, "+OK\r\n", "+PONG\r\n")
	f.infoReply = bulkReply("role:slave\r\nmaster_host:10.0.1.11\r\nmaster_port:6380\r\n")

	health, repl, err := probeInstance(f.addr(), "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != healthOK {
		t.Errorf("health = %q, want healthy", health)
	}
	if repl == nil {
		t.Fatal("应当返回运行时复制关系")
	}
	if repl.ReplicaOf != "10.0.1.11:6380" {
		t.Errorf("ReplicaOf = %q", repl.ReplicaOf)
	}
}

// TestProbeInstance_InfoFailureDoesNotAffectHealth 验证拿不到 INFO 不会误报实例故障。
// 复制关系是顺路取回的观测信息，让一个纯观测功能去否定 PING 的结论是本末倒置。
func TestProbeInstance_InfoFailureDoesNotAffectHealth(t *testing.T) {
	// 不设 infoReply，fakeRedis 对 INFO 回 +OK（不是批量字符串）
	f := newFakeRedis(t, "+OK\r\n", "+PONG\r\n")

	health, repl, err := probeInstance(f.addr(), "", nil)
	if err != nil {
		t.Fatalf("INFO 失败不应变成 probe 错误: %v", err)
	}
	if health != healthOK {
		t.Errorf("health = %q, want healthy", health)
	}
	if repl != nil {
		t.Errorf("读不到复制关系时必须返回 nil 让调用方跳过，实际 %+v", repl)
	}
}

// TestProbeInstance_AuthThenPingThenInfoOnOneConnection 验证一条连接上连发三条命令
// 不会因为缓冲区被丢弃而错位。
//
// 此前每读一条回复都新建 bufio.NewReader(conn)，已经读进缓冲区、但属于下一条回复的
// 字节会连同 reader 一起被丢掉。之所以一直没出问题，是因为每条命令都等回复读完才发
// 下一条，缓冲区恰好只有一行——那是时序上的侥幸，而 INFO 的回复跨多行。
func TestProbeInstance_AuthThenPingThenInfoOnOneConnection(t *testing.T) {
	f := newFakeRedis(t, "+OK\r\n", "+PONG\r\n")
	f.infoReply = bulkReply("# Replication\r\nrole:master\r\nconnected_slaves:2\r\nslave0:ip=1.2.3.4\r\n")

	health, repl, err := probeInstance(f.addr(), "secret", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != healthOK {
		t.Errorf("health = %q", health)
	}
	if repl == nil || repl.Role != "master" || repl.ReplicaOf != "" {
		t.Errorf("repl = %+v, want role=master ReplicaOf=\"\"", repl)
	}
}

// ---------------------------------------------------------------------------
// monitor 的复制观测缓存
// ---------------------------------------------------------------------------

// TestMonitor_ReplicaOfMapSkipsStaleObservations 验证过期观测不会上报给 Server。
//
// Server 会据此下发 REPLICAOF。判据必须是"刚刚看到的事实"：拿一份上次开机时的观测去
// 对着已经变化的拓扑动手，比没有观测更糟。缺失是安全的（Server 跳过），过期不是。
func TestMonitor_ReplicaOfMapSkipsStaleObservations(t *testing.T) {
	m := newMonitor()

	m.setRepl("fresh", &replicationState{Role: "slave", ReplicaOf: "10.0.1.11:6380"})
	// 直接写一个陈旧时间戳，setRepl 会把 UpdatedAt 覆盖成当前时间
	m.repl["stale"] = &replicationState{
		Role: "slave", ReplicaOf: "10.0.1.99:6379",
		UpdatedAt: time.Now().Add(-replicationStaleAfter - time.Second),
	}

	got := m.replicaOfMap()
	if _, ok := got["fresh"]; !ok {
		t.Error("新鲜观测应当上报")
	}
	if _, ok := got["stale"]; ok {
		t.Error("过期观测不应上报")
	}
}

// TestMonitor_ClearReplDropsObservation 验证探测失败时旧观测被丢弃，
// 而不是留着让一个已经不可达的实例继续以陈旧观测的身份充当判据。
func TestMonitor_ClearReplDropsObservation(t *testing.T) {
	m := newMonitor()
	m.setRepl("inst", &replicationState{Role: "slave", ReplicaOf: "10.0.1.11:6380"})
	if _, ok := m.replicaOfMap()["inst"]; !ok {
		t.Fatal("前置条件不成立")
	}
	m.clearRepl("inst")
	if _, ok := m.replicaOfMap()["inst"]; ok {
		t.Error("clearRepl 之后不应再出现在上报里")
	}
}

// TestMonitor_ReplicaOfMapReportsMasterAsEmptyString 验证 master 上报为空串，
// 与 Server 侧 resolveReplicaOfAddr 对 master 返回空串的约定一致。
func TestMonitor_ReplicaOfMapReportsMasterAsEmptyString(t *testing.T) {
	m := newMonitor()
	m.setRepl("inst", &replicationState{Role: "master"})
	got, ok := m.replicaOfMap()["inst"]
	if !ok {
		t.Fatal("master 也要上报，否则 Server 无法发现\"该是 replica 却成了 master\"")
	}
	if got != "" {
		t.Errorf("master 应上报空串，实际 %q", got)
	}
}
