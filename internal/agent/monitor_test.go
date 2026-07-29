package agent

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
)

// fakeRedis 是一个极简的 RESP 服务端，用于测试 tcpPing 对各种回复的分类。
// authReply / pingReply 直接作为原始 RESP 行写回（需自带 \r\n）。
type fakeRedis struct {
	listener  net.Listener
	authReply string
	pingReply string
}

func newFakeRedis(t *testing.T, authReply, pingReply string) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{listener: ln, authReply: authReply, pingReply: pingReply}
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeRedis) addr() string { return f.listener.Addr().String() }

func (f *fakeRedis) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				// 读一条 RESP 命令：*N 后跟 N 个 $len\r\npayload
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if !strings.HasPrefix(line, "*") {
					continue
				}
				var n int
				fmt.Sscanf(strings.TrimSpace(line), "*%d", &n)
				var cmd string
				for i := 0; i < n; i++ {
					if _, err := r.ReadString('\n'); err != nil { // $len
						return
					}
					arg, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if i == 0 {
						cmd = strings.ToUpper(strings.TrimSpace(arg))
					}
				}
				switch cmd {
				case "AUTH":
					fmt.Fprint(c, f.authReply)
				case "PING":
					fmt.Fprint(c, f.pingReply)
				default:
					fmt.Fprint(c, "+OK\r\n")
				}
			}
		}(conn)
	}
}

// TestTcpPing_Classification 验证 tcpPing 把不同回复归入正确的健康分类。
//
// 回归背景：早期实现只返回一个无区分的 error，判定条件是
// !Contains(resp,"PONG") && !HasPrefix(resp,"+")，于是 -LOADING、-NOAUTH、
// -MASTERDOWN 全部被归成同一个"不健康"，再触发同一个动作 podman restart。后果：
// 正在加载大数据集的实例被反复重启而永远起不来；密码不一致的健康实例每 30s 被重启一次。
func TestTcpPing_Classification(t *testing.T) {
	cases := []struct {
		name      string
		password  string
		authReply string
		pingReply string
		want      instanceHealth
		wantErr   bool
	}{
		{
			name:      "健康实例（无密码）",
			pingReply: "+PONG\r\n",
			want:      healthOK,
		},
		{
			name:      "健康实例（带密码，AUTH 成功）",
			password:  "pass",
			authReply: "+OK\r\n",
			pingReply: "+PONG\r\n",
			want:      healthOK,
		},
		{
			name:      "正在加载数据集：不是故障，重启会导致永远起不来",
			pingReply: "-LOADING Redis is loading the dataset in memory\r\n",
			want:      healthLoading,
			wantErr:   true,
		},
		{
			name:      "未认证：配置问题而非实例故障",
			pingReply: "-NOAUTH Authentication required.\r\n",
			want:      healthAuthFailed,
			wantErr:   true,
		},
		{
			name:      "密码错误：AUTH 阶段就被拒，必须检查 AUTH 回复才能发现",
			password:  "wrong",
			authReply: "-WRONGPASS invalid username-password pair\r\n",
			pingReply: "+PONG\r\n",
			want:      healthAuthFailed,
			wantErr:   true,
		},
		{
			name:      "复制链路故障：实例本身活着",
			pingReply: "-MASTERDOWN Link with MASTER is down\r\n",
			want:      healthReplLinkErr,
			wantErr:   true,
		},
		{
			name:      "非预期回复",
			pingReply: "-ERR something else\r\n",
			want:      healthUnexpected,
			wantErr:   true,
		},
		{
			name:      "不能把残留的 +OK 当成健康",
			pingReply: "+OK\r\n",
			want:      healthUnexpected,
			wantErr:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRedis(t, tc.authReply, tc.pingReply)
			got, err := tcpPing(f.addr(), tc.password)
			if got != tc.want {
				t.Errorf("health = %q, want %q (err=%v)", got, tc.want, err)
			}
			if tc.wantErr && err == nil {
				t.Error("expected a non-nil error alongside the classification")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestTcpPing_Unreachable 验证 TCP 层连不上时归类为 unreachable。
func TestTcpPing_Unreachable(t *testing.T) {
	// 先占一个端口再立即释放，得到一个几乎确定无人监听的地址
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	got, err := tcpPing(addr, "")
	if got != healthUnreachable {
		t.Errorf("health = %q, want %q", got, healthUnreachable)
	}
	if err == nil {
		t.Error("expected error for unreachable address")
	}
}

// TestClassifyRESPError 固定错误分类的映射关系。
func TestClassifyRESPError(t *testing.T) {
	cases := map[string]instanceHealth{
		"-LOADING Redis is loading the dataset in memory\r\n":                                         healthLoading,
		"-NOAUTH Authentication required.\r\n":                                                        healthAuthFailed,
		"-WRONGPASS invalid username-password pair\r\n":                                               healthAuthFailed,
		"-ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?\r\n": healthUnexpected,
		"-MASTERDOWN Link with MASTER is down\r\n":                                                    healthReplLinkErr,
		"-ERR unknown command\r\n":                                                                    healthUnexpected,
	}
	for resp, want := range cases {
		got, ok := classifyRESPError(resp)
		if !ok {
			t.Errorf("classifyRESPError(%q) reported not-an-error", resp)
			continue
		}
		if got != want {
			t.Errorf("classifyRESPError(%q) = %q, want %q", resp, got, want)
		}
	}

	// 非错误回复不应被识别为错误
	for _, resp := range []string{"+PONG\r\n", "+OK\r\n", ":1\r\n"} {
		if _, ok := classifyRESPError(resp); ok {
			t.Errorf("classifyRESPError(%q) should report not-an-error", resp)
		}
	}
}

// TestMonitorHealthStore 验证健康分类的存取。
func TestMonitorHealthStore(t *testing.T) {
	m := newMonitor()
	if got := m.getHealth("never-probed"); got != nil {
		t.Errorf("expected nil for never-probed instance, got %#v", got)
	}

	m.setHealth("foo", healthLoading, "loading dataset")
	h := m.getHealth("foo")
	if h == nil {
		t.Fatal("expected health state to be stored")
	}
	if h.Health != healthLoading || h.Detail != "loading dataset" {
		t.Errorf("unexpected health state: %#v", h)
	}
	if h.UpdatedAt.IsZero() {
		t.Error("expected UpdatedAt to be set")
	}

	// 覆盖写
	m.setHealth("foo", healthOK, "")
	if h := m.getHealth("foo"); h.Health != healthOK || h.Detail != "" {
		t.Errorf("expected health to be overwritten, got %#v", h)
	}
}

// readSourceFile 读取本包内的源码文件，供结构性约束测试使用。
func readSourceFile(name string) (string, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// stripComments 去掉行注释，使结构性约束测试只针对真实代码断言——注释里为了解释
// 「为什么删掉了自主重启」会合理地提到被删除的函数名。
func stripComments(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// TestNoAutonomousRestart 是一条结构性约束测试：Agent 不得再自主重启实例。
//
// 重启是破坏性且有全局影响的动作（断连接、丢未落盘数据、按配置文件旧角色重启导致
// Sentinel 的提升被回退、可能触发不必要的 failover），而一次 PING 失败的信息量不足以
// 支撑这个决策。进程退出由 podman --restart on-failure:5 承担；超限后停住并由 Server
// 的 reconcile 告警是期望行为，不应被静默重启掩盖。
//
// 这条测试通过源码断言来防止将来有人重新引入自主重启。
func TestNoAutonomousRestart(t *testing.T) {
	data, err := readSourceFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := stripComments(data)
	for _, forbidden := range []string{
		`"restart"`,      // runShell("podman", "restart", ...)
		"podmanRestart(", // 已删除的辅助函数
	} {
		if strings.Contains(code, forbidden) {
			t.Errorf("monitor.go 的代码中出现了 %q：Agent 不应基于本机局部观测自主重启实例，"+
				"容器级自动重启由 podman --restart on-failure:5 承担", forbidden)
		}
	}
}
