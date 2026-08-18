package sentinel

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/discovery"
)

// fakeTopo 返回一个固定的拓扑供测试使用。
func fakeTopo(groups map[string]*discovery.GroupTopology) TopoFunc {
	return func() *discovery.Topology {
		if groups == nil {
			return nil
		}
		return &discovery.Topology{Groups: groups, Version: "test-v1"}
	}
}

// dialSentinel 启动一个伪 Sentinel 并返回与其对话的 TCP 连接。
func dialSentinel(t *testing.T, cfg Config, topo TopoFunc) (net.Conn, *Server) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, topo)
	go srv.Serve(ln)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})
	return conn, srv
}

// sendCmd 发送一条 RESP 命令。
func sendCmd(conn net.Conn, args ...string) {
	var buf []byte
	buf = append(buf, fmt.Sprintf("*%d\r\n", len(args))...)
	for _, a := range args {
		buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
	}
	conn.Write(buf)
}

// readLine 从连接读一行（含 \r\n）。
func readLine(r *bufio.Reader) string {
	line, _ := r.ReadString('\n')
	return line
}

// readReply 读取一个完整 RESP 响应。
func readReply(r *bufio.Reader) string {
	first, err := r.ReadByte()
	if err != nil {
		return ""
	}

	switch first {
	case '+', '-', ':':
		line, _ := r.ReadString('\n')
		return string(first) + line
	case '$':
		line, _ := r.ReadString('\n')
		header := "$" + line
		lenStr := strings.TrimSpace(line)
		if lenStr == "-1" {
			return header
		}
		n := 0
		fmt.Sscanf(lenStr, "%d", &n)
		data := make([]byte, n+2)
		r.Read(data)
		return header + string(data)
	case '*':
		line, _ := r.ReadString('\n')
		header := "*" + line
		countStr := strings.TrimSpace(line)
		if countStr == "-1" {
			return header
		}
		count := 0
		fmt.Sscanf(countStr, "%d", &count)
		result := header
		for i := 0; i < count; i++ {
			result += readReply(r)
		}
		return result
	}
	return ""
}

func TestGetMasterAddrByName_Hit(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379, MasterPort: 16500},
	}
	conn, _ := dialSentinel(t, Config{}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "order")
	reply := readReply(r)

	// 期望 2 元素数组 ["127.0.0.1", "16379"]
	if !strings.Contains(reply, "*2\r\n") {
		t.Fatalf("expected 2-element array, got: %q", reply)
	}
	if !strings.Contains(reply, "16379") {
		t.Fatalf("expected port 16379, got: %q", reply)
	}
}

func TestGetMasterAddrByName_FallbackMasterPort(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 0, MasterPort: 16500},
	}
	conn, _ := dialSentinel(t, Config{}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "order")
	reply := readReply(r)

	if !strings.Contains(reply, "16500") {
		t.Fatalf("expected port 16500, got: %q", reply)
	}
}

func TestGetMasterAddrByName_NotFound(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "nonexist")
	reply := readReply(r)

	// 期望 null 数组
	if reply != "*-1\r\n" {
		t.Fatalf("expected null array, got: %q", reply)
	}
}

func TestGetMasterAddrByName_AnnounceIP(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{AnnounceIP: "10.0.0.9"}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "order")
	reply := readReply(r)

	if !strings.Contains(reply, "10.0.0.9") {
		t.Fatalf("expected AnnounceIP 10.0.0.9, got: %q", reply)
	}
}

func TestSlaves_Replicas_Sentinels_Empty(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	for _, sub := range []string{"slaves", "replicas", "sentinels"} {
		sendCmd(conn, "SENTINEL", sub, "order")
		reply := readReply(r)
		if reply != "*0\r\n" {
			t.Fatalf("SENTINEL %s: expected empty array, got: %q", sub, reply)
		}
	}
}

func TestMaster(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{AnnounceIP: "10.0.0.1"}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "master", "order")
	reply := readReply(r)

	// 扁平数组应包含关键字段
	if !strings.Contains(reply, "name") || !strings.Contains(reply, "order") {
		t.Fatalf("expected name=order in master response, got: %q", reply)
	}
	if !strings.Contains(reply, "10.0.0.1") {
		t.Fatalf("expected ip=10.0.0.1 in master response, got: %q", reply)
	}
	if !strings.Contains(reply, "16379") {
		t.Fatalf("expected port=16379 in master response, got: %q", reply)
	}
	if !strings.Contains(reply, "master") {
		t.Fatalf("expected flags=master in master response, got: %q", reply)
	}
	if !strings.Contains(reply, "num-slaves") {
		t.Fatalf("expected num-slaves field in master response, got: %q", reply)
	}
}

func TestAuth_RequiredAndAccepted(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{Password: "secret123"}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	// 未认证直接发 SENTINEL 应被拒
	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "order")
	reply := readReply(r)
	if !strings.Contains(reply, "NOAUTH") {
		t.Fatalf("expected NOAUTH, got: %q", reply)
	}

	// 错误密码
	sendCmd(conn, "AUTH", "wrong")
	reply = readReply(r)
	if !strings.Contains(reply, "ERR invalid password") {
		t.Fatalf("expected ERR invalid password, got: %q", reply)
	}

	// 正确密码
	sendCmd(conn, "AUTH", "secret123")
	reply = readReply(r)
	if !strings.Contains(reply, "+OK") {
		t.Fatalf("expected +OK, got: %q", reply)
	}

	// 认证后命令应放行
	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "order")
	reply = readReply(r)
	if !strings.Contains(reply, "16379") {
		t.Fatalf("expected port 16379 after auth, got: %q", reply)
	}
}

func TestAuth_NoPassword(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{Password: ""}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	// 无密码配置时 AUTH 应报错但不影响连接
	sendCmd(conn, "AUTH", "anything")
	reply := readReply(r)
	if !strings.Contains(reply, "ERR Client sent AUTH, but no password is set") {
		t.Fatalf("expected no-password error, got: %q", reply)
	}

	// 应已认证，命令正常
	sendCmd(conn, "PING")
	reply = readReply(r)
	if !strings.Contains(reply, "PONG") {
		t.Fatalf("expected PONG, got: %q", reply)
	}
}

func TestFailoverRejected(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	conn, _ := dialSentinel(t, Config{}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "failover", "order")
	reply := readReply(r)
	if !strings.Contains(reply, "ERR unsupported") {
		t.Fatalf("expected ERR unsupported, got: %q", reply)
	}
}

func TestHello_NOPROTO(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "HELLO", "3")
	reply := readReply(r)
	if !strings.Contains(reply, "NOPROTO") {
		t.Fatalf("expected NOPROTO, got: %q", reply)
	}
}

func TestSubscribe(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SUBSCRIBE", "+switch-master")
	reply := readReply(r)

	// 期望订阅确认帧：*3 [subscribe, +switch-master, 1]
	if !strings.Contains(reply, "subscribe") {
		t.Fatalf("expected subscribe in reply, got: %q", reply)
	}
	if !strings.Contains(reply, "+switch-master") {
		t.Fatalf("expected channel name in reply, got: %q", reply)
	}

	// 连接应保持活跃（能发 PING）
	sendCmd(conn, "PING")
	reply = readReply(r)
	if !strings.Contains(reply, "PONG") {
		t.Fatalf("expected PONG after subscribe, got: %q", reply)
	}
}

func TestPing(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "PING")
	reply := readReply(r)
	if reply != "+PONG\r\n" {
		t.Fatalf("expected +PONG, got: %q", reply)
	}
}

func TestTopologyHotUpdate(t *testing.T) {
	// 初始拓扑只有 order
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
	}
	topo := &discovery.Topology{Groups: groups, Version: "v1"}
	topoPtr := &topo

	topoFunc := func() *discovery.Topology {
		return *topoPtr
	}

	conn, _ := dialSentinel(t, Config{}, topoFunc)
	r := bufio.NewReader(conn)

	// 新增 group cache 后立即查
	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "cache")
	reply := readReply(r)
	if reply != "*-1\r\n" {
		t.Fatalf("expected null for cache before update, got: %q", reply)
	}

	// 更新拓扑
	newGroups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
		"cache": {Name: "cache", AutoPort: 16380},
	}
	*topoPtr = &discovery.Topology{Groups: newGroups, Version: "v2"}

	// 再次查询应命中
	sendCmd(conn, "SENTINEL", "get-master-addr-by-name", "cache")
	reply = readReply(r)
	if !strings.Contains(reply, "16380") {
		t.Fatalf("expected port 16380 for cache after update, got: %q", reply)
	}
}

func TestMasters(t *testing.T) {
	groups := map[string]*discovery.GroupTopology{
		"order": {Name: "order", AutoPort: 16379},
		"cache": {Name: "cache", AutoPort: 16380},
	}
	conn, _ := dialSentinel(t, Config{AnnounceIP: "10.0.0.1"}, fakeTopo(groups))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "masters")
	reply := readReply(r)

	// 应包含两个 group
	if !strings.Contains(reply, "order") {
		t.Fatalf("expected order in masters response, got: %q", reply)
	}
	if !strings.Contains(reply, "cache") {
		t.Fatalf("expected cache in masters response, got: %q", reply)
	}
}

func TestInfo(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "INFO")
	reply := readReply(r)

	if !strings.Contains(reply, "redis_mode:sentinel") {
		t.Fatalf("expected redis_mode:sentinel in INFO response, got: %q", reply)
	}
}

func TestUnsubscribe(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SUBSCRIBE", "+switch-master")
	_ = readReply(r) // 消费 subscribe 确认

	sendCmd(conn, "UNSUBSCRIBE", "+switch-master")
	reply := readReply(r)
	if !strings.Contains(reply, "unsubscribe") {
		t.Fatalf("expected unsubscribe in reply, got: %q", reply)
	}
}

func TestCommand_EmptyArray(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "COMMAND")
	reply := readReply(r)
	if reply != "*0\r\n" {
		t.Fatalf("expected empty array for COMMAND, got: %q", reply)
	}
}

func TestClient_OK(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "CLIENT", "SETNAME", "test")
	reply := readReply(r)
	if reply != "+OK\r\n" {
		t.Fatalf("expected +OK for CLIENT, got: %q", reply)
	}
}

func TestUnknownCommand(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "FOOBAR")
	reply := readReply(r)
	if !strings.Contains(reply, "ERR unknown command") {
		t.Fatalf("expected ERR unknown command, got: %q", reply)
	}
}

func TestUnknownSentinelSubcommand(t *testing.T) {
	conn, _ := dialSentinel(t, Config{}, fakeTopo(nil))
	r := bufio.NewReader(conn)

	sendCmd(conn, "SENTINEL", "foobar")
	reply := readReply(r)
	if !strings.Contains(reply, "ERR unknown sentinel subcommand") {
		t.Fatalf("expected ERR unknown sentinel subcommand, got: %q", reply)
	}
}
