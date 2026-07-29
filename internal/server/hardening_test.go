package server

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// TestReadRESP_RejectsResourceExhaustion 验证长度字段来自网络时不会被直接当成分配依据。
//
// 三种崩法都会带走整个 Server 进程：watchSentinelNode 跑在裸 goroutine 里，
// 那里 panic 没有 recover；栈溢出更是不可恢复的 fatal error。
func TestReadRESP_RejectsResourceExhaustion(t *testing.T) {
	cases := []struct {
		name  string
		input string
		why   string
	}{
		{
			name:  "bulk 长度溢出",
			input: "$9223372036854775807\r\n",
			why:   "make([]byte, n+2) 里 n+2 溢出成负数，make 会 panic",
		},
		{
			name:  "bulk 长度过大",
			input: "$1073741824\r\n",
			why:   "一次要求分配 1GiB",
		},
		{
			name:  "数组元素数过大",
			input: fmt.Sprintf("*%d\r\n", 1<<30),
			why:   "声称十亿个元素",
		},
		{
			name:  "嵌套过深",
			input: strings.Repeat("*1\r\n", 100),
			why:   "递归耗尽 goroutine 栈",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(c.input))
			_, err := readRESP(r)
			if err == nil {
				t.Errorf("应当返回错误而不是尝试分配：%s", c.why)
			}
		})
	}
}

// TestReadRESP_ParsesLegitimateReplies 确认加上限没有破坏正常解析。
// SENTINEL MASTERS 的回复形状是"数组套数组套字符串"，深度 2。
func TestReadRESP_ParsesLegitimateReplies(t *testing.T) {
	input := "*1\r\n*4\r\n$4\r\nname\r\n$5\r\norder\r\n$2\r\nip\r\n$9\r\n10.0.1.10\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	reply, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	outer, ok := reply.([]interface{})
	if !ok || len(outer) != 1 {
		t.Fatalf("外层应当是长度 1 的数组，实际 %#v", reply)
	}
	fields, ok := outer[0].([]interface{})
	if !ok || len(fields) != 4 {
		t.Fatalf("内层应当是长度 4 的数组，实际 %#v", outer[0])
	}
	if fields[1] != "order" || fields[3] != "10.0.1.10" {
		t.Errorf("字段解析错误：%#v", fields)
	}
}

func TestReadRESP_EmptyArray(t *testing.T) {
	// 空数组和 nil 数组都要能解析，且不能返回 nil 让调用方的类型断言失败
	for _, input := range []string{"*0\r\n", "*-1\r\n"} {
		r := bufio.NewReader(strings.NewReader(input))
		reply, err := readRESP(r)
		if err != nil {
			t.Errorf("%q: %v", input, err)
			continue
		}
		if _, ok := reply.([]interface{}); !ok {
			t.Errorf("%q 应当断言为 []interface{}，实际 %#v", input, reply)
		}
	}
}

// TestNodeAdd_ConcurrentSameNameYieldsExactlyOneWinner 验证节点增删改的读-改-写是原子的。
//
// 早期实现是 ReadNode → 检查是否已存在 → WriteNode，检查和写入之间没有互斥：
// 两个同名的并发 node add 会双双通过存在性检查，后写的静默覆盖先写的，
// 冲突检查形同虚设。
func TestNodeAdd_ConcurrentSameNameYieldsExactlyOneWinner(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()

	const n = 8
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := doRequest(r, "POST", "/node/add", map[string]interface{}{
				"name": "srv-race",
				"server": map[string]interface{}{
					"endpoint":   fmt.Sprintf("10.0.1.%d", idx+1),
					"agent_port": 8400,
				},
			})
			codes[idx] = w.Code
		}(i)
	}
	wg.Wait()

	created, conflicts := 0, 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			t.Errorf("意外的状态码 %d", code)
		}
	}
	if created != 1 {
		t.Errorf("成功创建 %d 次，应当恰好 1 次（其余应为 409）", created)
	}
	if conflicts != n-1 {
		t.Errorf("冲突 %d 次，应当 %d 次", conflicts, n-1)
	}
}

// TestNodeRemove_ConcurrentWithUpdateDoesNotResurrect 验证被删除的节点不会被
// 一个持过期快照的 update 写回来。
func TestNodeRemove_ConcurrentWithUpdateDoesNotResurrect(t *testing.T) {
	s := newTestServer(t, "")
	r := s.Router()

	if w := doRequest(r, "POST", "/node/add", map[string]interface{}{
		"name":   "srv-1",
		"server": map[string]interface{}{"endpoint": "10.0.1.10", "agent_port": 8400},
	}); w.Code != http.StatusOK {
		t.Fatalf("准备阶段 node add 失败：%d %s", w.Code, w.Body.String())
	}

	var wg sync.WaitGroup
	var removeCode, updateCode int
	wg.Add(2)
	go func() {
		defer wg.Done()
		removeCode = doRequest(r, "POST", "/node/remove", map[string]interface{}{"name": "srv-1"}).Code
	}()
	go func() {
		defer wg.Done()
		updateCode = doRequest(r, "POST", "/node/update", map[string]interface{}{
			"name":   "srv-1",
			"server": map[string]interface{}{"endpoint": "10.0.1.99", "agent_port": 8400},
		}).Code
	}()
	wg.Wait()

	node, err := s.state.ReadNode()
	if err != nil {
		t.Fatal(err)
	}
	_, exists := node.Servers["srv-1"]

	// 两种合法结局：remove 后 update（update 得 404，节点不存在）
	// 或 update 后 remove（都成功，节点不存在）。
	// 不合法的是"remove 返回成功但节点还在"——那说明 update 用过期快照复活了它。
	if removeCode == http.StatusOK && exists && updateCode == http.StatusOK {
		t.Error("remove 与 update 都返回成功且节点仍存在：update 用过期快照把已删除的节点写回来了")
	}
	if removeCode == http.StatusOK && updateCode == http.StatusNotFound && exists {
		t.Error("remove 成功、update 报不存在，但节点还在")
	}
}
