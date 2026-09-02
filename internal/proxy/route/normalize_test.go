package route

import "testing"

func TestNormalizeCommand(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// 已知读命令 -> 大写归一
		{"read lower", "get", "GET"},
		{"read mixed", "HgEtAlL", "HGETALL"},
		// 已知写命令（走 knownWriteCommands 白名单）
		{"write set", "set", "SET"},
		{"write del", "DEL", "DEL"},
		{"write xadd", "xadd", "XADD"},
		// 子命令依赖类命令本体名
		{"config", "config", "CONFIG"},
		{"memory", "MEMORY", "MEMORY"},
		{"object", "object", "OBJECT"},
		{"xread", "xread", "XREAD"},
		// 事务/订阅/阻塞/连接态/拒绝类
		{"multi", "multi", "MULTI"},
		{"subscribe", "subscribe", "SUBSCRIBE"},
		{"blpop", "blpop", "BLPOP"},
		{"ping", "ping", "PING"},
		{"keys deny", "keys", "KEYS"},
		{"master-only eval", "eval", "EVAL"},
		// 未知命令 -> OTHER（基数保护的核心）
		{"unknown", "FOOBAR", "OTHER"},
		{"gibberish", "\x01\x02random", "OTHER"},
		{"empty", "", "OTHER"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := NormalizeCommand([]byte(c.in))
			if got != c.want {
				t.Fatalf("NormalizeCommand(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
