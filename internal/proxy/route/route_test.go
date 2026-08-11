package route

import (
	"testing"
)

// helper：构造 argv
func argv(s ...string) [][]byte {
	out := make([][]byte, len(s))
	for i, v := range s {
		out[i] = []byte(v)
	}
	return out
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		argv [][]byte
		want Class
		pin  bool
		tgt  Target
	}{
		// ── WRITE（兜底 + 已知写命令）──
		{"SET", argv("SET", "k", "v"), ClassWrite, false, TargetMaster},
		{"DEL", argv("DEL", "k"), ClassWrite, false, TargetMaster},
		{"HSET", argv("HSET", "h", "f", "v"), ClassWrite, false, TargetMaster},
		{"LPUSH", argv("LPUSH", "l", "v"), ClassWrite, false, TargetMaster},
		{"ZADD", argv("ZADD", "z", "1", "m"), ClassWrite, false, TargetMaster},
		{"XADD", argv("XADD", "s", "*", "f", "v"), ClassWrite, false, TargetMaster},
		{"XGROUP CREATE", argv("XGROUP", "CREATE", "s", "g", "$"), ClassWrite, false, TargetMaster},
		{"XACK", argv("XACK", "s", "g", "id"), ClassWrite, false, TargetMaster},
		{"SORT", argv("SORT", "k"), ClassWrite, false, TargetMaster},
		{"GEORADIUS", argv("GEORADIUS", "k", "1", "2", "1", "km"), ClassWrite, false, TargetMaster},
		{"PFADD", argv("PFADD", "k", "v"), ClassWrite, false, TargetMaster},
		{"PFCOUNT", argv("PFCOUNT", "k"), ClassWrite, false, TargetMaster},
		{"unknown command fallback", argv("FOOBAR", "k"), ClassWrite, false, TargetMaster},

		// ── READ（只读白名单）──
		{"GET", argv("GET", "k"), ClassRead, false, TargetReadPool},
		{"MGET", argv("MGET", "k1", "k2"), ClassRead, false, TargetReadPool},
		{"EXISTS", argv("EXISTS", "k"), ClassRead, false, TargetReadPool},
		{"HGET", argv("HGET", "h", "f"), ClassRead, false, TargetReadPool},
		{"ZRANGE", argv("ZRANGE", "z", "0", "-1"), ClassRead, false, TargetReadPool},
		{"RANDOMKEY", argv("RANDOMKEY"), ClassRead, false, TargetReadPool},
		{"BITFIELD_RO", argv("BITFIELD_RO", "k", "GET", "u4", "0"), ClassRead, false, TargetReadPool},
		{"GEORADIUS_RO", argv("GEORADIUS_RO", "k", "1", "2", "1", "km"), ClassRead, false, TargetReadPool},
		{"XRANGE", argv("XRANGE", "s", "-", "+"), ClassRead, false, TargetReadPool},
		{"XLEN", argv("XLEN", "s"), ClassRead, false, TargetReadPool},
		{"XINFO", argv("XINFO", "STREAM", "s"), ClassRead, false, TargetReadPool},
		{"XPENDING", argv("XPENDING", "s", "g"), ClassRead, false, TargetReadPool},

		// ── BLOCKING（固有）──
		{"BLPOP", argv("BLPOP", "k", "0"), ClassBlocking, true, TargetMaster},
		{"BRPOP", argv("BRPOP", "k", "0"), ClassBlocking, true, TargetMaster},
		{"BLMOVE", argv("BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"), ClassBlocking, true, TargetMaster},
		{"BZPOPMIN", argv("BZPOPMIN", "z", "0"), ClassBlocking, true, TargetMaster},
		{"BZMPOP", argv("BZMPOP", "0", "1", "z"), ClassBlocking, true, TargetMaster},
		{"WAIT", argv("WAIT", "1", "100"), ClassBlocking, true, TargetMaster},
		{"WAITAOF", argv("WAITAOF", "1", "0", "100"), ClassBlocking, true, TargetMaster},

		// ── BLOCKING（参数触发：XREAD / XREADGROUP 带 BLOCK）──
		{"XREAD no BLOCK", argv("XREAD", "STREAMS", "s", "0"), ClassRead, false, TargetReadPool},
		{"XREAD with BLOCK", argv("XREAD", "BLOCK", "1000", "STREAMS", "s", "$"), ClassBlocking, true, TargetMaster},
		{"XREAD with lowercase block", argv("XREAD", "block", "1000", "STREAMS", "s", "$"), ClassBlocking, true, TargetMaster},
		{"XREADGROUP no BLOCK", argv("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"), ClassMasterOnly, false, TargetMaster},
		{"XREADGROUP with BLOCK", argv("XREADGROUP", "GROUP", "g", "c", "BLOCK", "1000", "STREAMS", "s", ">"), ClassBlocking, true, TargetMaster},

		// ── TXN ──
		{"MULTI", argv("MULTI"), ClassTxn, true, TargetMaster},
		{"EXEC", argv("EXEC"), ClassTxn, false, TargetMaster},
		{"DISCARD", argv("DISCARD"), ClassTxn, false, TargetMaster},
		{"WATCH", argv("WATCH", "k"), ClassTxn, true, TargetMaster},
		{"UNWATCH", argv("UNWATCH"), ClassTxn, false, TargetMaster},

		// ── SUBSCRIBE ──
		{"SUBSCRIBE", argv("SUBSCRIBE", "ch"), ClassSubscribe, true, TargetMaster},
		{"PSUBSCRIBE", argv("PSUBSCRIBE", "pat*"), ClassSubscribe, true, TargetMaster},
		{"SSUBSCRIBE", argv("SSUBSCRIBE", "ch"), ClassSubscribe, true, TargetMaster},
		{"UNSUBSCRIBE", argv("UNSUBSCRIBE", "ch"), ClassSubscribe, true, TargetMaster},
		{"PUNSUBSCRIBE", argv("PUNSUBSCRIBE", "pat*"), ClassSubscribe, true, TargetMaster},
		{"SUNSUBSCRIBE", argv("SUNSUBSCRIBE", "ch"), ClassSubscribe, true, TargetMaster},

		// ── MASTER_ONLY ──
		{"EVAL", argv("EVAL", "return 1", "0"), ClassMasterOnly, false, TargetMaster},
		{"EVALSHA", argv("EVALSHA", "abc123", "0"), ClassMasterOnly, false, TargetMaster},
		{"FCALL", argv("FCALL", "fn", "0"), ClassMasterOnly, false, TargetMaster},
		{"SCAN", argv("SCAN", "0"), ClassMasterOnly, false, TargetMaster},
		{"INFO", argv("INFO"), ClassMasterOnly, false, TargetMaster},
		{"DBSIZE", argv("DBSIZE"), ClassMasterOnly, false, TargetMaster},
		{"ROLE", argv("ROLE"), ClassMasterOnly, false, TargetMaster},
		{"PUBLISH", argv("PUBLISH", "ch", "msg"), ClassMasterOnly, false, TargetMaster},
		{"COMMAND", argv("COMMAND"), ClassMasterOnly, false, TargetMaster},
		{"COMMAND DOCS", argv("COMMAND", "DOCS"), ClassMasterOnly, false, TargetMaster},
		{"COMMAND LIST", argv("COMMAND", "LIST"), ClassMasterOnly, false, TargetMaster},

		// ── CONN_STATE ──
		{"AUTH", argv("AUTH", "pass"), ClassConnState, false, TargetLocal},
		{"SELECT", argv("SELECT", "3"), ClassConnState, true, TargetMaster},
		{"HELLO", argv("HELLO"), ClassConnState, false, TargetLocal},
		{"PING", argv("PING"), ClassConnState, false, TargetLocal},
		{"PING with msg", argv("PING", "msg"), ClassConnState, false, TargetLocal},
		{"ECHO", argv("ECHO", "hi"), ClassConnState, false, TargetLocal},
		{"QUIT", argv("QUIT"), ClassConnState, false, TargetLocal},
		{"RESET", argv("RESET"), ClassConnState, false, TargetLocal},

		// ── CONN_STATE: CLIENT 子命令 ──
		{"CLIENT GETNAME", argv("CLIENT", "GETNAME"), ClassConnState, false, TargetLocal},
		{"CLIENT SETNAME", argv("CLIENT", "SETNAME", "my"), ClassConnState, false, TargetLocal},
		{"CLIENT ID", argv("CLIENT", "ID"), ClassConnState, false, TargetLocal},
		{"CLIENT INFO", argv("CLIENT", "INFO"), ClassConnState, false, TargetLocal},
		{"CLIENT LIST", argv("CLIENT", "LIST"), ClassMasterOnly, false, TargetMaster},
		{"CLIENT KILL", argv("CLIENT", "KILL", "addr", "1.2.3.4:6379"), ClassDeny, false, TargetLocal},
		{"CLIENT PAUSE", argv("CLIENT", "PAUSE", "1000"), ClassDeny, false, TargetLocal},
		{"CLIENT NO-EVICT", argv("CLIENT", "NO-EVICT", "ON"), ClassDeny, false, TargetLocal},
		{"CLIENT unknown sub", argv("CLIENT", "FOOBAR"), ClassDeny, false, TargetLocal},
		{"CLIENT no sub", argv("CLIENT"), ClassDeny, false, TargetLocal},

		// ── MEMORY 子命令 ──
		{"MEMORY USAGE", argv("MEMORY", "USAGE", "k"), ClassRead, false, TargetReadPool},
		{"MEMORY STATS", argv("MEMORY", "STATS"), ClassAdmin, false, TargetMaster},
		{"MEMORY PURGE", argv("MEMORY", "PURGE"), ClassAdmin, false, TargetMaster},
		{"MEMORY DOCTOR", argv("MEMORY", "DOCTOR"), ClassAdmin, false, TargetMaster},
		{"MEMORY unknown", argv("MEMORY", "FOO"), ClassDeny, false, TargetLocal},
		{"MEMORY no sub", argv("MEMORY"), ClassDeny, false, TargetLocal},

		// ── OBJECT 子命令 ──
		{"OBJECT ENCODING", argv("OBJECT", "ENCODING", "k"), ClassRead, false, TargetReadPool},
		{"OBJECT IDLETIME", argv("OBJECT", "IDLETIME", "k"), ClassRead, false, TargetReadPool},
		{"OBJECT FREQ", argv("OBJECT", "FREQ", "k"), ClassRead, false, TargetReadPool},
		{"OBJECT unknown sub", argv("OBJECT", "FOO", "k"), ClassMasterOnly, false, TargetMaster},
		{"OBJECT no sub", argv("OBJECT"), ClassMasterOnly, false, TargetMaster},

		// ── DENY ──
		{"MONITOR", argv("MONITOR"), ClassDeny, false, TargetLocal},
		{"FLUSHALL", argv("FLUSHALL"), ClassDeny, false, TargetLocal},
		{"FLUSHDB", argv("FLUSHDB"), ClassDeny, false, TargetLocal},
		{"KEYS", argv("KEYS", "*"), ClassDeny, false, TargetLocal},
		{"SHUTDOWN", argv("SHUTDOWN"), ClassDeny, false, TargetLocal},
		{"DEBUG", argv("DEBUG", "SLEEP", "1"), ClassDeny, false, TargetLocal},
		{"CONFIG GET", argv("CONFIG", "GET", "maxmemory"), ClassAdmin, false, TargetMaster},
		{"CONFIG SET", argv("CONFIG", "SET", "maxmemory", "100mb"), ClassDeny, false, TargetLocal},
		{"CONFIG no sub", argv("CONFIG"), ClassDeny, false, TargetLocal},
		{"CLUSTER", argv("CLUSTER", "INFO"), ClassConnState, false, TargetLocal},
		{"REPLICAOF", argv("REPLICAOF", "1.2.3.4", "6379"), ClassDeny, false, TargetLocal},
		{"SLAVEOF", argv("SLAVEOF", "1.2.3.4", "6379"), ClassDeny, false, TargetLocal},
		{"PSYNC", argv("PSYNC", "?", "-1"), ClassDeny, false, TargetLocal},
		{"ACL", argv("ACL", "WHOAMI"), ClassDeny, false, TargetLocal},
		{"LATENCY", argv("LATENCY", "GRAPH"), ClassDeny, false, TargetLocal},
		{"FAILOVER", argv("FAILOVER"), ClassDeny, false, TargetLocal},
		{"SWAPDB", argv("SWAPDB", "0", "1"), ClassDeny, false, TargetLocal},

		// ── 大小写不敏感 ──
		{"lowercase get", argv("get", "k"), ClassRead, false, TargetReadPool},
		{"mixed case Set", argv("Set", "k", "v"), ClassWrite, false, TargetMaster},
		{"lowercase blpop", argv("blpop", "k", "0"), ClassBlocking, true, TargetMaster},
		{"lowercase auth", argv("auth", "pass"), ClassConnState, false, TargetLocal},

		// ── 边界 ──
		{"empty argv", argv(), ClassDeny, false, TargetLocal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Classify(tt.argv)
			if d.Class != tt.want {
				t.Errorf("Class = %v, want %v", d.Class, tt.want)
			}
			if d.Pin != tt.pin {
				t.Errorf("Pin = %v, want %v", d.Pin, tt.pin)
			}
			if d.Target != tt.tgt {
				t.Errorf("Target = %v, want %v", d.Target, tt.tgt)
			}
		})
	}
}

// TestReadWhitelistInvariant 守护 §2 不变量：
// 只读白名单外的命令都不得判成 READ。
// 遍历 readCommands map 里每条命令，确认确实返回 ClassRead。
// 这是防止有人误把写命令加进白名单的元测试。
func TestReadWhitelistInvariant(t *testing.T) {
	for cmd := range readCommands {
		d := Classify(argv(cmd, "k"))
		if d.Class != ClassRead {
			t.Errorf("readCommands contains %q but Classify returned %v (not Read)", cmd, d.Class)
		}
	}
}

// TestNoWriteInReadList 确认已知的写命令不在只读白名单里。
func TestNoWriteInReadList(t *testing.T) {
	writeCmds := []string{
		"SET", "DEL", "INCR", "HSET", "LPUSH", "ZADD",
		"SADD", "SETBIT", "BITOP", "PFADD", "GEOADD",
		"XADD", "XACK", "XDEL", "XTRIM",
	}
	for _, cmd := range writeCmds {
		if _, ok := readCommands[cmd]; ok {
			t.Errorf("write command %q found in readCommands", cmd)
		}
	}
}
