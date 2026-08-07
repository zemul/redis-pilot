// Package route 是命令路由决策的纯函数层。
//
// 输入是 RESP 解析后的 argv（命令名 + 参数），输出是 Decision（分类 + 是否 pin +
// 路由目标）。整个包不依赖连接、后端、快照等任何运行时状态，可独立单测。
//
// 判定优先级见 proxy-command-routing.md §2：DENY > CONN_STATE > SUBSCRIBE > TXN >
// 参数触发 BLOCKING > 固有 BLOCKING > MASTER_ONLY > READ > 兜底 WRITE。
package route

// Class 是命令的默认分类。
type Class int

const (
	ClassWrite     Class = iota // 写命令，走 master
	ClassRead                   // 只读命令，走读池（master + replica 按 weight）
	ClassBlocking               // 阻塞命令，pin 独占 + 走 master
	ClassTxn                    // 事务控制，pin 独占 + 走 master
	ClassSubscribe              // 发布订阅，pin 独占 + 走 master
	ClassMasterOnly             // 固定落主，不 pin
	ClassAdmin                  // 管理/信息类，走 master（部分可 DENY）
	ClassConnState              // 连接级状态，proxy 终结或 pin
	ClassDeny                   // 拒绝执行
)

// Target 是命令的路由目标。
type Target int

const (
	TargetMaster   Target = iota // 走 master 后端
	TargetReadPool                // 走读池（master + replica 按 weight）
	TargetLocal                   // proxy 本地处理，不下发上游
)

// Decision 是 Classify 的输出，供 session 层直接消费。
type Decision struct {
	Class  Class
	Pin    bool   // 是否触发独占升级
	Target Target // 路由目标
}

// 预计算的 Decision，避免每次分配。
var (
	decWrite     = Decision{ClassWrite, false, TargetMaster}
	decRead      = Decision{ClassRead, false, TargetReadPool}
	decBlocking  = Decision{ClassBlocking, true, TargetMaster}
	decTxnPin    = Decision{ClassTxn, true, TargetMaster}   // MULTI / WATCH
	decTxn       = Decision{ClassTxn, false, TargetMaster}   // EXEC / DISCARD / UNWATCH
	decSubscribe = Decision{ClassSubscribe, true, TargetMaster}
	decMaster    = Decision{ClassMasterOnly, false, TargetMaster}
	decAdmin     = Decision{ClassAdmin, false, TargetMaster}
	decDeny      = Decision{ClassDeny, false, TargetLocal}
	decConnLocal = Decision{ClassConnState, false, TargetLocal}  // AUTH / PING / HELLO / ECHO / QUIT / RESET
	decSelect    = Decision{ClassConnState, true, TargetMaster}   // SELECT 触发 pin
)

// Classify 对一条已解析的命令做路由分类。
//
// argv[0] 是命令名，argv[1:] 是参数。命令名和子命令名大小写不敏感，内部统一转大写
// 查表。判定优先级严格按 proxy-command-routing.md §2。
func Classify(argv [][]byte) Decision {
	if len(argv) == 0 {
		return decDeny
	}

	cmd := upper(argv[0])

	// 优先级 1：DENY 名单
	if _, deny := denyCommands[cmd]; deny {
		return decDeny
	}

	// 优先级 2：CONN_STATE（含子命令判定）
	if d, ok := classifyConnState(cmd, argv); ok {
		return d
	}

	// 优先级 2b：子命令依赖分类（MEMORY / OBJECT）
	if cmd == "MEMORY" {
		return classifyMemorySub(argv)
	}
	if cmd == "OBJECT" {
		return classifyObjectSub(argv)
	}

	// 优先级 3：SUBSCRIBE 族
	if _, ok := subscribeCommands[cmd]; ok {
		return decSubscribe
	}

	// 优先级 4：TXN 族
	if d, ok := txnCommands[cmd]; ok {
		return d
	}

	// 优先级 5：参数触发 BLOCKING（XREAD / XREADGROUP 带 BLOCK）
	if cmd == "XREAD" || cmd == "XREADGROUP" {
		if hasBlockArg(argv) {
			return decBlocking
		}
		if cmd == "XREAD" {
			return decRead // XREAD 不带 BLOCK -> READ
		}
		return decMaster // XREADGROUP 不带 BLOCK -> MASTER_ONLY（推进消费组游标，有写副作用）
	}

	// 优先级 6：固有 BLOCKING
	if _, ok := blockingCommands[cmd]; ok {
		return decBlocking
	}

	// 优先级 7：MASTER_ONLY / ADMIN
	if _, ok := masterOnlyCommands[cmd]; ok {
		return decMaster
	}
	if _, ok := adminCommands[cmd]; ok {
		return decAdmin
	}

	// 优先级 8：只读白名单
	if _, ok := readCommands[cmd]; ok {
		return decRead
	}

	// 优先级 9：兜底 WRITE（保守默认安全）
	return decWrite
}

// classifyConnState 处理连接状态命令及其子命令。
// 返回 ok=true 表示已判定，ok=false 表示不是 CONN_STATE 命令。
func classifyConnState(cmd string, argv [][]byte) (Decision, bool) {
	d, ok := connStateCommands[cmd]
	if !ok {
		return Decision{}, false
	}

	// 简单 CONN_STATE 命令（无子命令判定）
	if d.Pin || cmd == "AUTH" || cmd == "HELLO" || cmd == "PING" ||
		cmd == "ECHO" || cmd == "QUIT" || cmd == "RESET" {
		return d, true
	}

	// CLIENT 子命令区分
	if cmd == "CLIENT" {
		return classifyClientSub(argv), true
	}

	return d, true
}

// classifyClientSub 按 CLIENT 子命令分类。
func classifyClientSub(argv [][]byte) Decision {
	if len(argv) < 2 {
		return decDeny // CLIENT 无子命令 -> 拒绝
	}
	sub := upper(argv[1])
	if _, ok := clientLocalSubs[sub]; ok {
		return decConnLocal // GETNAME / SETNAME / ID / INFO / SETINFO
	}
	if sub == "LIST" {
		return decMaster
	}
	// KILL / PAUSE / UNPAUSE / NO-EVICT / NO-TOUCH / TRACKING / TRACKINGINFO / REPLY
	// 及未知子命令 -> DENY
	return decDeny
}

// classifyMemorySub 按 MEMORY 子命令分类（在 table.go 的 classifyMemory 中调用）。
func classifyMemorySub(argv [][]byte) Decision {
	if len(argv) < 2 {
		return decDeny
	}
	sub := upper(argv[1])
	if sub == "USAGE" {
		return decRead
	}
	if _, ok := memoryAdminSubs[sub]; ok {
		return decAdmin // STATS / DOCTOR / PURGE / MALLOC-STATS
	}
	return decDeny
}

// classifyObjectSub 按 OBJECT 子命令分类。
func classifyObjectSub(argv [][]byte) Decision {
	if len(argv) < 2 {
		return decMaster
	}
	sub := upper(argv[1])
	if _, ok := objectReadSubs[sub]; ok {
		return decRead // ENCODING / IDLETIME / FREQ / REFCOUNT
	}
	return decMaster
}

// hasBlockArg 扫描 argv 是否含 BLOCK 参数（大小写不敏感）。
func hasBlockArg(argv [][]byte) bool {
	for i := 1; i < len(argv); i++ {
		if len(argv[i]) == 5 {
			b := argv[i]
			if (b[0] == 'B' || b[0] == 'b') &&
				(b[1] == 'L' || b[1] == 'l') &&
				(b[2] == 'O' || b[2] == 'o') &&
				(b[3] == 'C' || b[3] == 'c') &&
				(b[4] == 'K' || b[4] == 'k') {
				return true
			}
		}
	}
	return false
}

// upper 把 byte slice 转大写，返回 string。避免分配：如果输入已全大写，
// 直接 string(b) 转换（Go 1.23+ 的 string() 对 []byte 不拷贝时是安全的引用）。
// 这里每次都分配一个新 string，但命令名通常很短，性能不是瓶颈。
func upper(b []byte) string {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			out[i] = c - 32
		} else {
			out[i] = c
		}
	}
	return string(out)
}
