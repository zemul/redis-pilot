// 静态命令分类表。按 proxy-command-routing.md §3-§10 逐条录入。
// 命令名一律大写归一，Classify 调用前会做 upper() 转换。
//
// 不在此表内的命令走 §2 兜底规则 -> WRITE（保守默认安全）。
// 只有明确在只读白名单（readCommands）里的才判 READ。

package route

// ── CONN_STATE（§5）：proxy 终结或 pin ──

var connStateCommands = map[string]Decision{
	"AUTH":   decConnLocal, // proxy 终结（D7）
	"SELECT": decSelect,    // 触发 pin，永久 EXCLUSIVE（§5.8）
	"HELLO":  decConnLocal, // 不应答 RESP3（D8）
	"PING":   decConnLocal, // proxy 本地回 PONG
	"ECHO":   decConnLocal, // proxy 本地回显
	"QUIT":   decConnLocal, // proxy 关闭该客户端连接
	"RESET":  decConnLocal, // 重置会话状态
	"CLUSTER": decConnLocal, // SLOTS/NODES/INFO 本地应答（§5.9）
	"CLIENT": {},           // 子命令区分，见 classifyClientSub
}

// CLIENT 子命令
var clientLocalSubs = map[string]struct{}{
	"GETNAME": {},
	"SETNAME": {},
	"ID":      {},
	"INFO":    {},
	"SETINFO": {},
}

// ── DENY（§8）：直接拒绝 ──

var denyCommands = map[string]struct{}{
	"MONITOR":   {},
	"FLUSHALL":  {},
	"FLUSHDB":   {},
	"KEYS":      {},
	"SWAPDB":    {},
	"SHUTDOWN":  {},
	"DEBUG":     {},
	"REPLICAOF": {},
	"SLAVEOF":   {},
	"PSYNC":     {},
	"REPLCONF":  {},
	"SYNC":      {},
	"FAILOVER":  {},
	"ACL":       {}, // v1 不支持 ACL（D7）
	"LATENCY":   {},
}

// ── SUBSCRIBE（§7）：pin 独占 + 走 master ──

var subscribeCommands = map[string]struct{}{
	"SUBSCRIBE":   {},
	"PSUBSCRIBE":   {},
	"SSUBSCRIBE":   {},
	"UNSUBSCRIBE":  {},
	"PUNSUBSCRIBE": {},
	"SUNSUBSCRIBE": {},
}

// ── TXN（§6）：pin 独占 + 走 master ──
// MULTI / WATCH 触发 pin；EXEC / DISCARD / UNWATCH 不触发（session 层负责解 pin）。

var txnCommands = map[string]Decision{
	"MULTI":    decTxnPin,
	"EXEC":     decTxn,
	"DISCARD":  decTxn,
	"WATCH":    decTxnPin, // 乐观锁依赖连接状态，必须与后续 MULTI 同连接
	"UNWATCH":  decTxn,
}

// ── 固有 BLOCKING（§4.2）：pin + 走 master ──

var blockingCommands = map[string]struct{}{
	"BLPOP":    {},
	"BRPOP":    {},
	"BLMOVE":   {},
	"BRPOPLPUSH": {},
	"BLMPOP":   {},
	"BZPOPMIN": {},
	"BZPOPMAX": {},
	"BZMPOP":   {},
	"WAIT":     {},
	"WAITAOF":  {},
}

// ── MASTER_ONLY（§9）：固定落主，不 pin ──

var masterOnlyCommands = map[string]struct{}{
	// 脚本 / 函数
	"EVAL":      {},
	"EVALSHA":   {},
	"EVAL_RO":   {},
	"EVALSHA_RO": {},
	"FCALL":     {},
	"FCALL_RO":  {},
	"SCRIPT":    {},
	"FUNCTION":  {},
	// 游标一致性
	"SCAN": {},
	// 信息
	"INFO":    {},
	"DBSIZE":  {},
	"LASTSAVE": {},
	"ROLE":    {},
	// Pub/Sub 发布
	"PUBLISH":   {},
	"SPUBLISH":  {},
	"PUBSUB":    {},
	// 命令发现
	"COMMAND": {},
	// 注：COMMAND DOCS / COMMAND INFO / COMMAND LIST 在 argv 里是两条命令，
	// 需要子命令处理。这里 COMMAND 本体归 MASTER_ONLY，子命令在 Classify 里判定。
}

// ── ADMIN（管理/调试类）：走 master ──
// MEMORY 的子命令在这里处理。MEMORY USAGE 归 READ（见 classifyMemorySub）。

var adminCommands = map[string]struct{}{}

// MEMORY 管理类子命令
var memoryAdminSubs = map[string]struct{}{
	"STATS":        {},
	"DOCTOR":       {},
	"PURGE":        {},
	"MALLOC-STATS": {},
}

// ── READ（§3）：只读白名单 ──

var readCommands = map[string]struct{}{
	// String
	"GET": {}, "MGET": {}, "GETRANGE": {}, "STRLEN": {}, "SUBSTR": {},
	"EXISTS": {}, "TYPE": {}, "TTL": {}, "PTTL": {},
	"EXPIRETIME": {}, "PEXPIRETIME": {}, "DUMP": {},
	// Hash
	"HGET": {}, "HMGET": {}, "HGETALL": {}, "HKEYS": {}, "HVALS": {},
	"HLEN": {}, "HEXISTS": {}, "HSTRLEN": {}, "HRANDFIELD": {}, "HSCAN": {},
	// List
	"LLEN": {}, "LINDEX": {}, "LRANGE": {}, "LPOS": {},
	// Set
	"SCARD": {}, "SISMEMBER": {}, "SMISMEMBER": {}, "SMEMBERS": {}, "SRANDMEMBER": {},
	"SINTER": {}, "SUNION": {}, "SDIFF": {}, "SINTERCARD": {}, "SSCAN": {},
	// Sorted Set
	"ZCARD": {}, "ZSCORE": {}, "ZMSCORE": {}, "ZCOUNT": {}, "ZLEXCOUNT": {},
	"ZRANK": {}, "ZREVRANK": {}, "ZRANGE": {}, "ZREVRANGE": {},
	"ZRANGEBYSCORE": {}, "ZREVRANGEBYSCORE": {}, "ZRANGEBYLEX": {}, "ZREVRANGEBYLEX": {},
	"ZRANDMEMBER": {}, "ZDIFF": {}, "ZINTER": {}, "ZUNION": {}, "ZINTERCARD": {}, "ZSCAN": {},
	// Bitmap
	"GETBIT": {}, "BITCOUNT": {}, "BITPOS": {}, "BITFIELD_RO": {},
	// Geo
	"GEOPOS": {}, "GEODIST": {}, "GEOHASH": {}, "GEOSEARCH": {},
	"GEORADIUS_RO": {}, "GEORADIUSBYMEMBER_RO": {},
	// 通用
	"RANDOMKEY": {},
	// Stream（不带 BLOCK 的 XREAD 归 READ，但 XREAD 在 Classify 里单独处理，不在静态表）
	"XRANGE": {}, "XREVRANGE": {}, "XLEN": {}, "XINFO": {}, "XPENDING": {},
}

// OBJECT 只读子命令
var objectReadSubs = map[string]struct{}{
	"ENCODING":  {},
	"IDLETIME":  {},
	"FREQ":      {},
	"REFCOUNT":  {},
}

// ── WRITE（§3）：走 master ──
// 兜底规则保证：不在上述任何表里的命令都走 WRITE，所以 WRITE 不需要显式表。
// 但把已知的 WRITE 命令列出来便于审计和阅读。这张表不参与判定逻辑（兜底
// 已经覆盖），仅作文档参考。
//
// 完整 WRITE 清单见 proxy-command-routing.md §3。以下为示例：
// SET SETNX SETEX PSETEX SETRANGE APPEND GETSET GETDEL GETEX
// MSET MSETNX DEL UNLINK RENAME RENAMENX MOVE COPY RESTORE
// EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
// INCR INCRBY INCRBYFLOAT DECR DECRBY
// HSET HSETNX HMSET HDEL HINCRBY HINCRBYFLOAT
// LPUSH LPUSHX RPUSH RPUSHX LPOP RPOP LSET LINSERT LREM LTRIM RPOPLPUSH LMOVE LMPOP
// SADD SREM SPOP SMOVE SINTERSTORE SUNIONSTORE SDIFFSTORE
// ZADD ZINCRBY ZREM ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX
// ZPOPMIN ZPOPMAX ZMPOP ZRANGESTORE ZDIFFSTORE ZINTERSTORE ZUNIONSTORE
// SETBIT BITOP BITFIELD
// PFADD PFMERGE PFCOUNT
// GEOADD GEOSEARCHSTORE GEORADIUS GEORADIUSBYMEMBER
// SORT
// XADD XACK XDEL XTRIM XSETID XCLAIM XAUTOCLAIM
// XGROUP (all subcommands)
// OBJECT (不在 readCommands 子命令里的部分 -> 兜底 WRITE)
