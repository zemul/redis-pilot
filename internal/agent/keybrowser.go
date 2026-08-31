package agent

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 受限 K/V 浏览器（Agent 侧）。仅供 Server 的 admin-only 端点调用；直连本机实例的
// redis-cli 执行受限命令。大 value 保护、分页、写白名单都在这一层落地。
// 设计见 KEY_BROWSER_PLAN.md。
const (
	kvStringChunkBytes = 64 * 1024  // string 单页读取字节（GETRANGE 窗口）
	kvResponseByteCap  = 256 * 1024 // 单次响应值累计字节兜底
	kvMaxWriteBytes    = 512 * 1024 // 写入 value 上限
	kvDefaultCount     = 200        // 默认分页条数
	kvMaxCount         = 1000       // 分页条数上限
)

// kvWriteOps 是写操作白名单（Agent 侧防线，Server service 层亦会校验）。
var kvWriteOps = map[string]bool{
	"set": true, "hset": true, "push": true, "sadd": true, "zadd": true, "expire": true,
}

// redisCmdRaw 在 redisCmd 之上强制 --raw 输出：整数/状态回复为裸值、多行元素以 \n 分隔，
// 免去解析 redis-cli 默认的 "N) (integer) x" 编号格式。value 含换行的极端情况会被拆行，
// 属已知限制（受限浏览器可接受，见 KEY_BROWSER_PLAN.md §5）。
func redisCmdRaw(instanceName, password string, args ...string) (string, error) {
	return redisCmd(instanceName, password, append([]string{"--raw"}, args...)...)
}

// kvSplit 把 --raw 输出按行切分；空串返回 nil。
func kvSplit(out string) []string {
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// instanceKeyGet 读一个 key：先 TYPE 分流，再探大小 + 取一页值。meta=true（或 count=0）
// 只返回 type/size/ttl，不拉值。
func (a *Agent) instanceKeyGet(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		fail(c, http.StatusBadRequest, "name is required")
		return
	}
	if err := validateInstanceName(name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	key := c.Query("key")
	if key == "" {
		fail(c, http.StatusBadRequest, "key is required")
		return
	}
	pw := a.getPassword(name)

	meta := c.Query("meta") == "true"
	count := kvDefaultCount
	if v, err := strconv.Atoi(c.Query("count")); err == nil {
		if v == 0 {
			meta = true // count=0 视为只取元信息
		} else if v > 0 {
			count = v
		}
	}
	if count > kvMaxCount {
		count = kvMaxCount
	}
	cursor := c.DefaultQuery("cursor", "0")
	var offset int64
	if v, err := strconv.ParseInt(c.Query("offset"), 10, 64); err == nil && v > 0 {
		offset = v
	}

	typ, err := redisCmdRaw(name, pw, "TYPE", key)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	typ = strings.TrimSpace(typ)
	resp := apitypes.KeyGetResponse{Type: typ, TTL: -2, Items: []apitypes.KVItem{}}
	if typ == "" || typ == "none" {
		resp.Type = "none"
		ok(c, resp)
		return
	}
	if ttlOut, err := redisCmdRaw(name, pw, "TTL", key); err == nil {
		resp.TTL = parseIntFromReply(ttlOut)
	}
	resp.Size = kvKeySize(name, pw, typ, key)

	if meta {
		ok(c, resp)
		return
	}

	switch typ {
	case "string":
		kvReadString(name, pw, key, offset, &resp)
	case "list":
		kvReadList(name, pw, key, offset, count, &resp)
	case "hash":
		kvReadScan(name, pw, "HSCAN", key, cursor, count, &resp)
	case "set":
		kvReadScan(name, pw, "SSCAN", key, cursor, count, &resp)
	case "zset":
		kvReadScan(name, pw, "ZSCAN", key, cursor, count, &resp)
	default:
		fail(c, http.StatusBadRequest, "unsupported type: "+typ)
		return
	}
	ok(c, resp)
}

// kvKeySize 用 O(1) 命令探 key 大小（元素数或字节数）。
func kvKeySize(name, pw, typ, key string) int64 {
	var cmd string
	switch typ {
	case "string":
		cmd = "STRLEN"
	case "hash":
		cmd = "HLEN"
	case "list":
		cmd = "LLEN"
	case "set":
		cmd = "SCARD"
	case "zset":
		cmd = "ZCARD"
	default:
		return 0
	}
	out, err := redisCmdRaw(name, pw, cmd, key)
	if err != nil {
		return 0
	}
	return parseIntFromReply(out)
}

// kvReadString 用 GETRANGE 取一段（默认前 64KB），超出即标记 truncated。
func kvReadString(name, pw, key string, offset int64, resp *apitypes.KeyGetResponse) {
	end := offset + kvStringChunkBytes - 1
	out, err := redisCmdRaw(name, pw, "GETRANGE", key,
		strconv.FormatInt(offset, 10), strconv.FormatInt(end, 10))
	if err != nil {
		return
	}
	resp.Items = []apitypes.KVItem{{Value: out}}
	// 依据 STRLEN 判断是否还有后续分片，避免受输出 TrimSpace 影响。
	if resp.Size > offset+kvStringChunkBytes {
		resp.Truncated = true
	}
}

// kvReadList 用 LRANGE 偏移分页；带绝对下标；命中字节兜底则截断。
func kvReadList(name, pw, key string, offset int64, count int, resp *apitypes.KeyGetResponse) {
	end := offset + int64(count) - 1
	out, err := redisCmdRaw(name, pw, "LRANGE", key,
		strconv.FormatInt(offset, 10), strconv.FormatInt(end, 10))
	if err != nil {
		return
	}
	total := 0
	for i, v := range kvSplit(out) {
		idx := offset + int64(i)
		resp.Items = append(resp.Items, apitypes.KVItem{Index: &idx, Value: v})
		total += len(v)
		if total > kvResponseByteCap {
			resp.Truncated = true
			return
		}
	}
	if offset+int64(len(resp.Items)) < resp.Size {
		resp.Truncated = true
	}
}

// kvReadScan 用 HSCAN/SSCAN/ZSCAN 游标分页。首行是 next_cursor，其后为元素
// （HSCAN/ZSCAN 交替两列，SSCAN 单列）。next_cursor 回到 "0" 表示遍历结束。
func kvReadScan(name, pw, cmd, key, cursor string, count int, resp *apitypes.KeyGetResponse) {
	out, err := redisCmdRaw(name, pw, cmd, key, cursor, "COUNT", strconv.Itoa(count))
	if err != nil {
		return
	}
	lines := kvSplit(out)
	if len(lines) == 0 {
		resp.NextCursor = "0"
		return
	}
	resp.NextCursor = lines[0]
	rest := lines[1:]
	total := 0
	overCap := func(n int) bool {
		total += n
		if total > kvResponseByteCap {
			resp.Truncated = true
			return true
		}
		return false
	}
	switch cmd {
	case "HSCAN":
		for i := 0; i+1 < len(rest); i += 2 {
			resp.Items = append(resp.Items, apitypes.KVItem{Field: rest[i], Value: rest[i+1]})
			if overCap(len(rest[i]) + len(rest[i+1])) {
				return
			}
		}
	case "ZSCAN":
		for i := 0; i+1 < len(rest); i += 2 {
			resp.Items = append(resp.Items, apitypes.KVItem{Member: rest[i], Score: rest[i+1]})
			if overCap(len(rest[i]) + len(rest[i+1])) {
				return
			}
		}
	case "SSCAN":
		for _, m := range rest {
			resp.Items = append(resp.Items, apitypes.KVItem{Member: m})
			if overCap(len(m)) {
				return
			}
		}
	}
}

// instanceKeyWrite 按 op 走白名单写入（set/hset/push/sadd/zadd/expire）。
func (a *Agent) instanceKeyWrite(c *gin.Context) {
	var req apitypes.KeyWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if req.Key == "" {
		fail(c, http.StatusBadRequest, "key is required")
		return
	}
	if !kvWriteOps[req.Op] {
		fail(c, http.StatusBadRequest, "unsupported op: "+req.Op)
		return
	}
	args, err := kvBuildWriteArgs(req.Key, req.Op, req.Args)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	out, err := redisCmdChecked(req.Name, a.getPassword(req.Name), args...)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, apitypes.KeyWriteResponse{OK: true, Result: strings.TrimSpace(out)})
}

// kvBuildWriteArgs 把写请求翻译成 redis-cli 参数（含大小/必填校验）。
func kvBuildWriteArgs(key, op string, a apitypes.KeyWriteArgs) ([]string, error) {
	tooBig := func(v string) error {
		if len(v) > kvMaxWriteBytes {
			return fmt.Errorf("value too large: %d bytes (max %d)", len(v), kvMaxWriteBytes)
		}
		return nil
	}
	switch op {
	case "set":
		if err := tooBig(a.Value); err != nil {
			return nil, err
		}
		return []string{"SET", key, a.Value}, nil
	case "hset":
		if a.Field == "" {
			return nil, fmt.Errorf("field is required for hset")
		}
		if err := tooBig(a.Value); err != nil {
			return nil, err
		}
		return []string{"HSET", key, a.Field, a.Value}, nil
	case "push":
		if err := tooBig(a.Value); err != nil {
			return nil, err
		}
		cmd := "RPUSH"
		if a.Side == "left" {
			cmd = "LPUSH"
		}
		return []string{cmd, key, a.Value}, nil
	case "sadd":
		if a.Member == "" {
			return nil, fmt.Errorf("member is required for sadd")
		}
		if err := tooBig(a.Member); err != nil {
			return nil, err
		}
		return []string{"SADD", key, a.Member}, nil
	case "zadd":
		if a.Member == "" {
			return nil, fmt.Errorf("member is required for zadd")
		}
		if _, err := strconv.ParseFloat(a.Score, 64); err != nil {
			return nil, fmt.Errorf("invalid score: %q", a.Score)
		}
		return []string{"ZADD", key, a.Score, a.Member}, nil
	case "expire":
		if a.TTLSeconds < 0 {
			return []string{"PERSIST", key}, nil
		}
		return []string{"EXPIRE", key, strconv.FormatInt(a.TTLSeconds, 10)}, nil
	}
	return nil, fmt.Errorf("unsupported op: %s", op)
}

// instanceKeyDelete 删除整个 key（DEL）。
func (a *Agent) instanceKeyDelete(c *gin.Context) {
	var req apitypes.KeyDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateInstanceName(req.Name); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if req.Key == "" {
		fail(c, http.StatusBadRequest, "key is required")
		return
	}
	out, err := redisCmdChecked(req.Name, a.getPassword(req.Name), "DEL", req.Key)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, apitypes.KeyWriteResponse{OK: true, Result: strings.TrimSpace(out)})
}
