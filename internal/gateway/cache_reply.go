package gateway

import (
	"fmt"
	"strconv"
)

// Preserve native RESP2 shapes across the scripting implementations. Lua has
// one false value for both nil bulk and nil array; the public command grammar
// tells us which one to restore. LPOS in Kvrocks returns bulk-string indexes.
func cacheReply(cmd cacheCommand, args [][]byte, b []byte) []byte {
	if string(b) == "*-1\r\n" {
		return b
	}
	if len(args) == 2 && len(b) > 0 && b[0] == '*' && (cmd.name == "SRANDMEMBER" || cmd.name == "HRANDFIELD" || cmd.name == "ZRANDMEMBER") {
		p, e := frameParts(b)
		if e != nil || len(p) > 1 {
			return []byte("-ERR invalid random member response\r\n")
		}
		if len(p) == 0 {
			return []byte("$-1\r\n")
		}
		return p[0]
	}
	if string(b) == "$-1\r\n" {
		if ((cmd.name == "LPOP" || cmd.name == "RPOP") && len(args) == 3) ||
			((cmd.name == "ZRANK" || cmd.name == "ZREVRANK") && len(args) == 4) {
			return []byte("*-1\r\n")
		}
	}
	if len(b) == 0 || b[0] != '*' || (cmd.name != "LPOS" && cmd.name != "ZRANK" && cmd.name != "ZREVRANK") {
		return b
	}
	parts, err := frameParts(b)
	if err != nil {
		return []byte("-ERR invalid cold index response\r\n")
	}
	out := []byte(fmt.Sprintf("*%d\r\n", len(parts)))
	for i, p := range parts {
		if cmd.name == "LPOS" || i == 0 {
			v, ok := frameString(p)
			if _, err := strconv.ParseInt(v, 10, 64); !ok || err != nil {
				return []byte("-ERR invalid cold index response\r\n")
			}
			p = []byte(":" + v + "\r\n")
		}
		out = append(out, p...)
	}
	return out
}
