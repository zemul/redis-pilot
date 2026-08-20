package agent

import (
	"regexp"
	"strconv"
	"strings"
)

// SlowlogEntry 表示一条慢查询记录。
type SlowlogEntry struct {
	ID        int64  `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Duration  int64  `json:"duration_us"` // 微秒
	Command   string `json:"command"`
	Client    string `json:"client,omitempty"`
}

// parseSlowlog 解析 redis-cli SLOWLOG GET 的原始文本输出。
// redis-cli 输出格式:
//
//	 1) 1) (integer) 14
//	    2) (integer) 1692851234
//	    3) (integer) 11451
//	    4) 1) "GET"
//	       2) "mykey"
//	    5) "127.0.0.1:53210"
//	    6) ""
func parseSlowlog(raw string) []SlowlogEntry {
	if raw == "" || strings.HasPrefix(raw, "(empty") {
		return []SlowlogEntry{}
	}
	lines := strings.Split(raw, "\n")
	var entries []SlowlogEntry
	var cur *SlowlogEntry
	var inCmd bool
	var cmdParts []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		// Top level entry starts with N) where N is the entry number in outer list
		// Inner fields start with N) where the field number tells us what it is
		// Detect patterns like "1) (integer) 14"
		if matches := reEntryField.FindStringSubmatch(trimmed); matches != nil {
			fieldNum, _ := strconv.Atoi(matches[1])
			value := matches[2]

			switch fieldNum {
			case 1:
				// Could be outer entry marker or field 1 (id)
				if strings.HasPrefix(value, "1)") || strings.Contains(value, "(integer)") {
					// This is field 1 = ID
					if cur != nil {
						if inCmd {
							cur.Command = strings.Join(cmdParts, " ")
							inCmd = false
						}
						entries = append(entries, *cur)
					}
					cur = &SlowlogEntry{}
					cmdParts = nil
					cur.ID = parseInteger(value)
				} else if cur != nil && inCmd {
					cmdParts = append(cmdParts, stripQuotes(value))
				}
			case 2:
				if cur != nil && !inCmd {
					cur.Timestamp = parseInteger(value)
				} else if cur != nil && inCmd {
					cmdParts = append(cmdParts, stripQuotes(value))
				}
			case 3:
				if cur != nil && !inCmd {
					cur.Duration = parseInteger(value)
				} else if cur != nil && inCmd {
					cmdParts = append(cmdParts, stripQuotes(value))
				}
			case 4:
				if cur != nil && !inCmd {
					// Start of command array
					inCmd = true
					cmdParts = nil
					// value might contain first element: 1) "GET"
					if p := extractCmdPart(value); p != "" {
						cmdParts = append(cmdParts, p)
					}
				} else if cur != nil && inCmd {
					cmdParts = append(cmdParts, stripQuotes(value))
				}
			case 5:
				if cur != nil && inCmd {
					cur.Command = strings.Join(cmdParts, " ")
					inCmd = false
				}
				if cur != nil {
					cur.Client = stripQuotes(value)
				}
			case 6:
				// client name, ignore
			default:
				if cur != nil && inCmd {
					cmdParts = append(cmdParts, stripQuotes(value))
				}
			}
		} else if cur != nil && inCmd {
			// Continuation of command array
			if p := extractCmdPart(trimmed); p != "" {
				cmdParts = append(cmdParts, p)
			}
		}
	}
	if cur != nil {
		if inCmd {
			cur.Command = strings.Join(cmdParts, " ")
		}
		entries = append(entries, *cur)
	}
	return entries
}

var reEntryField = regexp.MustCompile(`^(\d+)\)\s+(.*)$`)

func parseInteger(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(integer) ")
	// Also handle nested: "1) (integer) 14" -> just get the number
	if idx := strings.LastIndex(s, " "); idx >= 0 {
		candidate := s[idx+1:]
		if n, err := strconv.ParseInt(candidate, 10, 64); err == nil {
			return n
		}
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func extractCmdPart(s string) string {
	// "1) \"GET\"" or just "\"GET\""
	if m := reEntryField.FindStringSubmatch(s); m != nil {
		return stripQuotes(m[2])
	}
	return stripQuotes(s)
}

// BigkeyEntry 表示一个大 Key。
type BigkeyEntry struct {
	Key      string `json:"key"`
	Type     string `json:"type"`
	Size     int64  `json:"size"`
	SizeDesc string `json:"size_desc"` // e.g. "1024 bytes", "256 elements"
}

// parseBigkeys 解析 redis-cli --bigkeys 的输出。
// 输出中每个大key形如:
// [type] Biggest string key found '"session:abc"' has 1024 bytes
// [type] Biggest list key found '"mylist"' has 256 items
// 另一种格式 (summary section):
// Biggest string found '"session:abc"' has 1024 bytes
func parseBigkeys(raw string) []BigkeyEntry {
	var entries []BigkeyEntry
	// Match lines like:
	// Biggest string found 'keyname' has 1024 bytes
	// Biggest list found 'keyname' has 256 items
	// Also: Biggest string key found 'keyname' has ...
	re := regexp.MustCompile(`Biggest\s+(\w+)\s+(?:key\s+)?found\s+'([^']+)'\s+has\s+(\d+)\s+(\w+)`)
	for _, line := range strings.Split(raw, "\n") {
		m := re.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		size, _ := strconv.ParseInt(m[3], 10, 64)
		entry := BigkeyEntry{
			Key:      strings.Trim(m[2], "\""),
			Type:     m[1],
			Size:     size,
			SizeDesc: m[3] + " " + m[4],
		}
		entries = append(entries, entry)
	}
	return entries
}

// parseIntFromReply 从 redis-cli 的输出中提取整数值。
// redis-cli 对 integer 回复输出 "(integer) 1234"。
func parseIntFromReply(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(integer) ")
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
