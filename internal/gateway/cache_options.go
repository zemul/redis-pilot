package gateway

import "strings"

// These parsers constrain key positions and the supported grammar. Numeric
// values and mutually exclusive conditions are validated by the cold engine.
func cacheFlags(args [][]byte, allowed ...string) bool {
	for _, arg := range args {
		found := false
		for _, flag := range allowed {
			if strings.EqualFold(string(arg), flag) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func cachePairs(args [][]byte, allowed ...string) bool {
	if len(args)%2 != 0 {
		return false
	}
	for i := 0; i < len(args); i += 2 {
		if !cacheFlags(args[i:i+1], allowed...) {
			return false
		}
	}
	return true
}

func cacheRangeOptions(args [][]byte, modern, scores bool) bool {
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "BYSCORE", "BYLEX", "REV":
			if !modern {
				return false
			}
		case "WITHSCORES":
			if !scores {
				return false
			}
		case "LIMIT":
			if i+2 >= len(args) {
				return false
			}
			i += 2
		default:
			return false
		}
	}
	return true
}

func cacheZAddStart(args [][]byte) int {
	i := 2
	for i < len(args) && cacheFlags(args[i:i+1], "NX", "XX", "GT", "LT", "CH", "INCR") {
		i++
	}
	return i
}

func cacheSetGet(args [][]byte) bool {
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "GET":
			return true
		case "EX", "PX", "EXAT", "PXAT":
			i++
		}
	}
	return false
}
