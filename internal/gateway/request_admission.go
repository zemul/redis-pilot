package gateway

import (
	"bytes"
	"errors"
	"strings"
)

var errAdmissionCommand = errors.New("command has no safe gateway key specification")
var errAdmissionMetadata = errors.New("reserved gateway metadata key")

// requestKeys covers the entire current passthrough whitelist, not the much
// smaller cold replay whitelist. This extracts keys only; Redis still validates
// option values and executes the original command. Unknown commands fail shut
// so future additions to forwardCommands cannot silently bypass a barrier.
func requestKeys(args [][]byte) ([][]byte, error) {
	if len(args) == 0 {
		return nil, errAdmissionCommand
	}
	n := len(args)
	var keys [][]byte
	switch strings.ToUpper(string(args[0])) {
	case "PING":
		if n != 1 && n != 2 {
			return nil, errAdmissionCommand
		}
	case "ECHO":
		if n != 2 {
			return nil, errAdmissionCommand
		}
	case "GET", "INCR", "DECR", "TYPE", "TTL", "PTTL", "PERSIST":
		if n != 2 {
			return nil, errAdmissionCommand
		}
		keys = args[1:2]
	case "SET", "EXPIRE", "PEXPIRE":
		if n < 3 {
			return nil, errAdmissionCommand
		}
		keys = args[1:2]
	case "APPEND":
		if n != 3 {
			return nil, errAdmissionCommand
		}
		keys = args[1:2]
	case "DEL", "UNLINK", "EXISTS", "MGET":
		if n < 2 {
			return nil, errAdmissionCommand
		}
		keys = args[1:]
	case "MSET":
		if n < 3 || n%2 != 1 {
			return nil, errAdmissionCommand
		}
		for i := 1; i < n; i += 2 {
			keys = append(keys, args[i])
		}
	default:
		return nil, errAdmissionCommand
	}
	for _, key := range keys {
		if bytes.HasPrefix(key, []byte(syncMetadataPrefix)) {
			return nil, errAdmissionMetadata
		}
	}
	return keys, nil
}

func (s *Service) admitRequest(q *request) {
	if s.admission == nil || q.local != nil {
		return
	}
	if s.preparation != nil && !hotPreparationCommand(q.args) {
		q.local = []byte("-ERR command outside hot preparation replay subset\r\n")
		s.rejects.WithLabelValues("preparation_subset").Inc()
		return
	}
	keys, err := requestKeys(q.args)
	if err == nil {
		q.admitted, err = s.admission.enterMany(keys)
	}
	if err != nil {
		// Fixed messages only: never echo keys, values or internal metadata.
		s.rejects.WithLabelValues("key_admission").Inc()
		if errors.Is(err, errAdmissionCommand) || errors.Is(err, errAdmissionMetadata) {
			q.local = []byte("-ERR command not eligible for guarded hot access\r\n")
		} else {
			q.local = []byte("-TRYAGAIN hot key admission unavailable; command not sent\r\n")
		}
	}
}

func settleRequest(q request, known bool) {
	for _, r := range q.admitted {
		r.settle(known)
	}
}

func settleBatch(batch []request, known bool) {
	for _, q := range batch {
		settleRequest(q, known)
	}
}
