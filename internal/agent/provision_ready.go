package agent

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
)

func (a *Agent) instanceVerifyProfile(c *gin.Context) {
	var req ensureProfileRequest
	if c.ShouldBindJSON(&req) != nil || validateInstanceName(req.Name) != nil || validateInstanceName(req.OperationID) != nil || req.Port < 1 || req.Port > 65535 {
		fail(c, 400, "invalid profile verification request")
		return
	}
	if req.Profile == "" || redisconf.ValidateProfile(req.Profile, req.Engine, req.Category, req.Type, req.ReplicaOf, req.ConfigOverrides) != nil {
		fail(c, 400, "invalid dedicated profile")
		return
	}
	unlock := a.lockInstance(req.Name)
	defer unlock()
	id, err := a.ensureProfileAttempt(req, false)
	if err != nil {
		fail(c, 409, err.Error())
		return
	}
	if err := probeProvisionDatabase(net.JoinHostPort("127.0.0.1", strconv.Itoa(req.Port)), req.Profile); err != nil {
		fail(c, 503, err.Error())
		return
	}
	// Recheck identity after probing, without creating/restarting anything.
	after, err := a.ensureProfileAttempt(req, false)
	if err != nil || after != id {
		fail(c, 409, "container identity changed during database verification")
		return
	}
	ok(c, gin.H{"operation_id": req.OperationID, "container_id": id, "database_ready": true, "empty": true})
}

// Deliberately read-only: do not flush, repair or inject a test key into a
// database whose ownership/readiness has not yet been accepted by Server.
func probeProvisionDatabase(addr, profile string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	r := bufio.NewReader(io.LimitReader(conn, 128<<10))
	call := func(args ...string) (any, error) {
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, v := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(v), v)
		}
		if _, err := io.WriteString(conn, b.String()); err != nil {
			return nil, err
		}
		return readProvisionReply(r, 0)
	}
	pong, err := call("PING")
	if err != nil || pong != "PONG" {
		return fmt.Errorf("database PING did not confirm readiness: %v", err)
	}
	info, err := call("INFO")
	if err != nil {
		return fmt.Errorf("database INFO: %w", err)
	}
	text, ok := info.(string)
	if !ok {
		return fmt.Errorf("invalid INFO response")
	}
	fields := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		k, v, ok := strings.Cut(strings.TrimSuffix(line, "\r"), ":")
		if ok {
			fields[k] = v
		}
	}
	if fields["role"] != "master" || fields["cluster_enabled"] != "0" || (fields["loading"] != "" && fields["loading"] != "0") {
		return fmt.Errorf("database is loading, not standalone, or not master")
	}
	if profile == redisconf.KvrocksStorageProfile {
		if fields["kvrocks_version"] == "" {
			return fmt.Errorf("expected Kvrocks engine")
		}
	} else if fields["redis_version"] == "" || fields["kvrocks_version"] != "" {
		return fmt.Errorf("expected Redis engine")
	}
	wants := map[string]string{"rocksdb.write_options.sync": "yes", "rocksdb.write_options.disable_wal": "no", "workers": "2", "max-db-size": "0"}
	if profile == redisconf.RedisCacheProfile {
		wants = map[string]string{"save": "", "appendonly": "no", "maxmemory-policy": "allkeys-lru", "maxmemory": "positive"}
	}
	for key, want := range wants {
		v, err := call("CONFIG", "GET", key)
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		pair, ok := v.([]any)
		if !ok || len(pair) != 2 || pair[0] != key {
			return fmt.Errorf("missing runtime config %s", key)
		}
		got, ok := pair[1].(string)
		if !ok {
			return fmt.Errorf("invalid runtime config %s", key)
		}
		if want == "positive" {
			n, err := strconv.ParseInt(got, 10, 64)
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid runtime maxmemory")
			}
		} else if got != want {
			return fmt.Errorf("runtime config %s does not match dedicated profile", key)
		}
	}
	size, err := call("DBSIZE")
	if err != nil || size != int64(0) {
		return fmt.Errorf("dedicated database is not confirmed empty: %v", err)
	}
	for key, value := range fields {
		if strings.HasPrefix(key, "db") && strings.Contains(value, "keys=") && !strings.HasPrefix(value, "keys=0,") {
			return fmt.Errorf("database keyspace is not empty")
		}
	}
	return nil
}

// Small bounded RESP2 decoder for fixed administrative replies only.
func readProvisionReply(r *bufio.Reader, depth int) (any, error) {
	if depth > 1 {
		return nil, fmt.Errorf("nested reply exceeds limit")
	}
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("invalid RESP line")
	}
	kind := line[0]
	value := string(line[1 : len(line)-2])
	switch kind {
	case '+':
		return value, nil
	case '-':
		return nil, fmt.Errorf("database rejected probe: %s", value)
	case ':':
		return strconv.ParseInt(value, 10, 64)
	case '$':
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > 64<<10 {
			return nil, fmt.Errorf("invalid bulk length")
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		if string(b[n:]) != "\r\n" {
			return nil, fmt.Errorf("invalid bulk ending")
		}
		return string(b[:n]), nil
	case '*':
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > 8 {
			return nil, fmt.Errorf("invalid array length")
		}
		out := make([]any, n)
		for i := range out {
			out[i], err = readProvisionReply(r, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported RESP reply")
}
