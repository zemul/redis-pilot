package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"time"
)

// Frames have already passed Reader's allocation and nesting limits. Children
// point into the same immutable frame, rather than allocating a second copy.
func frameParts(b []byte) ([][]byte, error) {
	if len(b) == 0 || b[0] != '*' {
		return nil, errors.New("expected array")
	}
	i := bytes.Index(b, []byte("\r\n"))
	if i < 0 {
		return nil, errors.New("invalid frame")
	}
	n, err := strconv.Atoi(string(b[1:i]))
	if err != nil || n < 0 || n > 65536 {
		return nil, errors.New("invalid array")
	}
	b = b[i+2:]
	out := make([][]byte, 0, n)
	for j := 0; j < n; j++ {
		end, e := frameEnd(b)
		if e != nil {
			return nil, e
		}
		out = append(out, b[:end])
		b = b[end:]
	}
	if len(b) != 0 {
		return nil, errors.New("trailing frame")
	}
	return out, nil
}

func frameEnd(b []byte) (int, error) {
	i := bytes.Index(b, []byte("\r\n"))
	if i < 1 {
		return 0, errors.New("invalid frame")
	}
	switch b[0] {
	case '+', '-', ':':
		return i + 2, nil
	case '$':
		n, err := strconv.Atoi(string(b[1:i]))
		if err != nil || n < -1 {
			return 0, errors.New("invalid bulk")
		}
		if n == -1 {
			return i + 2, nil
		}
		if n > len(b)-i-4 {
			return 0, errors.New("short bulk")
		}
		return i + 4 + n, nil
	case '*':
		n, err := strconv.Atoi(string(b[1:i]))
		if err != nil || n < -1 {
			return 0, errors.New("invalid array")
		}
		off := i + 2
		for j := 0; j < n; j++ {
			size, e := frameEnd(b[off:])
			if e != nil {
				return 0, e
			}
			off += size
		}
		return off, nil
	}
	return 0, errors.New("invalid frame type")
}

func frameString(b []byte) (string, bool) {
	if len(b) < 3 {
		return "", false
	}
	i := bytes.Index(b, []byte("\r\n"))
	if i < 1 {
		return "", false
	}
	if b[0] == '+' || b[0] == ':' {
		return string(b[1:i]), true
	}
	if b[0] != '$' {
		return "", false
	}
	n, e := strconv.Atoi(string(b[1:i]))
	if e != nil || n < 0 || len(b) != i+n+4 {
		return "", false
	}
	return string(b[i+2 : i+2+n]), true
}

type cacheConn struct {
	net.Conn
	reader *Reader
	cfg    Config
	shard  *cacheShardNode
}

func (c *cacheConn) migrationFrameBudget() int { return c.cfg.Limits.MaxBytes }

func (c *cacheConn) call(ctx context.Context, args ...string) ([]byte, error) {
	if c.shard != nil {
		a := shardAdmissionFrom(ctx)
		if a == nil || !a.live.Load() || a.node != c.shard {
			return nil, errors.New("cold connection has no matching shard admission")
		}
		var err error
		args, err = a.wrap(args)
		if err != nil {
			return nil, err
		}
		if len(encodeCommand(args...)) > c.cfg.Limits.MaxBytes {
			return nil, errors.New("expanded shard command exceeds frame budget")
		}
	}
	deadline := time.Now().Add(c.cfg.RequestTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	c.SetDeadline(deadline)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if err := writeAll(c.Conn, encodeCommand(args...)); err != nil {
		return nil, err
	}
	return c.reader.ReadFrame()
}

func (r *cacheRuntime) connect(ctx context.Context, hot bool) (*cacheConn, error) {
	if !hot && r.shards != nil {
		a := shardAdmissionFrom(ctx)
		if a == nil || a.runtime != r || !a.live.Load() {
			return nil, errors.New("cold shard selection required")
		}
		c, err := r.connectAddress(ctx, false, a.node.node.Address, a.node.password)
		if err == nil {
			c.shard = a.node
		}
		return c, err
	}
	addr, password := r.cfg.ColdAddress, r.coldPassword
	if hot {
		addr, password = r.s.cfg.RedisAddress, r.s.backendPassword
	}
	return r.connectAddress(ctx, hot, addr, password)
}

func (r *cacheRuntime) connectAddress(ctx context.Context, hot bool, addr, password string) (*cacheConn, error) {
	if hot && !r.cfg.enabled() {
		return nil, errors.New("hot cache is disabled")
	}
	if hot {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.HotTimeout)
		defer cancel()
	}
	c, err := (&net.Dialer{Timeout: r.s.cfg.ConnectTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	reader, _ := NewReader(c, r.s.cfg.Limits)
	x := &cacheConn{Conn: c, reader: reader, cfg: r.s.cfg}
	if hot {
		x.cfg.RequestTimeout = r.cfg.HotTimeout
	}
	fail := func() (*cacheConn, error) { c.Close(); return nil, errors.New("cache backend initialization failed") }
	if password != "" {
		b, e := x.call(ctx, "AUTH", password)
		if e != nil || string(b) != "+OK\r\n" {
			return fail()
		}
	}
	b, e := x.call(ctx, "INFO", "all")
	if e != nil || !standaloneMaster(b) {
		return fail()
	}
	if !hot {
		return x, nil
	}
	// A restarted cache must never resurrect a successfully invalidated key.
	// This development mode therefore requires persistence-free hot Redis.
	b, e = x.call(ctx, "CONFIG", "GET", "maxmemory", "maxmemory-policy", "appendonly", "save")
	if e != nil {
		return fail()
	}
	parts, e := frameParts(b)
	if e != nil || len(parts) != 8 {
		return fail()
	}
	values := map[string]string{}
	for i := 0; i < len(parts); i += 2 {
		k, ok := frameString(parts[i])
		v, yes := frameString(parts[i+1])
		if !ok || !yes {
			return fail()
		}
		values[k] = v
	}
	n, e := strconv.ParseInt(values["maxmemory"], 10, 64)
	if e != nil || n <= 0 || values["maxmemory-policy"] != "allkeys-lru" || values["appendonly"] != "no" || values["save"] != "" {
		return fail()
	}
	return x, nil
}
