package gateway

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// Resource limits bound each transfer, never the total object's eligibility.
// Same-key coordination remains held throughout the entire load.
func (r *cacheRuntime) fill(ctx context.Context, cold, hot *cacheConn, key []byte, data string) {
	select {
	case r.loads <- struct{}{}:
		defer func() { <-r.loads }()
	default:
		r.events.WithLabelValues("load_limited").Inc()
		return
	}
	touched, complete := false, false
	defer func() {
		if touched && !complete {
			r.invalidateHot(ctx, hot, data)
		}
	}()
	fail := func() { r.events.WithLabelValues("load_skipped").Inc() }
	expiry, err := r.cacheExpiry(ctx, cold, hot, data)
	if err != nil || expiry == 0 {
		fail()
		return
	}
	b, err := cold.call(ctx, "TYPE", data)
	if err != nil {
		fail()
		return
	}
	kind, ok := frameString(b)
	if !ok {
		fail()
		return
	}
	countCmd, op, scan := "", "", ""
	switch kind {
	case "string":
		countCmd, op = "STRLEN", "SET"
	case "hash":
		countCmd, op, scan = "HLEN", "HSET", "HSCAN"
	case "set":
		countCmd, op, scan = "SCARD", "SADD", "SSCAN"
	case "zset":
		countCmd, op, scan = "ZCARD", "ZADD", "ZSCAN"
	case "list":
		countCmd, op = "LLEN", "RPUSH"
	default:
		fail()
		return
	}
	b, err = cold.call(ctx, countCmd, data)
	if err != nil {
		fail()
		return
	}
	count, ok := frameString(b)
	n, e := strconv.ParseInt(count, 10, 64)
	if !ok || e != nil || n < 0 {
		fail()
		return
	}
	first := true
	publish := func(values []string) bool {
		script := cacheAppendScript
		if first {
			script = cachePublishScript
		}
		args := []string{"EVAL", script, "1", data, op, strconv.FormatInt(expiry, 10)}
		args = append(args, values...)
		touched = true
		reply, e := hot.call(ctx, args...)
		if e != nil || (string(reply) != ":1\r\n" && string(reply) != ":0\r\n") {
			r.hotFailed()
			return false
		}
		if string(reply) != ":1\r\n" {
			return false
		}
		first = false
		return true
	}
	if kind == "string" {
		chunk := int64(r.s.cfg.Limits.MaxBytes / 4)
		if chunk > 65536 {
			chunk = 65536
		}
		if chunk < 1 {
			fail()
			return
		}
		for offset := int64(0); offset < n || (n == 0 && offset == 0); offset += chunk {
			b, err = cold.call(ctx, "GETRANGE", data, strconv.FormatInt(offset, 10), strconv.FormatInt(offset+chunk-1, 10))
			value, valid := frameString(b)
			if err != nil || !valid {
				fail()
				return
			}
			want := n - offset
			if want > chunk {
				want = chunk
			}
			if int64(len(value)) != want {
				fail()
				return
			}
			if !publish([]string{value}) {
				fail()
				return
			}
			op = "APPEND"
			if n == 0 {
				break
			}
		}
	} else if kind == "list" {
		for offset := int64(0); offset < n; offset += 128 {
			b, err = cold.call(ctx, "LRANGE", data, strconv.FormatInt(offset, 10), strconv.FormatInt(offset+127, 10))
			values, e := cacheValues(b)
			if err != nil || e != nil || len(values) == 0 {
				fail()
				return
			}
			if !publish(values) {
				fail()
				return
			}
		}
	} else {
		cursor := "0"
		for {
			b, err = cold.call(ctx, scan, data, cursor, "COUNT", "128")
			parts, e := frameParts(b)
			if err != nil || e != nil || len(parts) != 2 {
				fail()
				return
			}
			cursor, ok = frameString(parts[0])
			if _, e = strconv.ParseUint(cursor, 10, 64); !ok || e != nil {
				fail()
				return
			}
			values, e := cacheValues(parts[1])
			if e != nil {
				fail()
				return
			}
			if kind == "hash" || kind == "zset" {
				if len(values)%2 != 0 {
					fail()
					return
				}
			}
			if kind == "zset" {
				for i := 0; i < len(values); i += 2 {
					values[i], values[i+1] = values[i+1], values[i]
				}
			}
			if len(values) > 0 && !publish(values) {
				fail()
				return
			}
			if cursor == "0" {
				break
			}
		}
	}
	if first {
		fail()
		return
	}
	b, err = hot.call(ctx, "EVAL", cacheVerifyScript, "1", data, countCmd, count)
	if err != nil || (string(b) != ":0\r\n" && string(b) != ":1\r\n") {
		r.hotFailed()
		fail()
		return
	}
	if string(b) != ":1\r\n" {
		fail()
		return
	}
	complete = true
	r.events.WithLabelValues("filled").Inc()
}

func cacheValues(b []byte) ([]string, error) {
	parts, err := frameParts(b)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		v, ok := frameString(part)
		if !ok {
			return nil, errors.New("invalid element")
		}
		values = append(values, v)
	}
	return values, nil
}

func (r *cacheRuntime) invalidateHot(ctx context.Context, hot *cacheConn, key string) {
	b, e := hot.call(ctx, "DEL", key)
	if e != nil || len(b) == 0 || b[0] != ':' {
		r.hotFailed()
		return
	}
	r.events.WithLabelValues("invalidated").Inc()
}

// Return an expiry on the hot server's clock; zero means absent/expired.
func (r *cacheRuntime) cacheExpiry(ctx context.Context, cold, hot *cacheConn, key string) (int64, error) {
	started := time.Now()
	b, err := cold.call(ctx, "PTTL", key)
	if err != nil {
		return 0, err
	}
	v, ok := frameString(b)
	ms, e := strconv.ParseInt(v, 10, 64)
	if !ok || e != nil || ms < -2 {
		return 0, errors.New("invalid PTTL")
	}
	if ms == -2 || ms == 0 {
		return 0, nil
	}
	ttl := r.cfg.TTL
	if ms > 0 && ms < ttl.Milliseconds() {
		ttl = time.Duration(ms) * time.Millisecond
	}
	deadline := started.Add(ttl)
	b, err = hot.call(ctx, "TIME")
	if err != nil {
		return 0, err
	}
	parts, e := frameParts(b)
	if e != nil || len(parts) != 2 {
		return 0, errors.New("invalid TIME")
	}
	secS, _ := frameString(parts[0])
	usS, _ := frameString(parts[1])
	sec, e1 := strconv.ParseInt(secS, 10, 64)
	us, e2 := strconv.ParseInt(usS, 10, 64)
	if e1 != nil || e2 != nil || sec < 0 || sec > 1<<40 || us < 0 || us >= 1000000 {
		return 0, errors.New("invalid TIME")
	}
	remaining := time.Until(deadline).Milliseconds()
	if remaining <= 0 {
		return 0, nil
	}
	return sec*1000 + us/1000 + remaining, nil
}
