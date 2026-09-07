package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func readPassword(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot open password file")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(b) > 4096 {
		return "", errors.New("cannot read password file or password too long")
	}
	p := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if p == "" || strings.ContainsAny(p, "\r\n") {
		return "", errors.New("password file must contain a non-empty single line")
	}
	return p, nil
}
func encodeCommand(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n", len(a))
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}
func writeAll(c net.Conn, b []byte) error {
	for len(b) > 0 {
		n, e := c.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

var forwardCommands = map[string]bool{
	"PING": true, "ECHO": true, "GET": true, "SET": true, "INCR": true, "DECR": true, "APPEND": true,
	"DEL": true, "UNLINK": true, "EXISTS": true, "TYPE": true, "TTL": true, "PTTL": true,
	"EXPIRE": true, "PEXPIRE": true, "PERSIST": true, "MGET": true, "MSET": true,
}

// openBackend authenticates using internal credentials, never client AUTH.
// INFO is on this dedicated connection before any business requests.
func (s *Service) openBackend(ctx context.Context) (net.Conn, *Reader, error) {
	d := net.Dialer{Timeout: s.cfg.ConnectTimeout}
	c, e := d.DialContext(ctx, "tcp", s.cfg.RedisAddress)
	if e != nil {
		return nil, nil, e
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	c.SetDeadline(time.Now().Add(s.cfg.RequestTimeout))
	r, _ := NewReader(c, s.cfg.Limits)
	fail := func() (net.Conn, *Reader, error) {
		c.Close()
		return nil, nil, errors.New("backend initialization failed")
	}
	if s.backendPassword != "" {
		if writeAll(c, encodeCommand("AUTH", s.backendPassword)) != nil {
			return fail()
		}
		b, err := r.ReadFrame()
		if err != nil || string(b) != "+OK\r\n" {
			return fail()
		}
	}
	if writeAll(c, encodeCommand("INFO", "all")) != nil {
		return fail()
	}
	b, err := r.ReadFrame()
	if err != nil || !standaloneMaster(b) {
		return fail()
	}
	c.SetDeadline(time.Time{})
	return c, r, nil
}
func standaloneMaster(raw []byte) bool {
	if len(raw) < 6 || raw[0] != '$' {
		return false
	}
	i := bytes.Index(raw, []byte("\r\n"))
	if i < 0 {
		return false
	}
	n, e := strconv.Atoi(string(raw[1:i]))
	if e != nil || n < 0 || n != len(raw)-i-4 {
		return false
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(raw[i+2:i+2+n]), "\r\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok {
			fields[k] = v
		}
	}
	return fields["role"] == "master" && fields["cluster_enabled"] == "0" && fields["redis_version"] != ""
}

func (s *Service) probe(ctx context.Context) {
	for {
		c, _, e := s.openBackend(ctx)
		if c != nil {
			c.Close()
		}
		s.backendReady.Store(e == nil)
		t := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}
