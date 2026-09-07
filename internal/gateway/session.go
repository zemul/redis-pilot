package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
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

func (s *Service) session(ctx context.Context, client net.Conn) {
	defer client.Close()
	r, _ := NewReader(client, s.cfg.Limits)
	var upstream net.Conn
	var replies *Reader
	defer func() {
		if upstream != nil {
			upstream.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { client.Close() })
	defer stop()
	authed := s.frontendPassword == ""
	send := func(b []byte) bool {
		client.SetWriteDeadline(time.Now().Add(s.cfg.RequestTimeout))
		return writeAll(client, b) == nil
	}
	for !s.draining.Load() {
		client.SetReadDeadline(time.Now().Add(s.cfg.ClientIdleTimeout))
		raw, args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if s.draining.Load() {
			return
		}
		cmd := strings.ToUpper(string(args[0]))
		var local string
		switch {
		case cmd == "AUTH":
			authed = false
			if len(args) != 2 {
				local = "-ERR wrong number of arguments for AUTH\r\n"
			} else if s.frontendPassword == "" {
				local = "-ERR no frontend password configured\r\n"
				authed = true
			} else {
				x := sha256.Sum256(args[1])
				y := sha256.Sum256([]byte(s.frontendPassword))
				authed = subtle.ConstantTimeCompare(x[:], y[:]) == 1
				if authed {
					local = "+OK\r\n"
				} else {
					local = "-WRONGPASS invalid credentials\r\n"
				}
			}
		case cmd == "QUIT" && len(args) == 1:
			send([]byte("+OK\r\n"))
			return
		case !authed:
			local = "-NOAUTH Authentication required.\r\n"
		case !forwardCommands[cmd]:
			local = "-ERR command not supported by gateway passthrough\r\n"
		}
		if local != "" {
			if !send([]byte(local)) {
				return
			}
			continue
		}
		if upstream == nil {
			upstream, replies, err = s.openBackend(ctx)
			if err != nil {
				s.backendReady.Store(false)
				send([]byte("-ERR backend unavailable; command not sent\r\n"))
				return
			}
			stopBackend := context.AfterFunc(ctx, func() { upstream.Close() })
			defer stopBackend()
		}
		// One in-flight request per session: pipelines remain ordered, but are
		// deliberately not batched until G3. Never reconnect/replay this request.
		upstream.SetDeadline(time.Now().Add(s.cfg.RequestTimeout))
		if err = writeAll(upstream, raw); err != nil {
			s.backendReady.Store(false)
			s.uncertain.Inc()
			return
		}
		b, err := replies.ReadFrame()
		if err != nil {
			s.backendReady.Store(false)
			s.uncertain.Inc()
			return
		}
		if bytes.HasPrefix(b, []byte("-MOVED ")) || bytes.HasPrefix(b, []byte("-ASK ")) {
			s.backendReady.Store(false)
			send(b)
			return
		}
		if !send(b) {
			return
		}
	}
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
