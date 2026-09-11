package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"strings"
	"sync"
	"time"
)

type sessionControl struct {
	mu       sync.Mutex
	client   net.Conn
	stopping bool
}

func (c *sessionControl) stopReading() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
	c.client.SetReadDeadline(time.Now())
}
func (c *sessionControl) prepareRead(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping {
		return false
	}
	c.client.SetReadDeadline(time.Now().Add(d))
	return true
}
func (c *sessionControl) admit() bool { c.mu.Lock(); defer c.mu.Unlock(); return !c.stopping }

type request struct {
	raw     []byte
	args    [][]byte
	local   []byte
	quit    bool
	started time.Time
}

func (s *Service) session(ctx context.Context, client net.Conn) {
	defer client.Close()
	workspace := s.cfg.workspaceBytes()
	if !s.acquireWorkspace(workspace) {
		s.rejects.WithLabelValues("buffer_budget").Inc()
		return
	}
	defer s.releaseWorkspace(workspace)
	ctl := &sessionControl{client: client}
	s.sessionsMu.Lock()
	if s.draining.Load() {
		s.sessionsMu.Unlock()
		return
	}
	s.sessions[ctl] = struct{}{}
	s.sessionsMu.Unlock()
	s.active.Inc()
	defer s.active.Dec()
	defer func() { s.sessionsMu.Lock(); delete(s.sessions, ctl); s.sessionsMu.Unlock() }()
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(work, func() { client.Close() })
	defer stop()
	r, _ := NewReader(client, s.cfg.Limits)
	input := make(chan request, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer close(input)
		for ctl.prepareRead(s.cfg.ClientIdleTimeout) {
			raw, args, e := r.ReadCommand()
			if e != nil {
				return
			}
			if !ctl.admit() {
				return
			}
			q := request{raw: raw, args: args, started: time.Now()}
			select {
			case input <- q:
			case <-work.Done():
				return
			}
		}
	}()
	defer func() { cancel(); client.Close(); <-readDone }()
	var upstream net.Conn
	var replies *Reader
	defer func() {
		if upstream != nil {
			upstream.Close()
		}
	}()
	authed := s.frontendPassword == ""
	cacheClient := &cacheSession{r: s.cache}
	defer cacheClient.close()
	var pending *request
	send := func(b []byte) bool {
		client.SetWriteDeadline(time.Now().Add(s.cfg.RequestTimeout))
		if writeAll(client, b) != nil {
			s.errors.WithLabelValues("client_write").Inc()
			return false
		}
		return true
	}
	for {
		var first request
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			q, ok := <-input
			if !ok {
				return
			}
			first = q
		}
		batch := []request{first}
		size := len(first.raw)
	collect:
		for len(batch) < s.cfg.MaxPipeline && size < s.cfg.MaxBatchBytes {
			select {
			case q, ok := <-input:
				if !ok {
					break collect
				}
				if size+len(q.raw) > s.cfg.MaxBatchBytes {
					pending = &q
					break collect
				}
				batch = append(batch, q)
				size += len(q.raw)
			default:
				break collect
			}
		}
		quit := false
		for i := range batch {
			cmd := strings.ToUpper(string(batch[i].args[0]))
			q := &batch[i]
			switch {
			case cmd == "AUTH":
				authed = false
				if len(q.args) != 2 {
					q.local = []byte("-ERR wrong number of arguments for AUTH\r\n")
				} else if s.frontendPassword == "" {
					q.local = []byte("-ERR no frontend password configured\r\n")
					authed = true
				} else {
					x := sha256.Sum256(q.args[1])
					y := sha256.Sum256([]byte(s.frontendPassword))
					authed = subtle.ConstantTimeCompare(x[:], y[:]) == 1
					if authed {
						q.local = []byte("+OK\r\n")
					} else {
						q.local = []byte("-WRONGPASS invalid credentials\r\n")
					}
				}
			case cmd == "QUIT" && len(q.args) == 1:
				q.local = []byte("+OK\r\n")
				q.quit = true
				quit = true
			case !authed:
				q.local = []byte("-NOAUTH Authentication required.\r\n")
			case s.cache == nil && !forwardCommands[cmd]:
				q.local = []byte("-ERR command not supported by gateway passthrough\r\n")
			}
			if quit {
				batch = batch[:i+1]
				break
			}
		}
		if s.cache != nil {
			s.batchSize.Observe(float64(len(batch)))
			for _, q := range batch {
				b := q.local
				result := "local"
				if b == nil {
					s.inflight.Inc()
					b = cacheClient.execute(work, q.args)
					s.inflight.Dec()
					result = "cache_mode"
				}
				if !send(b) {
					return
				}
				s.completed.WithLabelValues(result).Inc()
				s.duration.Observe(time.Since(q.started).Seconds())
			}
			if quit {
				return
			}
			continue
		}
		var raws [][]byte
		for _, q := range batch {
			if q.local == nil {
				raws = append(raws, q.raw)
			}
		}
		if len(raws) > 0 && upstream == nil {
			var e error
			upstream, replies, e = s.openBackend(work)
			if e != nil {
				s.backendReady.Store(false)
				s.errors.WithLabelValues("backend_init").Inc()
				for _, q := range batch {
					b := q.local
					if b == nil {
						b = []byte("-ERR backend unavailable; command not sent\r\n")
					}
					if !send(b) {
						return
					}
				}
				return
			}
			stopBackend := context.AfterFunc(work, func() { upstream.Close() })
			defer stopBackend()
		}
		if !s.executeBatch(work, upstream, replies, batch, raws, send) {
			return
		}
		if quit {
			return
		}
	}
}

func (s *Service) executeBatch(ctx context.Context, c net.Conn, r *Reader, batch []request, raws [][]byte, send func([]byte) bool) bool {
	s.inflight.Add(float64(len(batch)))
	defer s.inflight.Sub(float64(len(batch)))
	s.batchSize.Observe(float64(len(batch)))
	if len(raws) == 0 {
		for _, q := range batch {
			if !send(q.local) {
				return false
			}
			s.completed.WithLabelValues("local").Inc()
			s.duration.Observe(time.Since(q.started).Seconds())
		}
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	c.SetDeadline(time.Now().Add(s.cfg.RequestTimeout))
	written := make(chan error, 1)
	// Read replies concurrently: writing an entire large batch before reading
	// could deadlock against Redis blocked on its own output buffer.
	go func() {
		w := bufio.NewWriterSize(c, 32768)
		for _, raw := range raws {
			if _, e := w.Write(raw); e != nil {
				written <- e
				c.Close()
				return
			}
		}
		e := w.Flush()
		written <- e
		if e != nil {
			c.Close()
		}
	}()
	remaining := len(raws)
	ok := false
	defer func() {
		if !ok {
			c.Close()
		}
		e := <-written
		if e != nil {
			s.errors.WithLabelValues("backend_write").Inc()
		}
		if remaining > 0 {
			s.uncertain.Add(float64(remaining))
		}
	}()
	for _, q := range batch {
		b := q.local
		result := "local"
		if b == nil {
			var e error
			b, e = r.ReadFrame()
			if e != nil {
				s.backendReady.Store(false)
				s.errors.WithLabelValues("backend_read").Inc()
				return false
			}
			remaining--
			result = "backend"
		}
		if bytes.HasPrefix(b, []byte("-MOVED ")) || bytes.HasPrefix(b, []byte("-ASK ")) {
			s.backendReady.Store(false)
			send(b)
			return false
		}
		if !send(b) {
			return false
		}
		s.completed.WithLabelValues(result).Inc()
		s.duration.Observe(time.Since(q.started).Seconds())
	}
	ok = true
	return true
}
