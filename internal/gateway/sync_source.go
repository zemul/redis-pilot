package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// G6a-1: explicitly invoked by isolated tests only, not a service config mode.
// One connection attempt; callers decide when/if to reconnect. Never overwrite
// an existing journal after FULLRESYNC or silently adopt a new history.
type syncSource struct {
	address      string
	passwordFile string
	timeout      time.Duration
	maxSnapshot  int64
}

func syncLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return "", errors.New("invalid or interrupted replication control line")
	}
	if string(line) == "\n" {
		return "", nil
	} // RDB preparation keepalive
	if len(line) < 2 || len(line) > 1024 || line[len(line)-2] != '\r' {
		return "", errors.New("invalid replication control line")
	}
	return string(line[:len(line)-2]), nil
}

// Both isolated journal formats expose the same durable capture contract.
// Implementations must not advance this position before local fsync succeeds.
type syncCaptureLog interface {
	append(syncRecord) error
	durablePosition() syncPosition
}

func (s syncSource) capture(ctx context.Context, j syncCaptureLog) error {
	host, port, err := net.SplitHostPort(s.address)
	p, e := strconv.Atoi(port)
	ip := net.ParseIP(host)
	if err != nil || e != nil || p < 1 || p > 65535 || ip == nil || !ip.IsLoopback() || s.timeout <= 0 || s.timeout > 30*time.Second || s.maxSnapshot < 9 || s.maxSnapshot > 8<<20 {
		return errors.New("replication capture requires bounded settings and an isolated loopback source")
	}
	position := j.durablePosition()
	if position.History != "" && position.Source != s.address {
		return errors.New("replication journal belongs to another source")
	}
	password, err := readPassword(s.passwordFile)
	if err != nil {
		return err
	}
	conn, err := (&net.Dialer{Timeout: s.timeout}).DialContext(ctx, "tcp", s.address)
	if err != nil {
		return errors.New("cannot connect to replication source")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	reader := bufio.NewReaderSize(conn, 4096)
	var writeMu sync.Mutex
	send := func(args ...string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(s.timeout))
		return writeAll(conn, encodeCommand(args...))
	}
	expectOK := func(args ...string) error {
		if err := send(args...); err != nil {
			return err
		}
		conn.SetReadDeadline(time.Now().Add(s.timeout))
		line, err := syncLine(reader)
		if err != nil || line != "+OK" {
			return errors.New("replication handshake rejected")
		}
		return nil
	}
	if password != "" {
		if err = expectOK("AUTH", password); err != nil {
			return err
		}
	}
	if err = expectOK("REPLCONF", "listening-port", "0"); err != nil {
		return err
	}
	// Do not advertise EOF framing: this prototype only accepts length-prefixed RDB.
	if err = expectOK("REPLCONF", "capa", "psync2"); err != nil {
		return err
	}
	id, offset := "?", "-1"
	if position.History != "" {
		id = position.History
		offset = strconv.FormatInt(position.Offset+1, 10)
	}
	if err = send("PSYNC", id, offset); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(s.timeout))
	line, err := syncLine(reader)
	if err != nil {
		return err
	}
	fields := strings.Fields(line)
	switch {
	case len(fields) == 3 && fields[0] == "+FULLRESYNC":
		if position.History != "" {
			return errSyncHistoryGap
		}
		base, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || base < 0 || !validSyncHistory(fields[1]) {
			return errors.New("invalid FULLRESYNC identity")
		}
		for {
			conn.SetReadDeadline(time.Now().Add(s.timeout))
			line, err = syncLine(reader)
			if err != nil {
				return err
			}
			if line != "" {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if !strings.HasPrefix(line, "$") {
			return errors.New("expected length-prefixed RDB")
		}
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil || n < 9 || n > s.maxSnapshot {
			return errors.New("RDB exceeds capture limit or uses unsupported framing")
		}
		rdb := make([]byte, int(n))
		conn.SetReadDeadline(time.Now().Add(s.timeout))
		if _, err = io.ReadFull(reader, rdb); err != nil {
			return errors.New("incomplete replication snapshot")
		}
		position = syncPosition{Source: s.address, History: fields[1], Offset: base}
		if err = j.append(syncRecord{Kind: "snapshot", Position: position, Data: rdb}); err != nil {
			return err
		}
	case (len(fields) == 1 || len(fields) == 2) && fields[0] == "+CONTINUE":
		if position.History == "" || (len(fields) == 2 && fields[1] != position.History) {
			return errSyncHistoryGap
		}
	default:
		return errors.New("PSYNC rejected or unsupported response")
	}
	// ACK means captured-to-local-disk only, NOT applied or durable in Kvrocks.
	ack := func() error { return send("REPLCONF", "ACK", strconv.FormatInt(j.durablePosition().Offset, 10)) }
	if err = ack(); err != nil {
		return err
	}
	ackCtx, cancel := context.WithCancel(ctx)
	ackDone := make(chan struct{})
	defer func() { cancel(); conn.Close(); <-ackDone }()
	go func() {
		defer close(ackDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ackCtx.Done():
				return
			case <-ticker.C:
				if ack() != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	frames, _ := NewReader(reader, Limits{MaxBytes: 1 << 20, MaxElements: 4096, MaxDepth: 4})
	for {
		conn.SetReadDeadline(time.Now().Add(s.timeout))
		raw, args, err := frames.ReadCommand()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("replication stream interrupted or invalid frame")
		}
		position.Offset += int64(len(raw))
		if err = j.append(syncRecord{Kind: "command", Position: position, Data: raw}); err != nil {
			return err
		}
		if len(args) == 3 && strings.EqualFold(string(args[0]), "REPLCONF") && strings.EqualFold(string(args[1]), "GETACK") {
			if err = ack(); err != nil {
				return err
			}
		}
	}
}
