package gateway

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// Limits bound allocations before reading untrusted lengths. Elements are a
// total budget across a frame, not merely a per-array limit.
type Limits struct {
	MaxBytes    int `yaml:"max_bytes"`
	MaxElements int `yaml:"max_elements"`
	MaxDepth    int `yaml:"max_depth"`
}

func (l Limits) Validate() error {
	if l.MaxBytes < 16 || l.MaxBytes > 64<<20 || l.MaxElements < 1 || l.MaxElements > 65536 || l.MaxDepth < 1 || l.MaxDepth > 64 {
		return errors.New("invalid RESP limits (bytes 16..64MiB, elements 1..65536, depth 1..64)")
	}
	return nil
}

// Reader is an isolated, bounded RESP2 decoder for the new gateway. Existing
// proxy parsing behavior is intentionally unchanged. Do not reuse after error.
type Reader struct {
	r        *bufio.Reader
	limits   Limits
	raw      []byte
	elements int
}

func NewReader(r io.Reader, limits Limits) (*Reader, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Reader{r: bufio.NewReaderSize(r, 4096), limits: limits}, nil
}

func (r *Reader) reset() { r.raw = nil; r.elements = 0 }
func (r *Reader) take() (byte, error) {
	if len(r.raw) >= r.limits.MaxBytes {
		return 0, errors.New("RESP frame exceeds byte limit")
	}
	b, err := r.r.ReadByte()
	if err == nil {
		r.raw = append(r.raw, b)
	}
	return b, err
}
func (r *Reader) line() ([]byte, error) {
	start := len(r.raw)
	for {
		b, err := r.take()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return nil, errors.New("invalid RESP line ending")
		}
		if b == '\r' {
			b, err = r.take()
			if err != nil {
				return nil, err
			}
			if b != '\n' {
				return nil, errors.New("invalid RESP CRLF")
			}
			return r.raw[start : len(r.raw)-2], nil
		}
	}
}
func number(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty RESP integer")
	}
	for i, c := range b {
		if c < '0' || c > '9' {
			if i != 0 || c != '-' || len(b) == 1 {
				return 0, errors.New("invalid RESP integer")
			}
		}
	}
	return strconv.ParseInt(string(b), 10, 64)
}
func (r *Reader) count() (int64, error) {
	b, e := r.line()
	if e != nil {
		return 0, e
	}
	return number(b)
}
func (r *Reader) bulk(command bool) ([]byte, error) {
	n, err := r.count()
	if err != nil {
		return nil, err
	}
	if n == -1 && !command {
		return nil, nil
	}
	remaining := r.limits.MaxBytes - len(r.raw)
	if n < 0 || remaining < 2 || n > int64(remaining-2) {
		return nil, errors.New("invalid or oversized RESP bulk")
	}
	start := len(r.raw)
	r.raw = append(r.raw, make([]byte, int(n)+2)...)
	if _, err = io.ReadFull(r.r, r.raw[start:]); err != nil {
		return nil, err
	}
	end := start + int(n)
	if r.raw[end] != '\r' || r.raw[end+1] != '\n' {
		return nil, errors.New("invalid bulk CRLF")
	}
	return r.raw[start:end], nil
}
func (r *Reader) arrayCount(command bool) (int, error) {
	n, err := r.count()
	if err != nil {
		return 0, err
	}
	if n == -1 && !command {
		return 0, nil
	}
	if n < 0 || (command && n == 0) || n > int64(r.limits.MaxElements-r.elements) {
		return 0, errors.New("invalid or oversized RESP array")
	}
	r.elements += int(n)
	return int(n), nil
}

// ReadCommand accepts only a non-empty array of non-null bulk strings.
func (r *Reader) ReadCommand() ([]byte, [][]byte, error) {
	r.reset()
	b, err := r.take()
	if err != nil {
		return nil, nil, err
	}
	if b != '*' {
		return nil, nil, errors.New("expected RESP command array")
	}
	n, err := r.arrayCount(true)
	if err != nil {
		return nil, nil, err
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		b, err = r.take()
		if err != nil {
			return nil, nil, err
		}
		if b != '$' {
			return nil, nil, errors.New("expected bulk argument")
		}
		arg, e := r.bulk(true)
		if e != nil {
			return nil, nil, e
		}
		args = append(args, arg)
	}
	if len(args[0]) == 0 {
		return nil, nil, errors.New("empty command name")
	}
	return r.raw, args, nil
}

func (r *Reader) ReadFrame() ([]byte, error) {
	r.reset()
	if err := r.frame(1); err != nil {
		return nil, err
	}
	return r.raw, nil
}
func (r *Reader) frame(depth int) error {
	if depth > r.limits.MaxDepth {
		return errors.New("RESP nesting exceeds depth limit")
	}
	b, err := r.take()
	if err != nil {
		return err
	}
	switch b {
	case '+', '-':
		_, err = r.line()
	case ':':
		_, err = r.count()
	case '$':
		_, err = r.bulk(false)
	case '*':
		var n int
		n, err = r.arrayCount(false)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err = r.frame(depth + 1); err != nil {
				return err
			}
		}
	default:
		err = errors.New("unsupported RESP type")
	}
	return err
}
