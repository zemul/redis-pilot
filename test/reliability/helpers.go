package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

// redisConn wraps a TCP connection with RESP read/write.
type redisConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dial(address, pw string) (*redisConn, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, err
	}
	rc := &redisConn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 65536),
		w:    bufio.NewWriterSize(conn, 65536),
	}
	if pw != "" {
		if err := rc.Do("AUTH", pw); err != nil {
			conn.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return rc, nil
}

func (c *redisConn) Close() { c.conn.Close() }

// Do sends a command and reads one response. Returns error if response is "-".
func (c *redisConn) Do(args ...string) error {
	c.writeCmd(args...)
	c.w.Flush()
	resp, err := c.readResp()
	if err != nil {
		return err
	}
	if len(resp) > 0 && resp[0] == '-' {
		return fmt.Errorf("redis: %s", resp[1:])
	}
	return nil
}

// DoGet sends a command and returns the string value.
func (c *redisConn) DoGet(args ...string) (string, error) {
	c.writeCmd(args...)
	c.w.Flush()
	return c.readResp()
}

// DoGetMulti sends a command and reads N responses (for pipeline).
func (c *redisConn) SendCmd(args ...string) {
	c.writeCmd(args...)
}

func (c *redisConn) Flush() error {
	return c.w.Flush()
}

func (c *redisConn) ReadResp() (string, error) {
	return c.readResp()
}

func (c *redisConn) SetDeadline(t time.Time) {
	c.conn.SetDeadline(t)
}

func (c *redisConn) writeCmd(args ...string) {
	c.w.WriteByte('*')
	c.w.WriteString(strconv.Itoa(len(args)))
	c.w.WriteString("\r\n")
	for _, a := range args {
		c.w.WriteByte('$')
		c.w.WriteString(strconv.Itoa(len(a)))
		c.w.WriteString("\r\n")
		c.w.WriteString(a)
		c.w.WriteString("\r\n")
	}
}

func (c *redisConn) readResp() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 {
		return "", fmt.Errorf("short line: %q", line)
	}
	line = line[:len(line)-2] // strip \r\n

	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "", nil // nil
		}
		data := make([]byte, n+2)
		if _, err := readFull(c.r, data); err != nil {
			return "", err
		}
		return string(data[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "*-1", nil
		}
		// Read all elements, return count
		var elems []string
		for i := 0; i < n; i++ {
			elem, err := c.readResp()
			if err != nil {
				return "", err
			}
			elems = append(elems, elem)
		}
		return fmt.Sprintf("*%d:%v", n, elems), nil
	default:
		return "", fmt.Errorf("unknown resp: %q", line)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
