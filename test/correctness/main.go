package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr     = flag.String("addr", "127.0.0.1:16379", "proxy address")
	password = flag.String("password", "", "AUTH password")
	clients  = flag.Int("clients", 50, "number of concurrent clients")
	ops      = flag.Int("ops", 1000, "operations per client")
	pipeline = flag.Int("pipeline", 1, "pipeline depth (1=no pipeline)")
)

func main() {
	flag.Parse()

	fmt.Printf("正确性测试: addr=%s clients=%d ops=%d pipeline=%d\n", *addr, *clients, *ops, *pipeline)
	fmt.Println("每个客户端: SET unique_key → GET same_key → 验证值匹配")
	fmt.Println()

	var totalOps atomic.Int64
	var connErrors atomic.Int64
	var mismatches atomic.Int64

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := runClient(id, &totalOps, &mismatches); err != nil {
				connErrors.Add(1)
				fmt.Fprintf(os.Stderr, "client %d error: %v\n", id, err)
			}
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	total := totalOps.Load()
	fmt.Println()
	fmt.Printf("============ 结果 ============\n")
	fmt.Printf("耗时:     %v\n", elapsed)
	fmt.Printf("总操作:   %d\n", total)
	fmt.Printf("QPS:      %.0f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("错误连接: %d\n", connErrors.Load())
	fmt.Printf("数据串:   %d\n", mismatches.Load())
	fmt.Println()

	if mismatches.Load() > 0 {
		fmt.Println("❌ 发现数据串！FIFO 可能有 BUG")
		os.Exit(1)
	}
	if connErrors.Load() > 0 {
		fmt.Println("⚠️  有连接错误，但未发现数据串")
		os.Exit(1)
	}
	fmt.Println("✅ 所有数据正确，无串数据")
}

func runClient(id int, totalOps *atomic.Int64, mismatches *atomic.Int64) error {
	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReaderSize(conn, 65536)
	w := bufio.NewWriterSize(conn, 65536)

	// AUTH
	if *password != "" {
		writeCmd(w, "AUTH", *password)
		w.Flush()
		resp, err := readResp(r)
		if err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		if resp != "+OK" {
			return fmt.Errorf("auth failed: %s", resp)
		}
	}

	p := *pipeline
	if p <= 1 {
		// Serial mode
		for seq := 0; seq < *ops; seq++ {
			key := fmt.Sprintf("c_%d_%d", id, seq)
			val := randomHex(16)

			writeCmd(w, "SET", key, val)
			w.Flush()
			resp, err := readResp(r)
			if err != nil {
				return fmt.Errorf("set seq=%d: %w", seq, err)
			}
			if resp != "+OK" {
				return fmt.Errorf("set seq=%d: %s", seq, resp)
			}

			writeCmd(w, "GET", key)
			w.Flush()
			got, err := readResp(r)
			if err != nil {
				return fmt.Errorf("get seq=%d: %w", seq, err)
			}
			if got != val {
				mismatches.Add(1)
				fmt.Fprintf(os.Stderr, "❌ client=%d seq=%d key=%s expected=%s got=%s\n", id, seq, key, val, got)
			}
			totalOps.Add(2)
		}
	} else {
		// Pipeline mode
		for base := 0; base < *ops; base += p {
			batch := p
			if base+batch > *ops {
				batch = *ops - base
			}

			keys := make([]string, batch)
			vals := make([]string, batch)

			// Send all SETs
			for i := 0; i < batch; i++ {
				keys[i] = fmt.Sprintf("c_%d_%d", id, base+i)
				vals[i] = randomHex(16)
				writeCmd(w, "SET", keys[i], vals[i])
			}
			w.Flush()

			// Read all SET responses
			for i := 0; i < batch; i++ {
				resp, err := readResp(r)
				if err != nil {
					return fmt.Errorf("pipeline set %d: %w", base+i, err)
				}
				if resp != "+OK" {
					return fmt.Errorf("pipeline set %d: %s", base+i, resp)
				}
			}

			// Send all GETs
			for i := 0; i < batch; i++ {
				writeCmd(w, "GET", keys[i])
			}
			w.Flush()

			// Read all GET responses and verify
			for i := 0; i < batch; i++ {
				got, err := readResp(r)
				if err != nil {
					return fmt.Errorf("pipeline get %d: %w", base+i, err)
				}
				if got != vals[i] {
					mismatches.Add(1)
					fmt.Fprintf(os.Stderr, "❌ client=%d seq=%d key=%s expected=%s got=%s\n", id, base+i, keys[i], vals[i], got)
				}
			}
			totalOps.Add(int64(batch * 2))
		}
	}
	return nil
}

// writeCmd writes a RESP command into the buffered writer (no flush).
func writeCmd(w *bufio.Writer, args ...string) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(args)))
	w.WriteString("\r\n")
	for _, a := range args {
		w.WriteByte('$')
		w.WriteString(strconv.Itoa(len(a)))
		w.WriteString("\r\n")
		w.WriteString(a)
		w.WriteString("\r\n")
	}
}

// readResp reads one RESP response. Returns the value as string.
// For simple strings: "+OK" returns "OK" → we return "+OK" for easy checking.
// For bulk strings: "$5\r\nhello\r\n" returns "hello".
// For errors: "-ERR ..." returns the full line.
// For nil: "$-1\r\n" returns "".
func readResp(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 {
		return "", fmt.Errorf("short line: %q", line)
	}
	// Strip \r\n
	line = line[:len(line)-2]

	switch line[0] {
	case '+':
		return line, nil // "+OK"
	case '-':
		return line, nil // "-ERR ..."
	case ':':
		return line, nil // ":123"
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", fmt.Errorf("bad bulk len: %q", line)
		}
		if n < 0 {
			return "", nil // nil bulk string
		}
		data := make([]byte, n+2) // +2 for \r\n
		_, err = readFull(r, data)
		if err != nil {
			return "", err
		}
		return string(data[:n]), nil
	case '*':
		// Array — not expected in this test, but handle gracefully
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", fmt.Errorf("bad array len: %q", line)
		}
		for i := 0; i < n; i++ {
			_, err := readResp(r)
			if err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("*%d", n), nil
	default:
		return "", fmt.Errorf("unknown resp type: %q", line)
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

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
