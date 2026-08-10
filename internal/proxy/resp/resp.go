// Package resp 提供 RESP2 协议的流式解析与编码。
//
// proxy 的数据路径有两类需求：
//   - 客户端侧：解析命令（array of bulk string）拿到 argv 供 route.Classify 判定
//   - 后端侧：读取一帧完整响应（不解释内容，只定位边界）原样转发给客户端
//
// 因此 Reader 提供 ReadCommand（解析命令）和 ReadFrame（读取任意一帧的原始字节）。
package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// ── Reader ──

type Reader struct {
	br       *bufio.Reader
	maxBytes int // 单条命令最大字节数（0=不限）
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 8192)}
}

// SetMaxBytes 设置单条命令的最大字节数。
// 超限时 ReadCommand 返回错误，防止 OOM。
func (r *Reader) SetMaxBytes(n int) {
	r.maxBytes = n
}

// PendingCommand returns true if more data is buffered and likely forms
// another complete command. Used to detect pipelined commands without blocking.
func (r *Reader) PendingCommand() bool {
	return r.br.Buffered() > 0
}

// ReadCommand 读取一条客户端命令（RESP array of bulk string）。
// 返回原始字节（可直接转发后端）和解析出的 argv（命令名 + 参数，不含 RESP 框架）。
// 支持 inline 命令（非 * 开头）以兼容老客户端。
func (r *Reader) ReadCommand() (raw []byte, argv [][]byte, err error) {
	first, err := r.br.Peek(1)
	if err != nil {
		return nil, nil, err
	}

	if first[0] != '*' {
		// inline 命令：一整行按空格拆分
		return r.readInline()
	}
	return r.readArrayCommand()
}

// readArrayCommand 解析 *N\r\n + N 个 bulk string。
func (r *Reader) readArrayCommand() (raw []byte, argv [][]byte, err error) {
	line, err := r.readLine()
	if err != nil {
		return nil, nil, err
	}
	raw = make([]byte, 0, 256) // pre-allocate to reduce reallocations
	raw = append(raw, line...)

	count, err := parseArrayCount(line)
	if err != nil {
		return nil, nil, err
	}
	if count < 0 {
		// *-1 (null array) -- 不是合法命令
		return raw, nil, fmt.Errorf("resp: null array is not a valid command")
	}

	argv = make([][]byte, 0, min(count, 64))
	for i := 0; i < count; i++ {
		elem, err := r.readBulkString(&raw)
		if err != nil {
			return raw, argv, err
		}
		argv = append(argv, elem)
		if r.maxBytes > 0 && len(raw) > r.maxBytes {
			return raw, argv, fmt.Errorf("resp: command exceeds max %d bytes", r.maxBytes)
		}
	}
	return raw, argv, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// readBulkString 读取一个 $len\r\ndata\r\n，返回 data 和累积的 raw。
func (r *Reader) readBulkString(raw *[]byte) ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	*raw = append(*raw, line...)

	if len(line) < 1 || line[0] != '$' {
		return nil, fmt.Errorf("resp: expected bulk string, got %q", line)
	}

	lenStr := string(line[1 : len(line)-2]) // 去掉 \r\n
	if lenStr == "-1" {
		return nil, nil // null bulk string
	}

	n, err := strconv.Atoi(lenStr)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("resp: invalid bulk string length %q", lenStr)
	}
	if r.maxBytes > 0 && n > r.maxBytes {
		return nil, fmt.Errorf("resp: bulk string length %d exceeds max %d", n, r.maxBytes)
	}

	data := make([]byte, n+2) // data + \r\n
	if _, err := io.ReadFull(r.br, data); err != nil {
		return nil, err
	}
	*raw = append(*raw, data...)

	return data[:n], nil // 不含 \r\n
}

// readInline 解析 inline 命令（空格分隔，以 \r\n 结束）。
func (r *Reader) readInline() (raw []byte, argv [][]byte, err error) {
	line, err := r.readLine()
	if err != nil {
		return nil, nil, err
	}
	raw = line

	// 去掉 \r\n 后按空格拆分
	trimmed := line[:len(line)-2]
	start := 0
	for i := 0; i <= len(trimmed); i++ {
		if i == len(trimmed) || trimmed[i] == ' ' {
			if i > start {
				argv = append(argv, trimmed[start:i])
			}
			start = i + 1
		}
	}
	return raw, argv, nil
}

// readLine 读取一行（含 \r\n），返回包含 \r\n 的字节。
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.br.ReadSlice('\n')
		if err != nil {
			return nil, err
		}
		line = append(line, chunk...)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			return line, nil
		}
		// \n 前面不是 \r，继续读
	}
}

// ReadFrame 读取一帧完整的 RESP2 响应，返回原始字节。
// 不解释内容，只通过结构递归定位帧边界。用于后端响应的原样转发。
func (r *Reader) ReadFrame() ([]byte, error) {
	first, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}

	switch first[0] {
	case '+', '-', ':':
		// Simple String / Error / Integer: 一行
		return r.readLine()
	case '$':
		// Bulk String: $len\r\n + data + \r\n
		return r.readBulkFrame()
	case '*':
		// Array: *count\r\n + N 帧
		return r.readArrayFrame()
	default:
		return nil, fmt.Errorf("resp: unknown type byte %q", first[0])
	}
}

// readBulkFrame 读取 bulk string 的完整帧（含 $len\r\ndata\r\n）。
func (r *Reader) readBulkFrame() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 0, 256) // pre-allocate to reduce reallocations
	raw = append(raw, line...)

	if len(line) < 1 || line[0] != '$' {
		return nil, fmt.Errorf("resp: expected bulk string, got %q", line)
	}

	lenStr := string(line[1 : len(line)-2])
	if lenStr == "-1" {
		return raw, nil // null bulk string, no data
	}

	n, err := strconv.Atoi(lenStr)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("resp: invalid bulk string length %q", lenStr)
	}

	data := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, data); err != nil {
		return nil, err
	}
	raw = append(raw, data...)
	return raw, nil
}

// readArrayFrame 读取 array 的完整帧。
// 对 bulk string 元素内联读取（避免递归调用和中间 buffer 分配），
// 其他类型回退 ReadFrame 递归处理。
func (r *Reader) readArrayFrame() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	count, err := parseArrayCount(line)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		// null array
		raw := make([]byte, 0, len(line))
		return append(raw, line...), nil
	}

	// Pre-allocate based on element count: each bulk string is at least
	// $N\r\n + data + \r\n. 32 bytes/element is a conservative average.
	raw := make([]byte, 0, len(line)+count*32)
	raw = append(raw, line...)

	for i := 0; i < count; i++ {
		first, err := r.br.Peek(1)
		if err != nil {
			return raw, err
		}

		if first[0] == '$' {
			// Inline bulk string: read header + data directly into raw,
			// avoiding the intermediate allocation in readBulkFrame.
			elemLine, err := r.readLine()
			if err != nil {
				return raw, err
			}
			raw = append(raw, elemLine...)

			if len(elemLine) < 4 {
				return raw, fmt.Errorf("resp: invalid bulk string header %q", elemLine)
			}

			lenStr := string(elemLine[1 : len(elemLine)-2])
			if lenStr == "-1" {
				continue // null bulk string, no data
			}

			n, err := strconv.Atoi(lenStr)
			if err != nil || n < 0 {
				return raw, fmt.Errorf("resp: invalid bulk string length %q", lenStr)
			}

			// Grow raw by n+2, then read data+\r\n directly into it.
			start := len(raw)
			raw = append(raw, make([]byte, n+2)...)
			if _, err := io.ReadFull(r.br, raw[start:]); err != nil {
				return raw, err
			}
		} else {
			// Non-bulk-string element (simple string, error, integer, nested array):
			// fall back to recursive ReadFrame.
			elem, err := r.ReadFrame()
			if err != nil {
				return raw, err
			}
			raw = append(raw, elem...)
		}
	}
	return raw, nil
}

func parseArrayCount(line []byte) (int, error) {
	if len(line) < 3 || line[0] != '*' {
		return 0, fmt.Errorf("resp: invalid array header %q", line)
	}
	s := string(line[1 : len(line)-2]) // 去掉 \r\n
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("resp: invalid array count %q", s)
	}
	return n, nil
}

// ── Writer ──

type Writer struct {
	bw *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 8192)}
}

// WriteRaw 写入原始 RESP 字节并 flush。
func (w *Writer) WriteRaw(raw []byte) error {
	if _, err := w.bw.Write(raw); err != nil {
		return err
	}
	return w.bw.Flush()
}

// WriteRawNoFlush writes raw RESP bytes without flushing.
// Caller must Flush before waiting for responses. Used for pipeline batching.
func (w *Writer) WriteRawNoFlush(raw []byte) error {
	_, err := w.bw.Write(raw)
	return err
}

// WriteCommand 将 argv 编码为 RESP array 并写入（发给后端）。
func (w *Writer) WriteCommand(argv [][]byte) error {
	if _, err := fmt.Fprintf(w.bw, "*%d\r\n", len(argv)); err != nil {
		return err
	}
	for _, arg := range argv {
		if _, err := fmt.Fprintf(w.bw, "$%d\r\n", len(arg)); err != nil {
			return err
		}
		if _, err := w.bw.Write(arg); err != nil {
			return err
		}
		if _, err := w.bw.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return w.bw.Flush()
}

// WriteSimpleString 写入 +OK 风格响应。
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.bw, "+%s\r\n", s)
	if err == nil {
		err = w.bw.Flush()
	}
	return err
}

// WriteError 写入 -ERR 风格错误。
func (w *Writer) WriteError(s string) error {
	_, err := fmt.Fprintf(w.bw, "-%s\r\n", s)
	if err == nil {
		err = w.bw.Flush()
	}
	return err
}

// WriteInteger 写入 :n 风格响应。
func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.bw, ":%d\r\n", n)
	if err == nil {
		err = w.bw.Flush()
	}
	return err
}

// WriteBulkString 写入 $len\r\ndata\r\n 风格响应。
func (w *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.bw, "$%d\r\n%s\r\n", len(s), s)
	if err == nil {
		err = w.bw.Flush()
	}
	return err
}

// WriteNull 写入 $-1 (null bulk string)。
func (w *Writer) WriteNull() error {
	_, err := w.bw.WriteString("$-1\r\n")
	if err == nil {
		err = w.bw.Flush()
	}
	return err
}

// WriteArrayHeader 写入 array 头部（*N\r\n），后续元素自行 WriteRaw。
// 用于构造多元素响应。
func (w *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.bw, "*%d\r\n", n)
	return err
}

// Flush 刷新缓冲区。
func (w *Writer) Flush() error {
	return w.bw.Flush()
}
