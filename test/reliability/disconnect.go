package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

func runDisconnectTests() string {
	pass := true

	// Test 1: 客户端突然断开
	fmt.Print("  [1/4] 客户端突然断开 (半开连接)... ")
	if err := testAbruptDisconnect(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	// Test 2: 发送不完整命令后消失
	fmt.Print("  [2/4] 不完整命令后断开... ")
	if err := testPartialCommand(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	// Test 3: 大量连接快速建立和断开
	fmt.Print("  [3/4] 快速连接风暴 (200连接)... ")
	if err := testConnectionStorm(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	// Test 4: 断开后 Proxy 仍正常服务
	fmt.Print("  [4/4] 异常断开后 Proxy 仍正常... ")
	if err := testProxyStillWorks(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	if pass {
		return "PASS"
	}
	return "FAIL"
}

func testAbruptDisconnect() error {
	// 建立 20 个连接，AUTH 后立刻 Close（不发 QUIT）
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", *masterAddr, 3*time.Second)
		if err != nil {
			return fmt.Errorf("dial %d: %w", i, err)
		}
		// AUTH
		if *password != "" {
			cmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(*password), *password)
			conn.Write([]byte(cmd))
			buf := make([]byte, 64)
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			conn.Read(buf)
		}
		// 发一条 SET
		conn.Write([]byte("*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$1\r\n1\r\n"))
		// 不读响应，直接关闭
		conn.Close()
	}
	time.Sleep(100 * time.Millisecond)
	return nil
}

func testPartialCommand() error {
	// 发送半条命令然后断开
	partials := []string{
		"*3\r\n$3\r\nSET\r\n",           // 只发了一半参数
		"*3\r\n$3\r\nSET\r\n$5\r\nhel",  // bulk string 没发完
		"*2\r\n",                          // 只有数组头
		"$",                               // 一个字节
	}

	for i, partial := range partials {
		conn, err := net.DialTimeout("tcp", *masterAddr, 3*time.Second)
		if err != nil {
			return fmt.Errorf("dial %d: %w", i, err)
		}
		if *password != "" {
			cmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(*password), *password)
			conn.Write([]byte(cmd))
			buf := make([]byte, 64)
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			conn.Read(buf)
		}
		conn.Write([]byte(partial))
		time.Sleep(50 * time.Millisecond)
		conn.Close()
	}
	time.Sleep(100 * time.Millisecond)
	return nil
}

func testConnectionStorm() error {
	// 200 个连接快速建立、AUTH、发一条命令、断开
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		go func(i int) {
			conn, err := net.DialTimeout("tcp", *masterAddr, 5*time.Second)
			if err != nil {
				errs <- fmt.Errorf("dial %d: %w", i, err)
				return
			}
			defer conn.Close()

			if *password != "" {
				cmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(*password), *password)
				conn.Write([]byte(cmd))
				buf := make([]byte, 64)
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				conn.Read(buf)
			}
			conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
			buf := make([]byte, 64)
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			conn.Read(buf)
			errs <- nil
		}(i)
	}

	var failures int
	for i := 0; i < 200; i++ {
		if err := <-errs; err != nil {
			failures++
		}
	}
	if failures > 10 { // 允许少量失败（OS 文件描述符限制）
		return fmt.Errorf("%d/200 connections failed", failures)
	}
	return nil
}

func testProxyStillWorks() error {
	// 执行完上面所有异常操作后，正常客户端能否正常工作
	rc, err := dial(*masterAddr, *password)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer rc.Close()

	// SET + GET 验证
	if err := rc.Do("SET", "reliability_check", "alive"); err != nil {
		return fmt.Errorf("SET: %w", err)
	}
	val, err := rc.DoGet("GET", "reliability_check")
	if err != nil {
		return fmt.Errorf("GET: %w", err)
	}
	if !strings.Contains(val, "alive") && val != "alive" {
		return fmt.Errorf("GET returned %q, want 'alive'", val)
	}
	return nil
}
