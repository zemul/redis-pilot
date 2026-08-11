package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func runBoundaryTests() string {
	pass := true

	fmt.Print("  [1/6] 大 value (1MB)... ")
	if err := testLargeValue(1024 * 1024); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	fmt.Print("  [2/6] 大 value (5MB)... ")
	if err := testLargeValue(5 * 1024 * 1024); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	fmt.Print("  [3/6] MGET/MSET 100 keys... ")
	if err := testMGetMSet(100); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	fmt.Print("  [4/6] MGET/MSET 1000 keys... ")
	if err := testMGetMSet(1000); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	fmt.Print("  [5/6] MULTI/EXEC 事务完整性... ")
	if err := testTransaction(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		pass = false
	} else {
		fmt.Println("OK")
	}

	fmt.Print("  [6/6] SUBSCRIBE 发布订阅... ")
	if err := testPubSub(); err != nil {
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

func testLargeValue(size int) error {
	rc, err := dial(*masterAddr, *password)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Generate random data
	data := make([]byte, size/2)
	rand.Read(data)
	val := hex.EncodeToString(data) // size bytes of hex

	key := fmt.Sprintf("large_value_%d", size)

	if err := rc.Do("SET", key, val); err != nil {
		return fmt.Errorf("SET: %w", err)
	}

	got, err := rc.DoGet("GET", key)
	if err != nil {
		return fmt.Errorf("GET: %w", err)
	}
	if got != val {
		return fmt.Errorf("value mismatch: wrote %d bytes, got %d bytes", len(val), len(got))
	}

	// Cleanup
	rc.Do("DEL", key)
	return nil
}

func testMGetMSet(n int) error {
	rc, err := dial(*masterAddr, *password)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Build MSET args
	args := []string{"MSET"}
	expected := make(map[string]string)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("mkey_%d_%d", n, i)
		v := fmt.Sprintf("val_%d_%d", n, i)
		args = append(args, k, v)
		expected[k] = v
	}

	if err := rc.Do(args...); err != nil {
		return fmt.Errorf("MSET: %w", err)
	}

	// MGET
	getArgs := []string{"MGET"}
	for i := 0; i < n; i++ {
		getArgs = append(getArgs, fmt.Sprintf("mkey_%d_%d", n, i))
	}

	rc.SendCmd(getArgs...)
	rc.Flush()
	resp, err := rc.ReadResp()
	if err != nil {
		return fmt.Errorf("MGET read: %w", err)
	}

	// Verify we got an array response
	if !strings.HasPrefix(resp, fmt.Sprintf("*%d:", n)) {
		return fmt.Errorf("expected *%d array, got: %.50s", n, resp)
	}

	// Cleanup
	delArgs := []string{"DEL"}
	for i := 0; i < n; i++ {
		delArgs = append(delArgs, fmt.Sprintf("mkey_%d_%d", n, i))
	}
	rc.Do(delArgs...)
	return nil
}

func testTransaction() error {
	rc, err := dial(*masterAddr, *password)
	if err != nil {
		return err
	}
	defer rc.Close()

	// MULTI
	if err := rc.Do("MULTI"); err != nil {
		return fmt.Errorf("MULTI: %w", err)
	}

	// Queue commands
	rc.SendCmd("SET", "tx_key1", "100")
	rc.SendCmd("INCR", "tx_key1")
	rc.SendCmd("SET", "tx_key2", "hello")
	rc.SendCmd("GET", "tx_key2")
	rc.Flush()

	// Read 4 QUEUED responses
	for i := 0; i < 4; i++ {
		resp, err := rc.ReadResp()
		if err != nil {
			return fmt.Errorf("QUEUED %d: %w", i, err)
		}
		if resp != "+QUEUED" {
			return fmt.Errorf("expected QUEUED, got: %s", resp)
		}
	}

	// EXEC
	rc.SendCmd("EXEC")
	rc.Flush()
	resp, err := rc.ReadResp()
	if err != nil {
		return fmt.Errorf("EXEC: %w", err)
	}
	if !strings.HasPrefix(resp, "*4:") {
		return fmt.Errorf("EXEC expected *4 array, got: %.80s", resp)
	}

	// Verify result
	got, err := rc.DoGet("GET", "tx_key1")
	if err != nil {
		return fmt.Errorf("GET tx_key1: %w", err)
	}
	if got != "101" {
		return fmt.Errorf("tx_key1 = %q, want '101'", got)
	}

	got, err = rc.DoGet("GET", "tx_key2")
	if err != nil {
		return fmt.Errorf("GET tx_key2: %w", err)
	}
	if got != "hello" {
		return fmt.Errorf("tx_key2 = %q, want 'hello'", got)
	}

	// Cleanup
	rc.Do("DEL", "tx_key1", "tx_key2")
	return nil
}

func testPubSub() error {
	// Subscriber
	sub, err := dial(*masterAddr, *password)
	if err != nil {
		return err
	}
	defer sub.Close()

	sub.SendCmd("SUBSCRIBE", "test_channel")
	sub.Flush()

	// Read subscribe confirmation
	sub.SetDeadline(time.Now().Add(3 * time.Second))
	resp, err := sub.ReadResp()
	if err != nil {
		return fmt.Errorf("subscribe confirm: %w", err)
	}
	if !strings.Contains(resp, "subscribe") {
		return fmt.Errorf("expected subscribe confirmation, got: %.50s", resp)
	}

	// Publisher
	pub, err := dial(*masterAddr, *password)
	if err != nil {
		return err
	}
	defer pub.Close()

	// Publish
	if err := pub.Do("PUBLISH", "test_channel", "hello_proxy"); err != nil {
		return fmt.Errorf("PUBLISH: %w", err)
	}

	// Read message from subscriber
	sub.SetDeadline(time.Now().Add(3 * time.Second))
	msg, err := sub.ReadResp()
	if err != nil {
		return fmt.Errorf("read message: %w", err)
	}
	if !strings.Contains(msg, "hello_proxy") {
		return fmt.Errorf("expected 'hello_proxy' in message, got: %.80s", msg)
	}

	return nil
}
