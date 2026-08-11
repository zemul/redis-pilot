package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func runSoakTest() string {
	dur := *soakDur
	fmt.Printf("  持续时间: %v, 20 并发客户端\n", dur)
	fmt.Println("  每 10 秒输出一次状态...")

	var totalOps atomic.Int64
	var errors atomic.Int64
	var mismatches atomic.Int64
	stop := make(chan struct{})

	// 20 个 worker 持续跑
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			soakWorker(id, stop, &totalOps, &errors, &mismatches)
		}(i)
	}

	// 定时打印状态
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	lastOps := int64(0)

	deadline := time.After(dur)
	for {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()

			elapsed := time.Since(start)
			total := totalOps.Load()
			fmt.Printf("\n  最终结果:\n")
			fmt.Printf("    耗时:     %v\n", elapsed)
			fmt.Printf("    总操作:   %d\n", total)
			fmt.Printf("    平均QPS:  %.0f\n", float64(total)/elapsed.Seconds())
			fmt.Printf("    错误:     %d\n", errors.Load())
			fmt.Printf("    数据串:   %d\n", mismatches.Load())

			if mismatches.Load() > 0 {
				return "FAIL: data mismatch"
			}
			if errors.Load() > total/100 { // >1% 错误率
				return fmt.Sprintf("FAIL: error rate %.2f%%", float64(errors.Load())/float64(total)*100)
			}
			return "PASS"

		case <-ticker.C:
			now := totalOps.Load()
			delta := now - lastOps
			lastOps = now
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("    [%v] ops=%d qps=%d errs=%d mismatch=%d alloc=%.1fMB\n",
				time.Since(start).Round(time.Second),
				now, delta/10, errors.Load(), mismatches.Load(),
				float64(m.Alloc)/1024/1024)
		}
	}
}

func soakWorker(id int, stop chan struct{}, totalOps, errors, mismatches *atomic.Int64) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		rc, err := dial(*masterAddr, *password)
		if err != nil {
			errors.Add(1)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 每条连接跑 500 次操作后重连（模拟真实连接池行为）
		for seq := 0; seq < 500; seq++ {
			select {
			case <-stop:
				rc.Close()
				return
			default:
			}

			key := fmt.Sprintf("soak_%d_%d", id, seq)
			b := make([]byte, 8)
			rand.Read(b)
			val := hex.EncodeToString(b)

			// SET
			if err := rc.Do("SET", key, val); err != nil {
				errors.Add(1)
				break
			}

			// GET and verify
			got, err := rc.DoGet("GET", key)
			if err != nil {
				errors.Add(1)
				break
			}
			if got != val {
				mismatches.Add(1)
			}

			totalOps.Add(2)

			// DEL (cleanup)
			rc.Do("DEL", key)
		}
		rc.Close()
	}
}
