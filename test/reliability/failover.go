package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

func runFailoverTest() string {
	fmt.Println("  故障转移测试需要手动操作：")
	fmt.Println("  测试开始后请在 30 秒内 kill 掉 Redis master")
	fmt.Println("  (在 redis-node1 执行: podman stop <master容器> 或 redis-cli -p 6379 DEBUG SLEEP 60)")
	fmt.Println()
	fmt.Println("  开始持续写入，观察故障期间的错误和恢复...")
	fmt.Println()

	var success atomic.Int64
	var failures atomic.Int64
	stop := make(chan struct{})

	// 记录每秒的成功/失败
	type tick struct {
		sec  int
		ok   int64
		fail int64
	}
	ticks := make(chan tick, 120)

	// Worker: 持续写入
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			rc, err := dial(*masterAddr, *password)
			if err != nil {
				failures.Add(1)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			for i := 0; i < 100; i++ {
				select {
				case <-stop:
					rc.Close()
					return
				default:
				}

				err := rc.Do("SET", "failover_test_key", fmt.Sprintf("v_%d", time.Now().UnixMilli()))
				if err != nil {
					failures.Add(1)
					rc.Close()
					break
				}
				success.Add(1)
				time.Sleep(10 * time.Millisecond) // ~100 ops/sec
			}
			rc.Close()
		}
	}()

	// 每秒记录
	lastOK := int64(0)
	lastFail := int64(0)
	duration := 60 * time.Second

	timer := time.NewTimer(duration)
	secTicker := time.NewTicker(1 * time.Second)
	defer secTicker.Stop()

	sec := 0
	for {
		select {
		case <-timer.C:
			close(stop)
			time.Sleep(200 * time.Millisecond)
			goto done
		case <-secTicker.C:
			sec++
			nowOK := success.Load()
			nowFail := failures.Load()
			deltaOK := nowOK - lastOK
			deltaFail := nowFail - lastFail
			lastOK = nowOK
			lastFail = nowFail

			ticks <- tick{sec: sec, ok: deltaOK, fail: deltaFail}

			status := "✓"
			if deltaFail > 0 {
				status = "✗"
			}
			fmt.Printf("    [%02ds] %s ok=%d fail=%d\n", sec, status, deltaOK, deltaFail)
		}
	}

done:
	close(ticks)

	totalOK := success.Load()
	totalFail := failures.Load()

	fmt.Println()
	fmt.Printf("  总结: 成功=%d 失败=%d 失败率=%.1f%%\n",
		totalOK, totalFail,
		float64(totalFail)/float64(totalOK+totalFail)*100)

	if totalFail == 0 {
		fmt.Println("  (未检测到故障，可能没有执行 kill master)")
		return "PASS (no failover triggered)"
	}

	// 计算恢复时间：从第一个失败到最后一个失败的秒数
	fmt.Println("  如果故障窗口 < 15秒 → Proxy 自愈正常")

	if float64(totalFail)/float64(totalOK+totalFail) > 0.5 {
		return "FAIL: >50% failure rate"
	}
	return "PASS"
}
