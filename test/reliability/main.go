// 可靠性验证套件：覆盖故障转移、长稳、异常连接、边界场景。
//
// 用法：
//   go run ./test/reliability/ -addr 127.0.0.1:16379 -master-addr 127.0.0.1:16500 -password demo123
//
// 测试项：
//   1. failover    - 故障转移期间的可用性（需要手动 kill master）
//   2. soak        - 长稳测试（内存/连接稳定性）
//   3. disconnect  - 异常连接处理
//   4. boundary    - 边界场景（大 value、MGET、事务、SUBSCRIBE）
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	addr       = flag.String("addr", "127.0.0.1:16379", "proxy AUTO port")
	masterAddr = flag.String("master-addr", "127.0.0.1:16500", "proxy MASTER port")
	password   = flag.String("password", "", "AUTH password")
	tests      = flag.String("tests", "all", "comma-separated: failover,soak,disconnect,boundary,all")
	soakDur    = flag.Duration("soak-duration", 5*time.Minute, "soak test duration")
	metricsURL = flag.String("metrics", "", "proxy metrics URL (http://host:19100/metrics)")
)

func main() {
	flag.Parse()

	selected := parseTests(*tests)

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║    Redis Proxy 可靠性验证套件            ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Printf("AUTO端口:   %s\n", *addr)
	fmt.Printf("MASTER端口: %s\n", *masterAddr)
	fmt.Printf("测试项:     %v\n\n", selected)

	results := make(map[string]string)

	if contains(selected, "disconnect") {
		fmt.Println("\n━━━━━━━━━━ 异常连接测试 ━━━━━━━━━━")
		results["disconnect"] = runDisconnectTests()
	}

	if contains(selected, "boundary") {
		fmt.Println("\n━━━━━━━━━━ 边界场景测试 ━━━━━━━━━━")
		results["boundary"] = runBoundaryTests()
	}

	if contains(selected, "soak") {
		fmt.Println("\n━━━━━━━━━━ 长稳测试 ━━━━━━━━━━")
		results["soak"] = runSoakTest()
	}

	if contains(selected, "failover") {
		fmt.Println("\n━━━━━━━━━━ 故障转移测试 ━━━━━━━━━━")
		results["failover"] = runFailoverTest()
	}

	// 汇总
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║              测试结果汇总                ║")
	fmt.Println("╠══════════════════════════════════════════╣")
	allPass := true
	for name, result := range results {
		icon := "✅"
		if result != "PASS" {
			icon = "❌"
			allPass = false
		}
		fmt.Printf("║  %s %-12s %s\n", icon, name, result)
	}
	fmt.Println("╚══════════════════════════════════════════╝")

	if !allPass {
		os.Exit(1)
	}
}

func parseTests(s string) []string {
	if s == "all" {
		return []string{"disconnect", "boundary", "soak", "failover"}
	}
	return strings.Split(s, ",")
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
