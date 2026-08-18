// Package sentinel 实现伪 Sentinel 服务发现协议（proxy-design §19）。
//
// 让 sentinel 模式的客户端零改代码接入 Proxy——Proxy 伪装成只读 Sentinel，
// get-master-addr-by-name <group> 返回该 group 的 Proxy 数据端口。
package sentinel

import (
	"fmt"
	"strconv"
)

var (
	emptyArray = []byte("*0\r\n")
	nullArray  = []byte("*-1\r\n")
)

// encodeArrayHeader 编码 RESP 数组头 *N\r\n。
func encodeArrayHeader(n int) []byte {
	return []byte(fmt.Sprintf("*%d\r\n", n))
}

// encodeStringArray 将字符串切片编码为 RESP bulk string 数组。
func encodeStringArray(ss []string) []byte {
	buf := encodeArrayHeader(len(ss))
	for _, s := range ss {
		buf = append(buf, fmt.Sprintf("$%d\r\n", len(s))...)
		buf = append(buf, s...)
		buf = append(buf, "\r\n"...)
	}
	return buf
}

// encodeMasterInfo 编码 SENTINEL master 的扁平 field/value 数组。
// 字段集覆盖 Jedis/Lettuce/go-redis 常读的最小集。
func encodeMasterInfo(name, ip string, port int) []byte {
	fields := []string{
		"name", name,
		"ip", ip,
		"port", strconv.Itoa(port),
		"runid", "",
		"flags", "master",
		"link-pending-commands", "0",
		"link-refcount", "1",
		"last-ping-sent", "0",
		"last-ok-ping-reply", "0",
		"last-ping-reply", "0",
		"down-after-milliseconds", "30000",
		"info-refresh", "0",
		"role-reported", "master",
		"role-reported-time", "0",
		"config-epoch", "0",
		"num-slaves", "0",
		"num-other-sentinels", "0",
		"quorum", "2",
		"failover-timeout", "180000",
		"parallel-syncs", "1",
	}
	return encodeStringArray(fields)
}

// encodeSubscribeReply 编码订阅确认帧：*3\r\n [kind, channel, count]。
func encodeSubscribeReply(kind, channel string, count int) []byte {
	buf := []byte("*3\r\n")
	buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(kind), kind)...)
	buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel)...)
	buf = append(buf, fmt.Sprintf(":%d\r\n", count)...)
	return buf
}

// encodeUnsubscribeReply 编码退订确认帧。
func encodeUnsubscribeReply(kind, channel string, count int) []byte {
	buf := []byte("*3\r\n")
	buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(kind), kind)...)
	if channel == "" {
		buf = append(buf, "$-1\r\n"...)
	} else {
		buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel)...)
	}
	buf = append(buf, fmt.Sprintf(":%d\r\n", count)...)
	return buf
}

// minimalInfo 返回 INFO 命令的最小响应，标识自己为 sentinel 模式。
func minimalInfo() string {
	return "# Server\r\nredis_version:7.0.0\r\nredis_mode:sentinel\r\n" +
		"# Sentinel\r\nsentinel_masters:0\r\n"
}
