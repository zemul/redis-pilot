package discovery

import "time"

// ServerConfig 是到 Server 的连接配置。
type ServerConfig struct {
	NodeID   string // local platform node; scopes loopback Gateway routes
	Endpoint string // Server HTTP 地址，如 "http://127.0.0.1:8080"
	Token    string // Bearer token（空=不鉴权）
}

// PollConfig 控制快照轮询行为。
type PollConfig struct {
	// Interval 是两次请求之间的最小间隔。长轮询模式下它只作为下限（防止 Server 不认识
	// 长轮询参数时变成忙循环），短轮询模式下它就是轮询周期。
	Interval time.Duration
	// Timeout 是网络往返的余量，不包含服务端挂起的时长。
	Timeout time.Duration
	// Wait 是长轮询时允许 Server 挂起请求的最长时间，0 表示关闭长轮询、退回定期轮询。
	// Server 侧上限 30s，配大了会被截断。
	Wait time.Duration
}

// DefaultPollConfig 返回合理的默认轮询配置。
func DefaultPollConfig() PollConfig {
	return PollConfig{
		Interval: 2 * time.Second,
		Timeout:  5 * time.Second,
		Wait:     20 * time.Second,
	}
}
