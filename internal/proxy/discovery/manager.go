package discovery

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Manager 持续从 Server 拉取 ProxySnapshot 并维护后端拓扑。
//
// 职责：
//   - 长轮询拉取快照（复用原 xds/client.go 的逻辑，已搬到本包）
//   - 解析快照为 Topology
//   - 首次快照就绪前 WaitReady 阻塞（proxy-design §7.5）
//   - 快照拉取失败时保持最后快照（proxy-design §7.6）
//   - 拓扑变化时通过 onUpdate 回调通知 proxysvc
type Manager struct {
	client   *serverClient
	pollCfg  PollConfig
	httpCli  *http.Client

	mu      sync.RWMutex
	topo    *Topology
	version string

	ready    chan struct{} // 首次快照就绪后 close
	once     sync.Once
	onUpdate func(*Topology, *Topology) // (new, old) 回调
}

// NewManager 创建 discovery manager。
func NewManager(serverCfg ServerConfig, pollCfg PollConfig) *Manager {
	// 刻意不设 http.Client.Timeout：它会连长轮询的正常挂起一起掐断。
	// 每次请求的超时由 refresh 用 context 按 Wait+Timeout 单独设定。
	httpCli := &http.Client{}
	return &Manager{
		client:  newServerClient(serverCfg, httpCli),
		pollCfg: pollCfg,
		httpCli: httpCli,
		ready:   make(chan struct{}),
	}
}

// OnUpdate 注册拓扑变化回调。cb 在快照变化时被调用，参数为 (new, old)。
// old 为 nil 表示首次快照。
func (m *Manager) OnUpdate(cb func(newTopo, oldTopo *Topology)) {
	m.onUpdate = cb
}

// Run 启动轮询循环。阻塞直到 ctx 取消。
func (m *Manager) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		changed, err := m.refresh(ctx)

		switch {
		case err != nil:
			// 出错时退避，避免 Server 不可用期间被打成忙循环（§7.6）
			slog.Warn("discovery fetch snapshot failed, keeping last topology", "err", err)
			if !sleepCtx(ctx, m.pollCfg.Interval) {
				return
			}
		case changed:
			// 拿到新版本，立刻检查下一个
		default:
			// 版本没变：补足到 Interval 再重试
			if remaining := m.pollCfg.Interval - time.Since(start); remaining > 0 {
				if !sleepCtx(ctx, remaining) {
					return
				}
			}
		}
	}
}

// WaitReady 阻塞直到首次快照就绪或 ctx 取消（§7.5）。
func (m *Manager) WaitReady(ctx context.Context) error {
	select {
	case <-m.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Topology 返回当前拓扑（线程安全）。未就绪时返回 nil。
func (m *Manager) Topology() *Topology {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topo
}

// refresh 拉取一次快照，必要时更新拓扑并触发回调。
func (m *Manager) refresh(ctx context.Context) (bool, error) {
	m.mu.RLock()
	knownVersion := m.version
	m.mu.RUnlock()

	wait := m.pollCfg.Wait
	// 首次还没有版本可比，直接取一次，不进入长轮询
	if knownVersion == "" {
		wait = 0
	}

	// 请求超时必须覆盖服务端的挂起时长（§7.5 红线）
	reqCtx, cancel := context.WithTimeout(ctx, wait+m.pollCfg.Timeout)
	defer cancel()

	snap, unchanged, err := m.client.FetchProxySnapshot(reqCtx, knownVersion, wait)
	if err != nil {
		return false, err
	}
	if unchanged {
		return false, nil
	}
	if snap.Version == knownVersion {
		return false, nil
	}

	newTopo := ParseSnapshot(snap)

	m.mu.Lock()
	oldTopo := m.topo
	m.topo = newTopo
	m.version = snap.Version
	m.mu.Unlock()

	// 首次快照：signal ready
	m.once.Do(func() {
		close(m.ready)
	})

	// 触发回调
	if m.onUpdate != nil {
		m.onUpdate(newTopo, oldTopo)
	}

	slog.Info("discovery snapshot",
		"version", snap.Version,
		"groups", len(newTopo.Groups),
		"listeners", len(snap.Listeners),
		"clusters", len(snap.Clusters))
	return true, nil
}

// sleepCtx 睡 d，被 ctx 取消时返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
