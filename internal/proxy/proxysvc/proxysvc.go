// Package proxysvc 组装 listener、accept 循环、优雅关闭、指标。
//
// M5：共享连接池（Pool + FIFO 解复用），多个 session 复用同一组上游连接。
// Prometheus /metrics endpoint。
package proxysvc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/backend"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/discovery"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/metrics"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/session"
)

// Config 是 proxy 进程的配置。
type Config struct {
	// 通用
	ConnectTimeout    time.Duration
	ShutdownTimeout   time.Duration
	MasterReadWeight  int
	PoolSize          int           // 每后端池上限（§13，默认 64）
	IdleTimeout       time.Duration // 空闲连接回收（§13，默认 5m）
	MetricsListen     string        // /metrics HTTP 监听地址（空=不开）
	MaxRequestBytes   int           // 单条命令最大字节数（D13，默认 16MB）

	// Discovery 模式（M4）
	ServerEndpoint string
	ServerToken   string
	PollInterval  time.Duration
	PollTimeout   time.Duration
	PollWait      time.Duration

	// Static 模式（M3 兼容）
	Listen       string
	MasterAddr   string
	ReplicaAddrs []string
	Password     string
}

// Proxy 是 proxy 进程的组装器。
type Proxy struct {
	cfg       Config
	discovery *discovery.Manager
	metrics   *metrics.Metrics

	mu        sync.Mutex
	listeners map[string]*listenerEntry

	sessions      sync.Map // int64 -> *sessionEntry
	nextSessionID atomic.Int64

	// 连接池（per group）
	poolsMu sync.Mutex
	pools   map[string]*groupPools // group name -> pools

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

type listenerEntry struct {
	listener net.Listener
	group    string
	mode     string
	port     int
}

type sessionEntry struct {
	group   string
	session *session.Session
}

// groupPools 是一个 group 的共享连接池集合。
type groupPools struct {
	master   *backend.Pool
	replicas map[string]*backend.Pool // addr -> pool
}

// New 创建 Proxy。
func New(cfg Config) *Proxy {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 3 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 128
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.MaxRequestBytes == 0 {
		cfg.MaxRequestBytes = 16 * 1024 * 1024 // 16MB
	}

	p := &Proxy{
		cfg:       cfg,
		listeners: make(map[string]*listenerEntry),
		pools:     make(map[string]*groupPools),
		metrics:   metrics.New(),
	}

	if cfg.ServerEndpoint != "" {
		pollCfg := discovery.PollConfig{
			Interval: cfg.PollInterval,
			Timeout:  cfg.PollTimeout,
			Wait:     cfg.PollWait,
		}
		if pollCfg.Interval == 0 {
			pollCfg.Interval = 2 * time.Second
		}
		if pollCfg.Timeout == 0 {
			pollCfg.Timeout = 5 * time.Second
		}
		if pollCfg.Wait == 0 {
			pollCfg.Wait = 20 * time.Second
		}
		p.discovery = discovery.NewManager(discovery.ServerConfig{
			Endpoint: cfg.ServerEndpoint,
			Token:    cfg.ServerToken,
		}, pollCfg)
		p.discovery.OnUpdate(p.onTopologyChange)
	}

	return p
}

// ListenAndServe 开始监听并服务。
func (p *Proxy) ListenAndServe() error {
	// 启动 /metrics endpoint
	if p.cfg.MetricsListen != "" {
		go p.serveMetrics()
	}

	// 启动上游连接数上报（每 10s 刷新 SHARED 连接 Gauge）
	go p.reportUpstreamConns()

	if p.discovery != nil {
		return p.listenAndServeDiscovery()
	}
	return p.listenAndServeStatic()
}

// reportUpstreamConns 定期上报每个后端的 SHARED 连接数。
func (p *Proxy) reportUpstreamConns() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p.poolsMu.Lock()
		for group, pools := range p.pools {
			p.metrics.SetUpstreamConns(group, pools.master.Addr(), "master", pools.master.SharedConnCount())
			for addr, rp := range pools.replicas {
				p.metrics.SetUpstreamConns(group, addr, "replica", rp.SharedConnCount())
			}
		}
		p.poolsMu.Unlock()
	}
}

// serveMetrics 启动 Prometheus /metrics HTTP endpoint。
func (p *Proxy) serveMetrics() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", p.metrics.Handler())
	slog.Info("metrics listening", "addr", p.cfg.MetricsListen)
	if err := http.ListenAndServe(p.cfg.MetricsListen, mux); err != nil {
		slog.Error("metrics server error", "err", err)
	}
}

// listenAndServeDiscovery: snapshot-driven 模式（M4）。
func (p *Proxy) listenAndServeDiscovery() error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.discovery.Run(ctx)

	slog.Info("waiting for first snapshot", "server", p.cfg.ServerEndpoint)
	if err := p.discovery.WaitReady(ctx); err != nil {
		return err
	}

	topo := p.discovery.Topology()
	slog.Info("first snapshot received", "version", topo.Version, "groups", len(topo.Groups))

	p.openListeners(topo)

	// 保持进程存活即使 0 groups（等待后续 snapshot 动态添加 listener）。
	// signal handler 退出时 Done 释放这个 keep-alive。
	p.wg.Add(1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		defer p.wg.Done()
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		p.Shutdown()
	}()

	p.wg.Wait()
	return nil
}

// listenAndServeStatic: M3 兼容模式。
func (p *Proxy) listenAndServeStatic() error {
	ln, err := net.Listen("tcp", p.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.cfg.Listen, err)
	}
	slog.Info("proxy listening (static)", "listen", p.cfg.Listen, "master", p.cfg.MasterAddr)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		p.Shutdown()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				p.wg.Wait()
				return nil
			default:
				return fmt.Errorf("accept: %w", err)
			}
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handleConnStatic(conn)
		}()
	}
}

func (p *Proxy) onTopologyChange(newTopo, oldTopo *discovery.Topology) {
	if oldTopo == nil {
		return
	}
	for name, newGroup := range newTopo.Groups {
		var oldGroup *discovery.GroupTopology
		if oldTopo != nil {
			oldGroup = oldTopo.Groups[name]
		}
		if newGroup.MasterChanged(oldGroup) {
			oldAddr := "none"
			if oldGroup != nil {
				oldAddr = oldGroup.MasterAddr
			}
			slog.Info("master changed, closing sessions", "group", name, "old", oldAddr, "new", newGroup.MasterAddr)
			p.closePoolsForGroup(name)
			p.closeSessionsForGroup(name)
			p.metrics.IncBackendDown(name)
		}
	}
	p.updateListeners(newTopo)
}

func (p *Proxy) openListeners(topo *discovery.Topology) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, g := range topo.Groups {
		if g.AutoPort > 0 {
			p.ensureListenerLocked(name, "auto", g.AutoBind, g.AutoPort)
		}
		if g.MasterPort > 0 {
			p.ensureListenerLocked(name, "master", g.MasterBind, g.MasterPort)
		}
	}
}

func (p *Proxy) updateListeners(topo *discovery.Topology) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.listeners {
		if _, ok := topo.Groups[entry.group]; !ok {
			entry.listener.Close()
			delete(p.listeners, key)
		}
	}
	for name, g := range topo.Groups {
		if g.AutoPort > 0 {
			p.ensureListenerLocked(name, "auto", g.AutoBind, g.AutoPort)
		}
		if g.MasterPort > 0 {
			p.ensureListenerLocked(name, "master", g.MasterBind, g.MasterPort)
		}
	}
}

func (p *Proxy) ensureListenerLocked(group, mode, bind string, port int) {
	key := group + ":" + mode
	if existing, ok := p.listeners[key]; ok {
		if existing.port == port {
			return
		}
		existing.listener.Close()
		delete(p.listeners, key)
	}
	addr := fmt.Sprintf(":%d", port)
	if bind != "" {
		addr = fmt.Sprintf("%s:%d", bind, port)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "addr", addr, "group", group, "mode", mode, "err", err)
		return
	}
	entry := &listenerEntry{listener: ln, group: group, mode: mode, port: port}
	p.listeners[key] = entry
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.acceptLoop(ln, group, mode)
	}()
	slog.Info("proxy listening", "addr", addr, "group", group, "mode", mode)
}

func (p *Proxy) acceptLoop(ln net.Listener, group, mode string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handleConnDiscovery(conn, group, mode)
		}()
	}
}

func (p *Proxy) handleConnDiscovery(conn net.Conn, group, mode string) {
	p.metrics.IncClientConn(group)
	defer p.metrics.DecClientConn(group)

	topo := p.discovery.Topology()
	if topo == nil {
		conn.Close()
		return
	}
	g, ok := topo.Groups[group]
	if !ok {
		conn.Close()
		return
	}

	pools := p.getOrCreatePools(group, g)

	sessCfg := session.Config{
		Group:             group,
		MasterAddr:        g.MasterAddr,
		ReplicaAddrs:      g.ReplicaAddrs,
		Password:          g.Password,
		ConnectTimeout:    p.cfg.ConnectTimeout,
		MasterReadWeight:  p.cfg.MasterReadWeight,
		MaxRequestBytes:   p.cfg.MaxRequestBytes,
	}
	if mode == "master" {
		sessCfg.ReplicaAddrs = nil
	}

	sess, err := session.New(conn, sessCfg, pools.master, pools.replicas, p.metrics)
	if err != nil {
		slog.Error("session create failed", "err", err)
		return
	}

	id := p.nextSessionID.Add(1)
	entry := &sessionEntry{group: group, session: sess}
	p.sessions.Store(id, entry)
	defer p.sessions.Delete(id)

	p.metrics.IncClientConnTotal(group)
	if err := sess.Serve(); err != nil {
		slog.Error("session ended", "err", err)
		p.metrics.IncError(group)
	}
}

func (p *Proxy) handleConnStatic(conn net.Conn) {
	p.metrics.IncClientConn("_static")
	defer p.metrics.DecClientConn("_static")

	pools := p.getOrCreateStaticPools()

	sessCfg := session.Config{
		Group:             "_static",
		MasterAddr:        p.cfg.MasterAddr,
		ReplicaAddrs:      p.cfg.ReplicaAddrs,
		Password:          p.cfg.Password,
		ConnectTimeout:    p.cfg.ConnectTimeout,
		MasterReadWeight:  p.cfg.MasterReadWeight,
		MaxRequestBytes:   p.cfg.MaxRequestBytes,
	}

	sess, err := session.New(conn, sessCfg, pools.master, pools.replicas, p.metrics)
	if err != nil {
		slog.Error("session create failed", "err", err)
		return
	}

	if err := sess.Serve(); err != nil {
		slog.Error("session ended", "err", err)
		p.metrics.IncError("_static")
	}
}

// getOrCreatePools 获取或创建 group 的共享连接池。
func (p *Proxy) getOrCreatePools(group string, g *discovery.GroupTopology) *groupPools {
	p.poolsMu.Lock()
	defer p.poolsMu.Unlock()
	if pools, ok := p.pools[group]; ok {
		return pools
	}
	pools := &groupPools{
		master: backend.NewPool(g.MasterAddr, g.Password, p.cfg.ConnectTimeout, p.cfg.PoolSize, p.cfg.IdleTimeout),
		replicas: make(map[string]*backend.Pool),
	}
	for _, addr := range g.ReplicaAddrs {
		pools.replicas[addr] = backend.NewPool(addr, g.Password, p.cfg.ConnectTimeout, p.cfg.PoolSize, p.cfg.IdleTimeout)
	}
	p.pools[group] = pools
	slog.Info("created pools", "group", group, "master", g.MasterAddr, "replicas", len(g.ReplicaAddrs))
	return pools
}

// getOrCreateStaticPools 获取或创建 static 模式的连接池。
func (p *Proxy) getOrCreateStaticPools() *groupPools {
	p.poolsMu.Lock()
	defer p.poolsMu.Unlock()
	const group = "_static"
	if pools, ok := p.pools[group]; ok {
		return pools
	}
	pools := &groupPools{
		master: backend.NewPool(p.cfg.MasterAddr, p.cfg.Password, p.cfg.ConnectTimeout, p.cfg.PoolSize, p.cfg.IdleTimeout),
		replicas: make(map[string]*backend.Pool),
	}
	for _, addr := range p.cfg.ReplicaAddrs {
		pools.replicas[addr] = backend.NewPool(addr, p.cfg.Password, p.cfg.ConnectTimeout, p.cfg.PoolSize, p.cfg.IdleTimeout)
	}
	p.pools[group] = pools
	return pools
}

func (p *Proxy) closePoolsForGroup(group string) {
	p.poolsMu.Lock()
	defer p.poolsMu.Unlock()
	if pools, ok := p.pools[group]; ok {
		pools.master.Close()
		for _, rp := range pools.replicas {
			rp.Close()
		}
		delete(p.pools, group)
	}
}

func (p *Proxy) closeSessionsForGroup(group string) {
	p.sessions.Range(func(key, value any) bool {
		entry := value.(*sessionEntry)
		if entry.group == group {
			entry.session.Close()
			p.sessions.Delete(key)
		}
		return true
	})
}

// Shutdown 优雅关闭。
func (p *Proxy) Shutdown() {
	p.mu.Lock()
	for _, entry := range p.listeners {
		entry.listener.Close()
	}
	p.listeners = make(map[string]*listenerEntry)
	p.mu.Unlock()

	// 关闭所有池
	p.poolsMu.Lock()
	for _, pools := range p.pools {
		pools.master.Close()
		for _, rp := range pools.replicas {
			rp.Close()
		}
	}
	p.pools = make(map[string]*groupPools)
	p.poolsMu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("graceful shutdown complete")
	case <-time.After(p.cfg.ShutdownTimeout):
		slog.Warn("graceful shutdown timeout, forcing", "timeout", p.cfg.ShutdownTimeout)
	}
}
