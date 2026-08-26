package discovery

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// GroupTopology 是一个 group 的后端拓扑。
type GroupTopology struct {
	Name string
	// FrontPassword 是客户端 → Proxy 的鉴权密码（来自 snapshot 的 listener.Password）。
	// 这是本方案下唯一存在的密码：Proxy 校验客户端，后端实例本身无密码。
	FrontPassword string
	// BackendPassword 是 Proxy → 后端实例的鉴权密码（来自 snapshot 的 cluster.Password）。
	// 实例无密码方案下 Server 恒下发空串；保留字段以兼容"实例带密码"的历史/静态部署。
	BackendPassword string
	MasterAddr      string   // host:port
	ReplicaAddrs    []string // []host:port
	AutoPort        int      // 读写分离监听口（0=无）
	MasterPort      int      // 强制 master 监听口（0=无）
	AutoBind        string   // auto listener bind 地址
	MasterBind      string   // master listener bind 地址
	AccessMode      string   // rw-split | standard（空视为 rw-split）
}

// Topology 是全量拓扑（所有 group）。
type Topology struct {
	Groups  map[string]*GroupTopology // group name -> topology
	Version string
	// AnnounceIP 是 Proxy 对外暴露的域名/IP（来自 Server 的
	// server.yaml 的 proxy_public_endpoint），伪 Sentinel 应答时使用。
	AnnounceIP string
}

// ParseSnapshot 将 ProxySnapshot 解析为 Topology。
//
// 映射规则（proxy-design §11）：
//   - listener.Mode=="master" -> master 端口；其 Cluster 指向 master 集群
//   - listener.Mode=="auto"   -> auto 端口；其 Cluster 指向 master 集群，
//     ReadCluster 指向 replica 集群
//   - 前端密码取 listener.Password（客户端→Proxy）；后端密码取 cluster.Password（Proxy→实例）
func ParseSnapshot(snap *apitypes.ProxySnapshot) *Topology {
	// 构建 cluster 查找表
	clusters := make(map[string]*apitypes.ProxyCluster)
	for i := range snap.Clusters {
		clusters[snap.Clusters[i].Name] = &snap.Clusters[i]
	}

	groups := make(map[string]*GroupTopology)

	for _, listener := range snap.Listeners {
		g := groups[listener.Group]
		if g == nil {
			g = &GroupTopology{Name: listener.Group}
			groups[listener.Group] = g
		}

		// access_mode：取第一个非空值（同 group 的 listener 值一致）
		if g.AccessMode == "" && listener.AccessMode != "" {
			g.AccessMode = listener.AccessMode
		}

		// 前端密码：客户端连 Proxy 时用，来自 listener.Password
		if listener.Password != "" {
			g.FrontPassword = listener.Password
		}

		switch listener.Mode {
		case "master":
			g.MasterPort = listener.Port
			g.MasterBind = listener.Bind
			if c, ok := clusters[listener.Cluster]; ok {
				if g.MasterAddr == "" && len(c.Endpoints) > 0 {
					g.MasterAddr = endpointAddr(c.Endpoints[0])
				}
				if g.BackendPassword == "" {
					g.BackendPassword = c.Password
				}
			}

		case "auto":
			g.AutoPort = listener.Port
			g.AutoBind = listener.Bind
			// master 端点从 Cluster 取（auto listener 的 Cluster 指向 master 集群）
			if c, ok := clusters[listener.Cluster]; ok {
				if g.MasterAddr == "" && len(c.Endpoints) > 0 {
					g.MasterAddr = endpointAddr(c.Endpoints[0])
				}
				if g.BackendPassword == "" {
					g.BackendPassword = c.Password
				}
			}
			// replica 端点从 ReadCluster 取
			if listener.ReadCluster != "" {
				if c, ok := clusters[listener.ReadCluster]; ok {
					for _, ep := range c.Endpoints {
						g.ReplicaAddrs = append(g.ReplicaAddrs, endpointAddr(ep))
					}
				}
			}
		}
	}

	return &Topology{Groups: groups, Version: snap.Version, AnnounceIP: snap.AnnounceIP}
}

// endpointAddr 将 ProxyEndpoint 转为 "host:port" 字符串。
func endpointAddr(ep apitypes.ProxyEndpoint) string {
	return fmt.Sprintf("%s:%d", ep.Address, ep.Port)
}

// MasterChanged 比较两个 GroupTopology 的 master 地址是否变化。
func (g *GroupTopology) MasterChanged(old *GroupTopology) bool {
	if old == nil {
		return true // 新 group
	}
	return g.MasterAddr != old.MasterAddr
}

// ReplicasChanged 比较从库列表是否变化（增减或地址变动）。
func (g *GroupTopology) ReplicasChanged(old *GroupTopology) bool {
	if old == nil {
		return len(g.ReplicaAddrs) > 0
	}
	if len(g.ReplicaAddrs) != len(old.ReplicaAddrs) {
		return true
	}
	oldSet := make(map[string]struct{}, len(old.ReplicaAddrs))
	for _, addr := range old.ReplicaAddrs {
		oldSet[addr] = struct{}{}
	}
	for _, addr := range g.ReplicaAddrs {
		if _, ok := oldSet[addr]; !ok {
			return true
		}
	}
	return false
}

// PasswordChanged 比较前端或后端密码是否变化。
func (g *GroupTopology) PasswordChanged(old *GroupTopology) bool {
	if old == nil {
		return false
	}
	return g.FrontPassword != old.FrontPassword || g.BackendPassword != old.BackendPassword
}

// BindChanged 比较监听 bind 地址是否变化。
func (g *GroupTopology) BindChanged(old *GroupTopology) bool {
	if old == nil {
		return false
	}
	return g.AutoBind != old.AutoBind || g.MasterBind != old.MasterBind
}
