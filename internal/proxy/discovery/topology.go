package discovery

import (
	"fmt"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// GroupTopology 是一个 group 的后端拓扑。
type GroupTopology struct {
	Name         string
	Password     string
	MasterAddr   string   // host:port
	ReplicaAddrs []string  // []host:port
	AutoPort     int       // 读写分离监听口（0=无）
	MasterPort   int       // 强制 master 监听口（0=无）
	AutoBind     string   // auto listener bind 地址
	MasterBind   string   // master listener bind 地址
}

// Topology 是全量拓扑（所有 group）。
type Topology struct {
	Groups  map[string]*GroupTopology // group name -> topology
	Version string
}

// ParseSnapshot 将 ProxySnapshot 解析为 Topology。
//
// 映射规则（proxy-design §11）：
//   - listener.Mode=="master" -> master 端口；其 Cluster 指向 master 集群
//   - listener.Mode=="auto"   -> auto 端口；其 Cluster 指向 master 集群，
//     ReadCluster 指向 replica 集群
//   - 密码取 listener.Password 或 cluster.Password
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

		// 密码：listener 优先，cluster 兜底
		if listener.Password != "" {
			g.Password = listener.Password
		}

		switch listener.Mode {
		case "master":
			g.MasterPort = listener.Port
			g.MasterBind = listener.Bind
			if c, ok := clusters[listener.Cluster]; ok {
				if g.MasterAddr == "" && len(c.Endpoints) > 0 {
					g.MasterAddr = endpointAddr(c.Endpoints[0])
				}
				if g.Password == "" {
					g.Password = c.Password
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
				if g.Password == "" {
					g.Password = c.Password
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

	return &Topology{Groups: groups, Version: snap.Version}
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
