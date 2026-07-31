package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// maxProxySnapshotWait 限制单次长轮询的挂起时长。
//
// 上限存在的意义不是省资源，而是保证这条路径永远会结束：任何"等待被唤醒"的机制都有
// 漏唤醒的可能，一旦漏了而又没有超时，Envoy 的配置就永久停在旧值，且不报任何错。
// 有了上限，最坏情况退化成原来的定期轮询，不会卡死。
const maxProxySnapshotWait = 30 * time.Second

// proxySnapshotPollInterval 是等待期间检查代际的间隔。
//
// 检查的是一个内存里的原子计数器，不碰数据库，所以间隔可以取小。只有代际真的变了
// 才会重算快照。
const proxySnapshotPollInterval = 100 * time.Millisecond

// proxySnapshot 返回 Envoy 代理配置快照，支持长轮询。
//
// 查询参数（都可选，不带时行为与原来完全一致）：
//
//	known_version  调用方已持有的快照版本
//	wait           最长等待时长，如 "20s"，上限 30s
//
// 带上这两个参数时：若当前版本与 known_version 相同，请求会挂起直到版本变化或超时；
// 超时仍未变化则返回 304 Not Modified。
//
// 这样做的收益有两条。一是把故障转移传播到 Envoy 的延迟从"最多一个轮询周期"降到接近
// 即时——在这段窗口里 Envoy 还把写流量发给已经挂掉的旧 master，写请求直接失败，缩短
// 窗口就是缩短写不可用时间。二是快照里含 Redis 明文密码（Envoy 需要它连后端），原来
// 每 2 秒就把这份密码完整传输一次；改成只在真正变化时传输。
//
// 实现上等的是 store 的代际计数器而不是重复重算快照：后者要读数据库+算哈希，用 100ms
// 间隔轮询它反而比原来 2 秒轮询一次更耗资源。代际是内存原子量，且语义刻意做弱——它变了
// 只说明"有写入"，不保证代理配置真的变了，所以变了之后仍要重算并比对版本，弱信号加强
// 比对既不会漏也不会误报。
func (s *Server) proxySnapshot(c *gin.Context) {
	snapshot, err := s.buildProxySnapshot()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	knownVersion := c.Query("known_version")
	wait := parseProxySnapshotWait(c.Query("wait"))
	if knownVersion == "" || wait <= 0 || snapshot.Version != knownVersion {
		ok(c, snapshot)
		return
	}

	deadline := time.Now().Add(wait)
	lastGen := s.db.Generation()
	ticker := time.NewTicker(proxySnapshotPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			// 调用方断开就别再算了
			return
		case <-ticker.C:
		}
		if gen := s.db.Generation(); gen != lastGen {
			lastGen = gen
			fresh, err := s.buildProxySnapshot()
			if err != nil {
				fail(c, http.StatusInternalServerError, err.Error())
				return
			}
			if fresh.Version != knownVersion {
				ok(c, fresh)
				return
			}
			// 代际动了但代理配置没变（例如只更新了备份时间戳），继续等
		}
		if time.Now().After(deadline) {
			c.Status(http.StatusNotModified)
			return
		}
	}
}

func parseProxySnapshotWait(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0
	}
	if d > maxProxySnapshotWait {
		return maxProxySnapshotWait
	}
	return d
}

func (s *Server) buildProxySnapshot() (*apitypes.ProxySnapshot, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		return nil, err
	}
	node, err := s.db.ReadNode()
	if err != nil {
		return nil, err
	}

	groups := s.buildInstanceGroups(instances, node)
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	snapshot := &apitypes.ProxySnapshot{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Listeners:   make([]apitypes.ProxyListener, 0),
		Clusters:    make([]apitypes.ProxyCluster, 0),
	}

	for _, groupName := range groupNames {
		g := groups[groupName]
		sortEnvoyEndpoints(g.masterEndpoints)
		sortEnvoyEndpoints(g.replicaEndpoints)

		masterClusterName := "redis-" + groupName + "-master-cluster"
		replicaClusterName := "redis-" + groupName + "-replica-cluster"
		statPrefix := "redis_" + strings.ReplaceAll(groupName, "-", "_")

		if g.masterPort > 0 && len(g.masterEndpoints) > 0 {
			snapshot.Listeners = append(snapshot.Listeners, apitypes.ProxyListener{
				Name:       "redis-" + groupName + "-master",
				Group:      groupName,
				Mode:       "master",
				Bind:       "0.0.0.0",
				Port:       g.masterPort,
				StatPrefix: statPrefix + "_master",
				Cluster:    masterClusterName,
				Password:   g.password,
				ReadPolicy: "MASTER",
			})
		}

		// auto listener（读写分离）：有 master 就保留。replica 全挂时读降级到
		// master（ReadCluster 留空，Envoy 不设 ReadCommandPolicy，所有命令都走
		// Cluster 指向的 master），不中断服务。replica 恢复后下一轮快照自动恢复
		// 读写分离。
		if g.autoPort > 0 && len(g.masterEndpoints) > 0 {
			readCluster := replicaClusterName
			if len(g.replicaEndpoints) == 0 {
				readCluster = "" // 无 replica，读也走 master
			}
			snapshot.Listeners = append(snapshot.Listeners, apitypes.ProxyListener{
				Name:        "redis-" + groupName + "-auto",
				Group:       groupName,
				Mode:        "auto",
				Bind:        "0.0.0.0",
				Port:        g.autoPort,
				StatPrefix:  statPrefix + "_auto",
				Cluster:     masterClusterName,
				ReadCluster: readCluster,
				Password:    g.password,
				ReadPolicy:  "MASTER",
			})
		}

		if len(g.masterEndpoints) > 0 {
			snapshot.Clusters = append(snapshot.Clusters, apitypes.ProxyCluster{
				Name:      masterClusterName,
				Password:  g.password,
				Endpoints: proxyEndpoints(g.masterEndpoints),
			})
		}
		if g.autoPort > 0 && len(g.replicaEndpoints) > 0 {
			snapshot.Clusters = append(snapshot.Clusters, apitypes.ProxyCluster{
				Name:      replicaClusterName,
				Password:  g.password,
				Endpoints: proxyEndpoints(g.replicaEndpoints),
			})
		}
	}

	snapshot.Version = proxySnapshotVersion(snapshot)
	return snapshot, nil
}

func sortEnvoyEndpoints(endpoints []envoyEndpoint) {
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Address == endpoints[j].Address {
			return endpoints[i].Port < endpoints[j].Port
		}
		return endpoints[i].Address < endpoints[j].Address
	})
}

func proxyEndpoints(endpoints []envoyEndpoint) []apitypes.ProxyEndpoint {
	out := make([]apitypes.ProxyEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		out = append(out, apitypes.ProxyEndpoint{Address: ep.Address, Port: ep.Port})
	}
	return out
}

func proxySnapshotVersion(snapshot *apitypes.ProxySnapshot) string {
	stable := struct {
		Listeners []apitypes.ProxyListener `json:"listeners"`
		Clusters  []apitypes.ProxyCluster  `json:"clusters"`
	}{
		Listeners: snapshot.Listeners,
		Clusters:  snapshot.Clusters,
	}
	data, _ := json.Marshal(stable)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
