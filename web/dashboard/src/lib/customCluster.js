export function usesCache(cluster) {
  return cluster.cache_enabled !== false;
}

export function clusterMembers(cluster) {
  const rows = [];
  if (usesCache(cluster) && cluster.hot_instance) {
    rows.push({ id: 'cache', instance: cluster.hot_instance, address: cluster.hot_address, role: 'Redis 缓存' });
  }
  for (const shard of cluster.shards || []) {
    rows.push({ ...shard, role: `存储分片 ${shard.id}` });
  }
  return rows;
}

export function clusterRuntime(cluster, observations = []) {
  const entries = observations.filter(o => o.gateway_id === cluster.gateway_id && o.cluster_id === cluster.id && o.node_id === cluster.node_id);
  if (entries.some(o => o.status === 'conflict')) return '运行冲突';
  if (entries.some(o => o.status === 'ready' && o.serving && o.backend_ready)) return '运行正常';
  if (entries.some(o => o.status === 'draining')) return '正在停止接入';
  if (entries.some(o => o.status === 'unready')) return '尚未就绪';
  return '未收到有效心跳';
}
