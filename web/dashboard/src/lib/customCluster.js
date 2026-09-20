import { getField } from './format.js';

// Logical clusters share list filtering with ordinary instance groups, but never
// inherit their single-instance destructive actions.
export function clusterListGroups(clusters, instances, observations, error) {
  return clusters.map(c => {
    const members = clusterMembers(c, instances).map(m => instances.get(m.instance) || { name: m.instance });
    return { cluster: c, groupName: c.id, engine: c.engine || 'kvrocks',
      category: 'persistent', type: 'sharded', engineVersion: '',
      status: error ? 'unknown' : c.creation ? 'creating' : c.ingress?.ready && clusterRuntime(c, observations) === '运行正常' ? 'running' : 'pending',
      labels: [...new Set(members.flatMap(i => getField(i, 'labels', 'Labels', []) || []))],
      instances: members,
    };
  });
}

export function usesCache(cluster) {
  return cluster.cache_enabled !== false;
}

export function clusterMembers(cluster, instances = new Map()) {
  const rows = [];
  if (usesCache(cluster) && cluster.hot_instance) {
    rows.push({ id: 'cache', instance: cluster.hot_instance, address: cluster.hot_address, role: 'Redis 缓存' });
  }
  for (const shard of cluster.shards || []) {
    const anchor = instances.get(shard.instance);
    const group = getField(anchor, 'group', 'Group', '');
    const members = group ? [...instances.entries()].filter(([, i]) => getField(i, 'group', 'Group', '') === group) : [];
    if (members.length > 1) {
      members.sort(([an, a], [bn, b]) => Number(getField(b, 'role', 'Role') === 'master') - Number(getField(a, 'role', 'Role') === 'master') || an.localeCompare(bn));
      for (const [name, i] of members) {
        const replica = getField(i, 'role', 'Role') === 'replica';
        rows.push({ id: shard.id, instance: name, replica, role: `分片 ${shard.id} · ${replica ? '从库' : '主库'}` });
      }
    } else rows.push({ ...shard, role: `存储分片 ${shard.id}` });
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

export const clusterPhases = { queued: '等待创建', waiting_resources: '等待资源就绪', retry_wait: '等待自动重试', database_ready: '数据库已就绪', cluster_registered: '资源创建完成', creating: '创建中', verifying: '验证数据库' };
export function cachePolicy(cluster) {
  return !usesCache(cluster) ? '无缓存' : ({ invalidate: '删除缓存', update_existing: '双写更新' }[cluster.write_policy] || 'Redis 缓存（策略由 Gateway 配置）');
}
export function aggregateClusters(clusters = [], pending = []) {
  const rows = new Map(pending.map(p => [p.spec.id, { ...p.spec, creation: p.task, children: p.children || [] }]));
  for (const c of clusters) rows.set(c.id, c);
  return [...rows.values()];
}
export function ordinaryGroups(groups, clusters) {
  const owned = new Set(clusters.flatMap(c => [...clusterMembers(c).map(m => m.instance), ...(c.task?.target || []).map(m => m.instance), ...(c.task && c.task.phase !== 'complete' ? c.task.source || [] : []).map(m => m.instance)]));
  return groups.filter(g => !g.instances.some(i => owned.has(i.name)));
}
export function makeClusterSpec(form) {
	const engine = form.engine || 'kvrocks';
	if (!['redis', 'kvrocks'].includes(engine)) throw new Error('请选择 Redis 或 Kvrocks');
	if (engine === 'redis' && form.cache) throw new Error('Redis 分片不需要额外热缓存');
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/.test(form.id)) throw new Error('名称需为 1–32 位字母、数字、下划线或连字符，以字母或数字开头');
  if (!form.gateway_id || !form.node_id) throw new Error('请选择 Gateway 和部署节点');
  const count = Number(form.count);
  if (!Number.isInteger(count) || count < 1 || count > 128) throw new Error('分片数量需为 1–128');
  const resource = (memory, cpus, disk) => {
    if (!/^\d+(\.\d+)?(Mi|Gi|M|G)$/.test(memory) || parseFloat(memory) <= 0) throw new Error('内存请输入有效容量，如 256Mi 或 2Gi');
    if (!Number.isFinite(Number(cpus)) || Number(cpus) <= 0) throw new Error('CPU 必须大于 0');
    if (disk && (!/^\d+(\.\d+)?(Mi|Gi|M|G)$/.test(disk) || parseFloat(disk) <= 0)) throw new Error('磁盘请输入有效容量，如 10Gi');
    return { memory, cpus: Number(cpus), ...(disk ? { disk } : {}) };
  };
  const cold = { count, ...resource(form.cold_memory, form.cold_cpus, form.cold_disk), servers: Array.from({ length: count }, (_, i) => form.servers[i] || '') };
  const replicas = Number(form.replicas || 0);
  if (!Number.isInteger(replicas) || replicas < 0 || replicas > 2) throw new Error('每分片副本数需为 0–2');
  if (replicas) cold.replicas = replicas;
  return { id: form.id, engine, gateway_id: form.gateway_id, node_id: form.node_id, cold,
    ...(form.cache ? { cache: { ...resource(form.hot_memory, form.hot_cpus, ''), server: form.hot_server, write_policy: form.write_policy } } : {}) };
}
