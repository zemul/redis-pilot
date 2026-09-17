import { useEffect, useState } from 'react';
import { clusterMembers, clusterRuntime, usesCache } from '../lib/customCluster.js';

const phases = { pending: '等待执行', active: '迁移中', published: '清理迁移状态', complete: '已完成' };

// Read-only aggregate: resource provisioning is not yet exposed as a one-click
// action. Do not turn registration success into a claim that a Gateway is ready.
export function CustomClustersView({ request }) {
  const [clusters, setClusters] = useState([]);
  const [observations, setObservations] = useState([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [reload, setReload] = useState(0);
  const [selected, setSelected] = useState('');
  useEffect(() => {
    let canceled = false;
    let timer;
    async function load() {
      try {
        const [items, runtime] = await Promise.all([
          request('/api/v1/custom-clusters'), request('/api/v1/gateway/runtime'),
        ]);
        if (!canceled) {
          setClusters(items || []);
          setObservations(runtime?.observations || []);
          setError('');
        }
      } catch (err) {
        if (!canceled) setError(err.message);
      } finally {
        if (!canceled) {
          setLoading(false);
          timer = setTimeout(load, 10000);
        }
      }
    }
    load();
    return () => { canceled = true; clearTimeout(timer); };
  }, [request, reload]);
  const cluster = clusters.find(c => c.id === selected);
  return <div className="space-y-5">
    <div className="flex items-center justify-between gap-4">
      <div><h2 className="text-lg font-semibold">自定义集群</h2>
        <p className="text-sm text-gray-500 mt-1">按集群查看存储分片、可选 Redis 缓存及统一接入信息。当前不支持分片副本自动切换。</p></div>
      <button className="border rounded-md px-3 py-2 text-sm" onClick={() => setReload(v => v + 1)}>刷新</button>
    </div>
    {error && <p role="alert" className="text-red-700">刷新失败：{error}。下方可能为上次数据，请勿据此判断当前健康状态。</p>}
    {loading && <p>加载中…</p>}
    {!loading && !error && !clusters.length && <p className="text-gray-500">暂无自定义集群。当前通过 CLI 登记专用实例；统一资源创建流程尚未开放。</p>}
    <div className="overflow-x-auto border rounded-lg bg-white">
      <table className="w-full text-left text-sm"><thead className="bg-gray-50"><tr>
        {['集群', '存储引擎', '缓存加速', '存储分片', 'Gateway 状态', '操作'].map(h => <th key={h} className="px-4 py-3 font-medium">{h}</th>)}
      </tr></thead><tbody>{clusters.map(c => <tr key={c.id} className="border-t">
        <td className="px-4 py-3">{c.id}</td><td className="px-4 py-3">{c.engine || 'kvrocks'}</td>
        <td className="px-4 py-3">{usesCache(c) ? 'Redis 缓存' : '关闭'}</td>
        <td className="px-4 py-3">{c.shards?.length || 0}</td>
        <td className="px-4 py-3">{error ? '状态待刷新' : clusterRuntime(c, observations)}</td>
        <td className="px-4 py-3"><button className="underline" onClick={() => setSelected(c.id)}>查看详情</button></td>
      </tr>)}</tbody></table>
    </div>
    {cluster && <section className="border rounded-lg bg-white p-5 space-y-4" aria-label="集群详情">
      <div className="flex justify-between"><h3 className="font-semibold">{cluster.id}</h3><button onClick={() => setSelected('')}>收起</button></div>
      <p className="text-sm">Gateway：{cluster.gateway_id} · 所在节点：{cluster.node_id} · 拓扑版本：{cluster.revision}</p>
      <p className="text-sm">接入：{cluster.ingress ? `Proxy 节点 ${cluster.ingress.node_id}，端口 ${cluster.ingress.port}；${cluster.ingress.ready ? '已发布' : '等待 Gateway 确认'}；${cluster.ingress.has_password ? '需要业务密码' : '未设置业务密码'}` : '尚未配置统一入口'}</p>
      <p className="text-xs text-gray-500">入口发布状态不代表实时健康。业务连接 Proxy，不直接连接下列数据库端点。</p>
      <table className="w-full text-left text-sm"><thead><tr>{['角色', '实例', '后端地址'].map(h => <th key={h} className="py-2">{h}</th>)}</tr></thead>
        <tbody>{clusterMembers(cluster).map(m => <tr key={m.instance} className="border-t"><td className="py-2">{m.role}</td><td>{m.instance}</td><td>{m.address}</td></tr>)}</tbody></table>
      {cluster.task && <div className="text-sm space-y-1">
        <p>最近迁移：{cluster.task.id} · {phases[cluster.task.phase] || cluster.task.phase}</p>
        <p>目标分片：{cluster.task.target?.map(s => s.id).join('、')}；已扫描页数：{cluster.task.pages}；尝试次数：{cluster.task.attempts}</p>
        {cluster.task.error_code && <p role="alert">迁移提示：{cluster.task.error_code}</p>}
      </div>}
    </section>}
  </div>;
}
