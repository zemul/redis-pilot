import { useEffect, useState } from 'react';
import { Plus, RefreshCw, Search } from 'lucide-react';
import { aggregateClusters, clusterMembers, ordinaryGroups } from '../lib/customCluster.js';
import { buildInstanceGroups } from '../lib/instance.js';
import { CreateCustomClusterModal, ClusterAccessModal } from '../modals/CustomClusterModals.jsx';
import { CustomClusterCard } from '../components/CustomClusterCard.jsx';

export function CustomClustersView({ request, servers = {}, groups = [], isAdmin = false, publicHost, children }) {
  const [inventory, setInventory] = useState(null);
  const [error, setError] = useState('');
  const [reload, setReload] = useState(0);
  const [expanded, setExpanded] = useState({});
  const [query, setQuery] = useState('');
  const [modal, setModal] = useState(null);
  const [notice, setNotice] = useState('');
  useEffect(() => {
    let canceled = false, timer;
    const controller = new AbortController();
    const read = path => request(path, { signal: controller.signal });
    async function pending() {
      let records = [], page = 1;
      for (;;) {
        const data = await read(`/api/v1/custom-clusters/pending?page=${page}`);
        records.push(...(data.records || []));
        if (page * data.page_size >= data.total) return records;
        page++;
      }
    }
    async function load() {
      try {
        // Read pending plans before registered clusters so a plan completing
        // between requests remains represented on at least one side.
        const builds = await pending();
        const [clusters, runtime, instances] = await Promise.all([read('/api/v1/custom-clusters'), read('/api/v1/gateway/runtime'), read('/instance/list')]);
        if (!canceled) { setInventory({ clusters: aggregateClusters(clusters || [], builds), observations: runtime?.observations || [], groups: buildInstanceGroups(instances || {}) }); setError(''); }
      } catch (e) { if (!canceled) setError(e.message); }
      finally { if (!canceled) timer = setTimeout(load, 5000); }
    }
    load();
    return () => { canceled = true; controller.abort(); clearTimeout(timer); };
  }, [request, reload]);
  const clusters = inventory?.clusters || [];
  const instances = new Map((inventory?.groups || []).flatMap(g => g.instances.map(i => [i.name, i])));
  const selected = clusters.find(c => c.id === modal?.id);
  function refreshed() { setReload(v => v + 1); }
  return <div className="space-y-5">
    <section className="space-y-3" aria-label="自定义集群列表">
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-gray-200 bg-white p-4">
        <div><h2 className="text-sm font-semibold">自定义集群 <span className="font-normal text-gray-400">{clusters.length}</span></h2><p className="mt-1 text-xs text-gray-500">一个集群、统一入口；展开查看存储分片和缓存。热层不是副本。</p></div>
        <div className="flex flex-wrap items-center gap-2"><label className="flex items-center gap-2 rounded-md border border-gray-200 px-2 py-1.5 text-gray-400"><Search size={14} /><input aria-label="搜索自定义集群" className="w-40 text-xs text-gray-900 outline-none" placeholder="集群或成员名称" value={query} onChange={e => setQuery(e.target.value)} /></label><button className="rounded-md border p-2" onClick={() => setReload(v => v + 1)} aria-label="刷新集群"><RefreshCw size={14} /></button>
          {isAdmin && <button disabled={!inventory || !!error} className="flex items-center gap-1.5 rounded-md bg-gray-900 px-3 py-2 text-xs text-white disabled:opacity-40" onClick={() => setModal({ type: 'create' })}><Plus size={14} />创建集群</button>}
        </div>
      </div>
      {notice && <p role="status" className="text-sm text-gray-600">{notice}</p>}
      {error && <p role="alert" className="text-sm text-red-700">加载失败：{error}。已有数据仅供参考，集群操作已暂停，请刷新重试。</p>}
      {!inventory && !error && <p className="text-sm text-gray-500">正在读取集群归属…</p>}
      {inventory && !clusters.length && <p className="rounded-lg border bg-white p-5 text-sm text-gray-500">暂无自定义集群。{isAdmin ? '可创建 Kvrocks 集群，并选择是否启用 Redis 缓存。' : '请联系管理员创建。'}</p>}
      {clusters.filter(c => [c.id, c.gateway_id, ...clusterMembers(c).map(m => m.instance)].join(' ').toLowerCase().includes(query.trim().toLowerCase())).map(c => <CustomClusterCard key={c.id} cluster={c} observations={inventory.observations} instances={instances} expanded={!!expanded[c.id]} toggle={() => setExpanded(v => ({ ...v, [c.id]: !v[c.id] }))} error={error} isAdmin={isAdmin} publicHost={publicHost} onAction={type => setModal({ type, id: c.id })} />)}
    </section>
    {children && inventory && children(ordinaryGroups(inventory.groups, clusters).map(g => ({ ...g, incident: groups.find(old => old.groupName === g.groupName)?.incident })))}
    {modal?.type === 'create' && <CreateCustomClusterModal request={request} servers={servers} clusters={clusters} onClose={() => setModal(null)} onCreated={r => { setModal(null); setNotice(`已提交 ${r.cluster_id}，任务 ${r.task_id}。资源完成后仍需部署 Gateway 和配置入口。`); setExpanded(v => ({ ...v, [r.cluster_id]: true })); refreshed(); }} />}
    {selected && modal?.type !== 'create' && <ClusterAccessModal cluster={selected} mode={modal.type} request={request} onClose={() => setModal(null)} onSaved={() => { setModal(null); setNotice('入口已登记，等待 Gateway 确认后发布。'); refreshed(); }} />}
  </div>;
}
