import { useEffect, useState } from 'react';
import { aggregateClusters, clusterListGroups, ordinaryGroups } from '../lib/customCluster.js';
import { buildInstanceGroups } from '../lib/instance.js';
import { CreateCustomClusterModal, ClusterAccessModal } from '../modals/CustomClusterModals.jsx';
import { CustomClusterCard } from '../components/CustomClusterCard.jsx';
import { CustomClusterDetailModal } from '../modals/CustomClusterDetailModal.jsx';

export function CustomClustersView({ request, servers = {}, groups = [], isAdmin = false, publicHost, children }) {
  const [inventory, setInventory] = useState(null);
  const [error, setError] = useState('');
  const [reload, setReload] = useState(0);
  const [expanded, setExpanded] = useState({});
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
  const instances = new Map((inventory?.groups || []).flatMap(g => g.instances.map(i => [i.name, { ...i, labels: g.labels || [] }])));
  const selected = clusters.find(c => c.id === modal?.id);
  function refreshed() { setReload(v => v + 1); }
  return <div className="space-y-5">
    {notice && <p role="status" className="text-sm text-gray-600">{notice}</p>}
    {error && <p role="alert" className="text-sm text-red-700">加载失败：{error}。已有数据仅供参考，集群操作已暂停。<button className="ml-2 underline" onClick={refreshed}>重新加载</button></p>}
    {!inventory && !error && <p className="text-sm text-gray-500">正在读取实例…</p>}
    {inventory && children([
      ...ordinaryGroups(inventory.groups, clusters).map(g => ({ ...g, incident: groups.find(old => old.groupName === g.groupName)?.incident })),
      ...clusterListGroups(clusters, instances, inventory.observations, error),
    ].sort((a, b) => a.groupName.localeCompare(b.groupName)), {
      onCreate: isAdmin && !error ? () => setModal({ type: 'create' }) : null,
      canCreate: isAdmin,
      refresh: refreshed,
      render: (group, gridClass) => <CustomClusterCard key={`cluster:${group.groupName}`} cluster={group.cluster} group={group} gridClass={gridClass} observations={inventory.observations} instances={instances} expanded={!!expanded[group.groupName]} toggle={() => setExpanded(v => ({ ...v, [group.groupName]: !v[group.groupName] }))} error={error} isAdmin={isAdmin} publicHost={publicHost} onAction={(type, member) => setModal({ type, member, id: group.groupName })} />,
    })}
    {modal?.type === 'create' && <CreateCustomClusterModal request={request} servers={servers} clusters={clusters} onClose={() => setModal(null)} onCreated={r => { setModal(null); setNotice(`已提交 ${r.cluster_id}，任务 ${r.task_id}。资源完成后仍需部署 Gateway 和配置入口。`); setExpanded(v => ({ ...v, [r.cluster_id]: true })); refreshed(); }} />}
    {selected && ['detail', 'member'].includes(modal?.type) && <CustomClusterDetailModal cluster={selected} member={modal.member} instances={instances} observations={inventory.observations} error={error} isAdmin={isAdmin} onClose={() => setModal(null)} onAction={type => setModal({ type, id: selected.id })} />}
    {selected && ['config', 'ingress'].includes(modal?.type) && <ClusterAccessModal cluster={selected} mode={modal.type} request={request} onClose={() => setModal(null)} onSaved={() => { setModal(null); setNotice('入口已登记，等待 Gateway 确认后发布。'); refreshed(); }} />}
  </div>;
}
