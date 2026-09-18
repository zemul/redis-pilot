import { ChevronDown, ChevronRight } from 'lucide-react';
import { clusterMembers, clusterRuntime, clusterPhases } from '../lib/customCluster.js';
import { CategoryBadge, EngineIcon, CopyableText, CopyableEndpoint, StatusBadge, MemoryUsage } from './common.jsx';
import { getField, parseGi } from '../lib/format.js';
import { instanceMemoryUsed, memoryMetricTitle } from '../lib/memory.js';

const action = 'h-7 px-3 rounded-md border border-gray-200 bg-white text-[12px] text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer';
export function CustomClusterCard({ cluster: c, group, gridClass, observations, instances, expanded, toggle, error, publicHost, onAction }) {
  const members = clusterMembers(c), creating = !!c.creation;
  const runtime = clusterRuntime(c, observations);
  const ready = !error && !creating && c.ingress?.ready && runtime === '运行正常';
  return <article className={`border rounded-xl bg-white transition-all ${expanded ? 'border-blue-200 shadow-sm' : 'border-gray-200 hover:border-gray-300 hover:shadow-sm'}`}>
    <div className={`${gridClass} py-3.5 cursor-pointer`} onClick={toggle}>
      <div className="flex items-center gap-2.5 min-w-0">
        <button className={`w-5 h-5 rounded inline-flex items-center justify-center shrink-0 ${expanded ? 'text-blue-500 bg-blue-50' : 'text-gray-400'}`} onClick={e => { e.stopPropagation(); toggle(); }} aria-expanded={expanded} aria-label={`${expanded ? '收起' : '展开'}集群 ${c.id}`}>{expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}</button>
        <EngineIcon engine="kvrocks" size={32} />
        <div className="min-w-0"><CopyableText text={c.id} className="font-semibold text-gray-900 text-[14px]" /><span className="block mt-0.5 text-[11px] text-gray-400">{c.shards?.length || 0} 分片{c.hot_instance ? ' · 1 缓存' : ''}</span></div>
      </div>
      <div><CategoryBadge category="persistent" /></div>
      <div><span className="inline-flex rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700">标准</span></div>
      <div className="text-[13px] text-gray-500">分片集群</div>
      <div className="text-[13px] text-gray-700">Kvrocks{c.cache_enabled !== false && (c.hot_instance || c.cache) ? ' + Redis' : ''}</div>
      <div className="text-xs text-gray-500">{group.labels.length ? group.labels.join(' · ') : '—'}</div>
      <div className="text-[13px]" onClick={e => e.stopPropagation()}>{c.ingress ? <CopyableEndpoint host={publicHost || c.ingress.node_id} port={c.ingress.port} /> : <span className="text-gray-400">待配置入口</span>}</div>
      <div className="text-gray-400 text-xs" title="展开查看各存储分片和缓存资源">按成员查看</div>
      <div title={error ? '状态待刷新' : creating ? (clusterPhases[c.creation.phase] || '创建中') : ready ? '业务可用' : runtime}><StatusBadge status={group.status} /></div>
      <div className="flex justify-end"><button className={action} onClick={e => { e.stopPropagation(); onAction('detail'); }}>详情</button></div>
    </div>
    {expanded && <div className="relative border-t border-blue-100 rounded-b-xl overflow-hidden bg-slate-50/80">
      <span className="absolute left-[26px] top-0 bottom-6 w-px bg-blue-200/80 pointer-events-none" aria-hidden="true" />
      {members.map((m, index) => {
        const i = instances.get(m.instance), task = c.children?.find(t => t.instance_name === m.instance);
        const engine = m.id === 'cache' ? 'redis' : c.engine || 'kvrocks';
        const cpus = Number(getField(i, 'cpus', 'CPUs', 0) || 0);
        const memory = parseGi(getField(i, 'memory', 'Memory', ''));
        const specs = [cpus ? `${cpus}C` : '', memory ? `${Number(memory.toFixed(3))}G` : ''].filter(Boolean).join(' · ');
        const server = getField(i, 'server', 'Server', task?.server_name || '');
        const port = getField(i, 'port', 'Port', '');
        return <div key={m.instance} className={`${gridClass} py-3 hover:bg-blue-50/60 transition-colors ${index ? 'border-t border-slate-200/70' : ''}`}>
          <div className="relative flex items-center gap-2.5 min-w-0 pl-[30px]">
            <span className="absolute left-[10px] top-1/2 w-4 h-px bg-blue-200/80 pointer-events-none" aria-hidden="true" />
            <span className={`text-[11px] font-medium px-1.5 py-0.5 rounded border shrink-0 ${m.id === 'cache' ? 'bg-slate-100 text-slate-600 border-slate-200' : 'bg-blue-50 text-blue-700 border-blue-100'}`}>{m.id === 'cache' ? '热缓存' : `分片 ${m.id}`}</span>
            <div className="min-w-0"><CopyableText text={m.instance} className="font-medium text-gray-800 text-[13px]" /><span className="block mt-0.5 text-[11px] text-slate-400 tabular-nums">{specs || '资源待就绪'}</span></div>
          </div>
          <div /><div /><div /><div /><div />
          <div className="text-[13px]">{server && port ? <CopyableEndpoint host={server} port={port} className="text-gray-600" /> : <span className="text-gray-400">{server || '待分配'}</span>}</div>
          <div title={memoryMetricTitle(engine)}><MemoryUsage memory={memory} memoryUsed={instanceMemoryUsed(i, engine)} /></div>
          <div title={task?.error || task?.message || ''}><StatusBadge status={task && task.status !== 'success' ? 'creating' : getField(i, 'status', 'Status', 'unknown')} /></div>
          <div className="flex justify-end items-center gap-1.5"><button className={action} aria-label={`成员 ${m.instance} 详情`} onClick={() => onAction('member', m.instance)}>详情</button></div>
        </div>;
      })}
    </div>}
  </article>;
}
