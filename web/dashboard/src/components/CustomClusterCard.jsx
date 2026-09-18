import { ChevronDown, ChevronRight } from 'lucide-react';
import { cachePolicy, clusterMembers, clusterRuntime, clusterPhases } from '../lib/customCluster.js';
import { CategoryBadge, EngineIcon, CopyableText, CopyableEndpoint, StatusBadge } from './common.jsx';
import { getField, statusMeta } from '../lib/format.js';

const action = 'rounded-md border border-gray-200 bg-white px-3 py-1.5 text-xs hover:bg-gray-50 disabled:opacity-40';
export function CustomClusterCard({ cluster: c, group, gridClass, observations, instances, expanded, toggle, error, isAdmin, publicHost, onAction }) {
  const members = clusterMembers(c), creating = !!c.creation;
  const runtime = clusterRuntime(c, observations);
  const ready = !error && !creating && c.ingress?.ready && runtime === '运行正常';
  return <article className="overflow-hidden rounded-lg border border-gray-200 bg-white">
    <div className={`${gridClass} py-3.5 cursor-pointer`} onClick={toggle}>
      <div className="flex items-center gap-2.5 min-w-0">
        <button className="w-5 h-5 shrink-0 text-gray-400" onClick={e => { e.stopPropagation(); toggle(); }} aria-expanded={expanded} aria-label={`${expanded ? '收起' : '展开'}集群 ${c.id}`}>{expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}</button>
        <EngineIcon engine="kvrocks" size={32} />
        <div className="min-w-0"><CopyableText text={c.id} className="font-semibold text-gray-900 text-[14px]" /><span className="block mt-0.5 text-[11px] text-gray-400">{cachePolicy(c)}</span></div>
      </div>
      <div><CategoryBadge category="persistent" /></div>
      <div className="text-xs text-slate-700">统一入口</div>
      <div className="text-[13px] text-gray-500">分片集群<span className="block text-[11px]">{c.shards?.length || c.cold?.count || 0} 分片{c.cache_enabled !== false && (c.hot_instance || c.cache) ? ' · 缓存' : ''}</span></div>
      <div className="text-[13px] text-gray-700">Kvrocks{c.cache_enabled !== false && (c.hot_instance || c.cache) ? ' + Redis' : ''}</div>
      <div className="text-xs text-gray-500">{group.labels.length ? group.labels.join(' · ') : '—'}</div>
      <div className="text-[13px]" onClick={e => e.stopPropagation()}>{c.ingress ? <CopyableEndpoint host={publicHost || c.ingress.node_id} port={c.ingress.port} /> : <span className="text-gray-400">待配置入口</span>}</div>
      <div className="text-gray-400 text-xs" title="展开查看各存储分片和缓存资源">按成员查看</div>
      <div title={error ? '状态待刷新' : creating ? (clusterPhases[c.creation.phase] || '创建中') : ready ? '业务可用' : runtime}><StatusBadge status={group.status} /></div>
      <div className="flex justify-end"><button className={action} onClick={e => { e.stopPropagation(); toggle(); }}>{expanded ? '收起' : '详情'}</button></div>
    </div>
    {expanded && <div className="space-y-4 border-t bg-gray-50/50 p-4">
      <p className="text-xs text-gray-600">Gateway：{c.gateway_id} · 所在节点：{c.node_id} · {creating ? '正在准备资源' : `拓扑版本 ${c.revision}`}。Gateway 是独立接入组件，不是数据库分片。</p>
      {creating && <div className="space-y-1 text-xs"><p>创建任务：{c.creation.id}</p><p>{c.creation.message || 'Server 正在创建并检查数据库，临时失败会自动重试。'}</p>{c.creation.error && <p role="alert" className="text-red-700">{c.creation.error}</p>}</div>}
      <div className="overflow-x-auto"><table className="w-full min-w-[680px] text-left text-xs"><thead className="text-gray-500"><tr>{['角色', '成员实例', '服务器', '资源规格', '状态 / 创建进度'].map(h => <th className="pb-2 font-medium" key={h}>{h}</th>)}</tr></thead><tbody>{members.map(m => {
        const i = instances.get(m.instance), task = c.children?.find(t => t.instance_name === m.instance);
        return <tr className="border-t" key={m.instance}><td className="py-3">{m.role}</td><td>{m.instance}<div className="mt-1 text-gray-400">{m.address || '后端地址待就绪'}</div></td><td>{i ? getField(i, 'server', 'Server') : task?.server_name || '—'}</td><td>{i ? `${getField(i, 'memory', 'Memory', '—')} / ${getField(i, 'cpus', 'CPUs', '—')} CPU` : '待同步'}</td><td>{task ? (clusterPhases[task.phase] || task.status) : i ? statusMeta(getField(i, 'status', 'Status', 'unknown')).label : '待确认'}{task?.message && <div className="mt-1 max-w-md break-words text-gray-500">{task.message}</div>}{task?.error && <div role="alert" className="text-red-700">{task.error}</div>}</td></tr>;
      })}</tbody></table></div>
      <p className="text-xs text-gray-500">成员由集群管理，不提供单实例删除、变配或主从切换入口。业务只连接 Proxy；入口发布状态不代表实时健康。</p>
      {c.task && <p className="text-xs">迁移：{c.task.id} · {({ pending: '等待执行', active: '迁移中', published: '清理迁移状态', complete: '已完成' })[c.task.phase] || c.task.phase} · {c.task.pages} 页 / {c.task.attempts} 次尝试{c.task.error_code && ` · ${c.task.error_code}`}</p>}
      {!creating && isAdmin && <div className="flex flex-wrap gap-2"><button className={action} disabled={!!error || c.revision !== 1 || !!c.task || (c.cache_enabled !== false && !c.write_policy)} onClick={() => onAction('config')}>导出初始 Gateway 配置</button>{!c.ingress && <button className={action} disabled={!!error} onClick={() => onAction('ingress')}>配置统一入口</button>}<p className="w-full text-xs text-gray-500">仅初始拓扑且已声明缓存策略时可导出；旧集群保留原配置。已有运行状态不可重新初始化，服务仍由独立二进制部署。</p></div>}
    </div>}
  </article>;
}
