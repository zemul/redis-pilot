import { ChevronDown, ChevronRight } from 'lucide-react';
import { cachePolicy, clusterMembers, clusterRuntime, clusterPhases } from '../lib/customCluster.js';
import { getField, statusMeta } from '../lib/format.js';

const action = 'rounded-md border border-gray-200 bg-white px-3 py-1.5 text-xs hover:bg-gray-50 disabled:opacity-40';
export function CustomClusterCard({ cluster: c, observations, instances, expanded, toggle, error, isAdmin, publicHost, onAction }) {
  const members = clusterMembers(c), creating = !!c.creation;
  const runtime = clusterRuntime(c, observations);
  const ready = !error && !creating && c.ingress?.ready && runtime === '运行正常';
  return <article className="overflow-hidden rounded-lg border border-gray-200 bg-white">
    <div className="flex flex-wrap items-center gap-x-6 gap-y-3 p-4">
      <button className="flex min-w-44 items-center gap-2 text-sm font-semibold" onClick={toggle} aria-expanded={expanded} aria-label={`展开集群 ${c.id}`}>{expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}{c.id}</button>
      <div className="min-w-32 text-xs"><div>Kvrocks · {c.shards?.length || 0} 分片</div><div className="mt-1 text-gray-500">{cachePolicy(c)}</div></div>
      <div className="min-w-36 text-xs"><div>{error ? '状态待刷新' : creating ? (clusterPhases[c.creation.phase] || '创建中') : ready ? '业务可用' : '待接入 / Gateway 待就绪'}</div><div className="mt-1 text-gray-500">{creating ? `${c.children.filter(t => t.status === 'success').length}/${members.length} 个资源已就绪` : runtime}</div></div>
      <div className="min-w-48 flex-1 text-xs">{c.ingress ? <>Proxy：{publicHost || `节点 ${c.ingress.node_id}`}:{c.ingress.port}<div className="mt-1 text-gray-500">{c.ingress.ready ? '入口已发布' : '等待 Gateway 确认'} · {c.ingress.has_password ? '需要业务密码' : '未设置业务密码'}</div></> : <span className="text-gray-400">统一入口尚未配置</span>}</div>
      <button className={action} onClick={toggle}>{expanded ? '收起' : '详情'}</button>
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
