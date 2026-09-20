import { Dialog, DialogContent, DialogTitle, DialogDescription } from '../components/ui/dialog.jsx';
import { cachePolicy, clusterMembers, clusterRuntime, clusterPhases } from '../lib/customCluster.js';
import { getField, statusMeta } from '../lib/format.js';

const action = 'rounded-md border border-gray-200 bg-white px-3 py-1.5 text-xs hover:bg-gray-50 disabled:opacity-40';
export function CustomClusterDetailModal({ cluster: c, member, instances, observations, error, isAdmin, onAction, onClose }) {
  const creating = !!c.creation;
  const members = clusterMembers(c, instances).filter(m => !member || m.instance === member);
  return <Dialog open onOpenChange={open => { if (!open) onClose(); }}><DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
    <DialogTitle>{member ? `成员详情 · ${member}` : `集群详情 · ${c.id}`}</DialogTitle>
    <DialogDescription>{cachePolicy(c)} · {error ? '状态待刷新' : clusterRuntime(c, observations)}</DialogDescription>
    <div className="space-y-4">
      <p className="text-xs text-gray-600">Gateway：{c.gateway_id} · 所在节点：{c.node_id} · {creating ? '正在准备资源' : `拓扑版本 ${c.revision}`}。Gateway 是独立接入组件，不是数据库分片。</p>
      {creating && <div className="space-y-1 text-xs"><p>创建任务：{c.creation.id}</p><p>{c.creation.message || 'Server 正在创建并检查数据库，临时失败会自动重试。'}</p>{c.creation.error && <p role="alert" className="text-red-700">{c.creation.error}</p>}</div>}
      <div className="overflow-x-auto"><table className="w-full min-w-[680px] text-left text-xs"><thead className="text-gray-500"><tr>{['角色', '成员实例', '服务器', '资源规格', '状态 / 创建进度'].map(h => <th className="pb-2 font-medium" key={h}>{h}</th>)}</tr></thead><tbody>{members.map(m => {
        const i = instances.get(m.instance), task = c.children?.find(t => t.instance_name === m.instance);
        return <tr className="border-t" key={m.instance}><td className="py-3">{m.role}</td><td>{m.instance}<div className="mt-1 text-gray-400">{m.address || '后端地址待就绪'}</div></td><td>{i ? getField(i, 'server', 'Server') : task?.server_name || '—'}</td><td>{i ? `${getField(i, 'memory', 'Memory', '—')} / ${getField(i, 'cpus', 'CPUs', '—')} CPU` : '待同步'}</td><td>{task ? (clusterPhases[task.phase] || task.status) : i ? statusMeta(getField(i, 'status', 'Status', 'unknown')).label : '待确认'}{task?.message && <div className="mt-1 max-w-md break-words text-gray-500">{task.message}</div>}{task?.error && <div role="alert" className="text-red-700">{task.error}</div>}</td></tr>;
      })}</tbody></table></div>
      <p className="text-xs text-gray-500">成员由集群管理，不提供单实例删除、变配或主从切换入口。业务只连接 Proxy；入口发布状态不代表实时健康。</p>
      {c.task && <p className="text-xs">迁移：{c.task.id} · {({ pending: '等待执行', active: '迁移中', published: '清理迁移状态', complete: '已完成' })[c.task.phase] || c.task.phase} · {c.task.pages} 页 / {c.task.attempts} 次尝试{c.task.error_code && ` · ${c.task.error_code}`}</p>}
      {!member && !creating && isAdmin && <div className="flex flex-wrap gap-2"><button className={action} disabled={!!error || c.revision !== 1 || !!c.task || (c.cache_enabled !== false && !c.write_policy)} onClick={() => onAction('config')}>导出初始 Gateway 配置</button>{!c.ingress && <button className={action} disabled={!!error} onClick={() => onAction('ingress')}>配置统一入口</button>}<p className="w-full text-xs text-gray-500">仅初始拓扑且已声明缓存策略时可导出；旧集群保留原配置。已有运行状态不可重新初始化，服务仍由独立二进制部署。</p></div>}

    </div>
  </DialogContent></Dialog>;
}
