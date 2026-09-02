// 总览：待处理事项 → 运行保障 → 轻量实例列表 → 最近任务 / 最近事件。
import { useMemo, useState } from 'react';
import {
  AlertCircle, AlertTriangle, Bot, CheckCircle2, ChevronRight,
  CircleGauge, Clock3, FileSearch, Network, RefreshCcw, RefreshCw, Settings2,
  ShieldCheck, Upload,
} from 'lucide-react';
import { CategoryBadge, EmptyState, EngineIcon, MemoryUsage, StatusBadge } from '../components/common.jsx';
import {
  auditActionLabel, auditResultMeta, auditTargetDisplay, formatDateTime, formatDuration,
  getField, INCIDENT_TYPE_LABELS, incidentStatusMeta, taskKindLabel, taskStatusMeta, taskTargetDisplay,
} from '../lib/format.js';
import { groupDisplayTone, topologyLabel } from '../lib/instance.js';

const cardClass = 'border border-gray-200 rounded-lg bg-white overflow-hidden';
const headClass = 'min-h-[46px] px-4 py-2.5 border-b border-gray-200 flex items-center justify-between gap-3';

function relativeTime(value) {
  if (!value) return '-';
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (!Number.isFinite(seconds)) return '-';
  if (seconds < 60) return `${seconds} 秒前`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟前`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} 小时前`;
  return `${Math.floor(seconds / 86400)} 天前`;
}

function incidentImpact(incident) {
  const labels = {
    replica_unavailable: '影响读写可用性',
    replica_missing: '无自动故障转移能力',
    backup_not_configured: '存在数据丢失风险',
    backup_task_failed: '最近备份不完整',
    restore_task_failed: '恢复能力不可用',
    instance_unreachable: '实例服务不可用',
    node_unreachable: '节点上的实例可能受影响',
  };
  return labels[incident?.incident_type] || incident?.reason || '需要进一步确认影响范围';
}

function IncidentAction({ incident, groups, onOpenIncidents, onOpenBackups, onOpenTasks }) {
  const group = groups.find((item) => item.groupName === incident.group_name);
  const className = 'h-8 px-3 rounded-md border border-gray-200 bg-white text-[12px] font-medium text-gray-700 hover:bg-gray-50 cursor-pointer';
  if (incident.incident_type === 'backup_not_configured' && group) {
    return <button type="button" className={className} onClick={() => onOpenBackups?.(group)}>配置备份</button>;
  }
  if (['backup_task_failed', 'restore_task_failed'].includes(incident.incident_type)) {
    return <button type="button" className={className} onClick={() => onOpenTasks?.(incident.instance_name || incident.group_name || '')}>查看任务</button>;
  }
  return <button type="button" className={className} onClick={() => onOpenIncidents?.(incident.instance_name || '')}>查看异常</button>;
}

function PendingItems({ summary, groups, onOpenIncidents, onOpenBackups, onOpenTasks }) {
  const items = summary?.top || [];
  return (
    <section className={cardClass}>
      <div className={headClass}>
        <h3 className="m-0 text-[15px] font-semibold">待处理事项</h3>
        {items.length > 0 && <button type="button" onClick={() => onOpenIncidents?.('')} className="border-0 bg-transparent text-[12px] text-gray-400 hover:text-gray-800 cursor-pointer">查看全部 <ChevronRight size={14} className="inline" /></button>}
      </div>
      {items.length === 0 ? (
        <div className="px-4 py-5 flex items-center gap-2 text-[13px] text-emerald-600"><CheckCircle2 size={17} /> 当前没有需要处理的事项</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[900px] border-collapse">
            <thead><tr>{['严重程度', '实例组', '问题', '持续时间', '影响范围', '推荐操作', ''].map((label, index) => <th key={`${label}-${index}`} className="px-4 py-2.5 border-b border-gray-200 text-left text-[11px] font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
            <tbody>{items.map((incident) => {
              const critical = incident.severity === 'critical';
              return <tr key={incident.id} className="border-b border-gray-100 last:border-b-0 hover:bg-gray-50/70">
                <td className="px-4 py-4"><span className={`inline-flex items-center gap-2 text-[13px] font-semibold ${critical ? 'text-red-700' : 'text-amber-700'}`}>{critical ? <AlertCircle size={17} className="text-red-500" /> : <AlertTriangle size={17} className="text-amber-500" />}{critical ? '严重' : '警告'}</span></td>
                <td className="px-4 py-4 text-[13px] font-medium text-gray-900">{incident.group_name || incident.object_id || '-'}</td>
                <td className="px-4 py-4"><strong className="block text-[13px] font-medium text-gray-800">{incident.summary || INCIDENT_TYPE_LABELS[incident.incident_type] || incident.incident_type}</strong></td>
                <td className="px-4 py-4 text-[13px] text-gray-600 tabular-nums whitespace-nowrap"><Clock3 size={14} className="inline mr-1.5 text-gray-400" />{incident.duration_ms > 0 ? formatDuration(Math.round(incident.duration_ms / 1000)) : '-'}</td>
                <td className="px-4 py-4 text-[12px] text-gray-600">{incidentImpact(incident)}</td>
                <td className="px-4 py-4"><IncidentAction incident={incident} groups={groups} onOpenIncidents={onOpenIncidents} onOpenBackups={onOpenBackups} onOpenTasks={onOpenTasks} /></td>
                <td className="px-3 py-4 text-gray-300"><ChevronRight size={16} /></td>
              </tr>;
            })}</tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function RuntimeAssurance({ servers, sentinel, proxyStatus }) {
  const healthyAgents = servers.filter((item) => ['healthy', 'drain'].includes(getField(item, 'status', 'Status'))).length;
  const sentinelNodes = sentinel?.nodes || [];
  const onlineSentinels = sentinelNodes.filter((node) => node.status?.running).length;
  const sentinelHealthy = !sentinel?.enabled || (sentinelNodes.length > 0 && onlineSentinels >= sentinel.quorum);
  const proxies = proxyStatus?.proxies || [];
  const onlineProxies = proxyStatus?.online || 0;
  const versions = new Set(proxies.filter((proxy) => proxy.status === 'online' && proxy.snapshot_version).map((proxy) => proxy.snapshot_version));
  const proxyHealthy = proxies.length > 0 && onlineProxies === proxies.length && versions.size <= 1 && proxies.every((proxy) => proxy.snapshot_version);
  const lastReconcileAt = servers.map((item) => getField(item, 'last_heartbeat', 'LastHeartbeat')).filter(Boolean).sort().at(-1);
  const cells = [
    { label: 'Agent', Icon: Bot, ok: servers.length > 0 && healthyAgents === servers.length, value: `${healthyAgents} / ${servers.length} 在线`, detail: servers.length ? '节点状态采集正常' : '尚未注册节点' },
    { label: 'Sentinel', Icon: ShieldCheck, ok: sentinelHealthy, value: sentinel?.enabled ? `${onlineSentinels} / ${sentinelNodes.length} 健康` : '未启用', detail: sentinel?.enabled ? `quorum=${sentinel.quorum}` : '当前不使用 Sentinel' },
    { label: 'Proxy', Icon: Network, ok: proxyHealthy, value: proxies.length ? `${onlineProxies} / ${proxies.length} 在线` : '等待状态上报', detail: proxies.length ? (versions.size > 1 ? '快照版本不一致' : proxyHealthy ? '快照一致' : '等待快照应用') : '重启 Proxy 后自动接入' },
    { label: '最后一次对账', Icon: CircleGauge, ok: Boolean(lastReconcileAt), value: lastReconcileAt ? relativeTime(lastReconcileAt) : '等待首次对账', detail: lastReconcileAt ? formatDateTime(lastReconcileAt) : 'Server 每 30 秒执行一次' },
  ];
  return <section className={`${cardClass} h-full`}>
    <div className={headClass}><h3 className="m-0 text-[15px] font-semibold">运行保障</h3></div>
    <div className="divide-y divide-gray-100">
      {cells.map(({ label, Icon, ok, value, detail }) => <div key={label} className="min-h-[88px] px-4 py-4 flex items-center gap-3.5">
        <div className={`w-9 h-9 rounded-lg grid place-items-center shrink-0 ${ok ? 'bg-emerald-50 text-emerald-600' : 'bg-gray-100 text-gray-500'}`}><Icon size={20} /></div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-3"><strong className="text-[13px] font-semibold text-gray-800">{label}</strong><span className={`text-[12px] font-semibold whitespace-nowrap ${ok ? 'text-emerald-600' : 'text-gray-500'}`}>{value}</span></div>
          <span className="block mt-1 text-[11px] text-gray-400 truncate" title={detail}>{detail}</span>
        </div>
      </div>)}
    </div>
  </section>;
}

function LabelFilter({ options, selected, onToggle, onClear }) {
  const buttonClass = (active) => `h-7 px-2.5 rounded-md border text-[11px] font-medium transition-colors cursor-pointer ${active ? 'border-blue-200 bg-blue-50 text-blue-700' : 'border-gray-200 bg-white text-gray-500 hover:border-gray-300 hover:text-gray-800'}`;
  return <div className="flex flex-wrap items-center gap-1.5" role="group" aria-label="按标签筛选实例">
    <span className="mr-1 text-[11px] font-medium text-gray-400">标签</span>
    <button type="button" onClick={onClear} className={buttonClass(selected.length === 0)} aria-pressed={selected.length === 0}>全部</button>
    {options.map((label) => <button key={label} type="button" onClick={() => onToggle(label)} className={buttonClass(selected.includes(label))} aria-pressed={selected.includes(label)}>{label}</button>)}
    {selected.length > 1 && <span className="ml-1 text-[10px] font-medium text-gray-400">AND 匹配</span>}
  </div>;
}

function InstanceList({ groups, onOpenDetail, onViewAll }) {
  const [labels, setLabels] = useState([]);
  const options = useMemo(() => [...new Set(groups.flatMap((group) => group.labels || []))].sort((a, b) => a.localeCompare(b)), [groups]);
  const filtered = useMemo(() => groups.filter((group) => labels.every((label) => (group.labels || []).includes(label))), [groups, labels]);
  const ordered = useMemo(() => filtered.slice(0, 6), [filtered]);
  return <section className={cardClass}>
    <div className={headClass}>
      <div className="flex items-center gap-3"><h3 className="m-0 text-[15px] font-semibold">实例列表</h3><span className="text-[12px] font-semibold text-gray-500">{filtered.length}</span></div>
      <button type="button" onClick={onViewAll} className="border-0 bg-transparent text-[11px] text-gray-400 hover:text-gray-800 cursor-pointer">查看全部 <ChevronRight size={13} className="inline" /></button>
    </div>
    <div className="min-h-[46px] px-4 py-2.5 border-b border-gray-200 flex items-center">
      <LabelFilter options={options} selected={labels} onToggle={(label) => setLabels((current) => current.includes(label) ? current.filter((item) => item !== label) : [...current, label])} onClear={() => setLabels([])} />
    </div>
    {ordered.length === 0 ? <EmptyState title="没有符合标签条件的实例" /> : <div className="overflow-x-auto"><table className="w-full min-w-[760px] border-collapse">
      <thead><tr>{['实例组', '类型', '拓扑', '标签', '内存使用', '状态'].map((label) => <th key={label} className="px-4 py-2.5 border-b border-gray-200 text-left text-[11px] font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
      <tbody>{ordered.map((group) => {
        const tone = group.incident?.severity === 'critical' ? 'bad' : group.incident?.count ? 'warn' : groupDisplayTone(group);
        const topology = group.replicas.length > 0 ? `主从（${topologyLabel(group, group.master)}）` : '单点';
        const rowTone = tone === 'bad' ? 'bg-red-50/40 hover:bg-red-50/70' : tone === 'warn' ? 'bg-amber-50/35 hover:bg-amber-50/70' : 'hover:bg-gray-50';
        return <tr key={group.groupName} onClick={() => group.master && onOpenDetail?.(group.master, group)} className={`border-b border-gray-100 last:border-b-0 cursor-pointer ${rowTone}`}>
          <td className="px-4 py-2.5"><span className="inline-flex items-center gap-2.5"><EngineIcon engine={group.engine} size={26} /><strong className="text-[13px] font-semibold text-gray-800">{group.groupName}</strong></span></td>
          <td className="px-4 py-2.5"><CategoryBadge category={group.category} /></td>
          <td className="px-4 py-2.5 text-[12px] text-gray-600 whitespace-nowrap">{topology}</td>
          <td className="px-4 py-2.5"><div className="flex items-center gap-1.5 whitespace-nowrap">{(group.labels || []).length ? <>{group.labels.slice(0, 3).map((label) => <span key={label} className="rounded-md border border-blue-100 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-600">{label}</span>)}{group.labels.length > 3 && <span className="text-[11px] font-medium text-gray-400">+{group.labels.length - 3}</span>}</> : <span className="text-[12px] text-gray-300">—</span>}</div></td>
          <td className="px-4 py-2.5"><MemoryUsage memory={group.resource?.memory} memoryUsed={group.resource?.memoryUsed} /></td>
          <td className="px-4 py-2.5 w-28">{group.incident?.count ? <StatusBadge status="incident" metaFn={() => incidentStatusMeta(group.incident)} /> : <StatusBadge status={group.status} />}</td>
        </tr>;
      })}</tbody>
    </table></div>}
  </section>;
}

function taskIcon(kind) {
  if (kind === 'backup.create') return Upload;
  if (kind === 'backup.restore' || kind === 'topology.failover') return RefreshCcw;
  if (kind === 'config.update') return Settings2;
  return FileSearch;
}

function RecentTasks({ tasks, onOpenTasks }) {
  return <section className={cardClass}>
    <div className={headClass}><h3 className="m-0 text-[15px] font-semibold">最近任务</h3><button type="button" onClick={() => onOpenTasks?.('')} className="border-0 bg-transparent text-[11px] text-gray-400 hover:text-gray-800 cursor-pointer">查看全部 <ChevronRight size={13} className="inline" /></button></div>
    {tasks.length === 0 ? <EmptyState title="暂无任务" /> : <div className="overflow-x-auto"><table className="w-full min-w-[520px] border-collapse">
      <thead><tr>{['类型', '对象', '状态', '时间'].map((label) => <th key={label} className="px-4 py-2.5 border-b border-gray-200 text-left text-[11px] font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
      <tbody>{tasks.slice(0, 4).map((task) => { const meta = taskStatusMeta(task.status); const TaskIcon = taskIcon(task.kind); return <tr key={task.id} onClick={() => onOpenTasks?.(task.instance_name || task.group_name || '')} className="border-b border-gray-100 last:border-b-0 hover:bg-gray-50 cursor-pointer"><td className="px-4 py-4 text-[12px] font-medium text-gray-700"><span className="inline-flex items-center gap-2"><TaskIcon size={16} className={meta.tone === 'bad' ? 'text-red-500' : meta.tone === 'ok' ? 'text-emerald-500' : 'text-blue-500'} />{taskKindLabel(task.kind)}</span></td><td className="px-4 py-4 text-[12px] text-gray-600">{taskTargetDisplay(task)}</td><td className={`px-4 py-4 text-[12px] font-semibold ${meta.tone === 'bad' ? 'text-red-600' : meta.tone === 'ok' ? 'text-emerald-600' : 'text-amber-600'}`}>{meta.label}</td><td className="px-4 py-4 text-[11px] text-gray-500 tabular-nums whitespace-nowrap"><Clock3 size={13} className="inline mr-1" />{formatDateTime(task.updated_at || task.created_at)}</td></tr>; })}</tbody>
    </table></div>}
  </section>;
}

function RecentEvents({ audits, onOpenAudit }) {
  return <section className={cardClass}>
    <div className={headClass}><h3 className="m-0 text-[15px] font-semibold">最近事件</h3><button type="button" onClick={onOpenAudit} className="border-0 bg-transparent text-[11px] text-gray-400 hover:text-gray-800 cursor-pointer">查看全部 <ChevronRight size={13} className="inline" /></button></div>
    {audits.length === 0 ? <EmptyState title="暂无事件" /> : <div className="overflow-x-auto"><table className="w-full min-w-[650px] border-collapse">
      <thead><tr>{['类型', '对象', '描述', '时间'].map((label) => <th key={label} className="px-4 py-2.5 border-b border-gray-200 text-left text-[11px] font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
      <tbody>{audits.slice(0, 4).map((log, index) => {
        const action = log.action || log.Action;
        const result = log.result || log.Result;
        const meta = auditResultMeta(result);
        const target = { ...(log.target || log.Target || {}), group_name: log.group_name || log.GroupName, instance_name: log.instance_name || log.InstanceName, object_id: log.object_id || log.ObjectID };
        const EventIcon = meta.tone === 'bad' ? AlertCircle : meta.tone === 'ok' ? CheckCircle2 : ShieldCheck;
        return <tr key={log.id || log.ID || index} onClick={onOpenAudit} className="border-b border-gray-100 last:border-b-0 hover:bg-gray-50 cursor-pointer"><td className={`px-4 py-4 text-[12px] font-semibold ${meta.tone === 'bad' ? 'text-red-600' : meta.tone === 'ok' ? 'text-emerald-600' : 'text-blue-600'}`}><span className="inline-flex items-center gap-2"><EventIcon size={16} />{auditActionLabel(action)}</span></td><td className="px-4 py-4 text-[12px] text-gray-600">{auditTargetDisplay(target)}</td><td className="px-4 py-4 text-[12px] text-gray-500 max-w-72 truncate">{log.summary || log.Summary || `${auditActionLabel(action)} · ${meta.label}`}</td><td className="px-4 py-4 text-[11px] text-gray-500 tabular-nums whitespace-nowrap">{formatDateTime(log.timestamp || log.Timestamp)}</td></tr>;
      })}</tbody>
    </table></div>}
  </section>;
}

export function DashboardView({ groups, servers, audits = [], tasks = [], sentinel, proxyStatus, incidentSummary, lastUpdatedAt, onRefresh, onOpenDetail, onOpenIncidents, onOpenBackups, onOpenTasks, onOpenAudit, onViewInstances }) {
  const total = incidentSummary?.total || 0;
  const critical = incidentSummary?.critical || 0;
  const warning = incidentSummary?.warning || 0;
  return <div className="grid gap-5">
    <section className="min-h-[64px] px-5 flex items-center gap-4 max-md:flex-col max-md:items-start max-md:px-0">
      <div className={`w-10 h-10 rounded-full border-2 grid place-items-center shrink-0 ${total ? 'border-red-500 text-red-500' : 'border-emerald-500 text-emerald-500'}`}>{total ? <AlertCircle size={24} /> : <CheckCircle2 size={24} />}</div>
      <div className="flex-1"><strong className="text-[16px] font-semibold text-gray-800">当前有 <span className={total ? 'text-red-600' : 'text-emerald-600'}>{total}</span> 项需要处理</strong>{total > 0 && <span className="ml-3 text-[13px] text-gray-400">· <b className="text-red-600 font-semibold">{critical} 严重</b> · <b className="text-amber-600 font-semibold">{warning} 警告</b></span>}</div>
      <span className="text-[11px] text-gray-400 tabular-nums whitespace-nowrap">最后更新：{formatDateTime(lastUpdatedAt)}</span>
      <button type="button" onClick={onRefresh} className="h-9 px-3 rounded-md border border-gray-200 bg-white text-[12px] font-medium text-gray-700 inline-flex items-center gap-2 hover:bg-gray-50 cursor-pointer"><RefreshCw size={15} />刷新</button>
    </section>
    <PendingItems summary={incidentSummary} groups={groups} onOpenIncidents={onOpenIncidents} onOpenBackups={onOpenBackups} onOpenTasks={onOpenTasks} />
    <div className="grid grid-cols-[minmax(0,2fr)_minmax(300px,1fr)] items-stretch gap-5 max-xl:grid-cols-1">
      <InstanceList groups={groups} onOpenDetail={onOpenDetail} onViewAll={onViewInstances} />
      <RuntimeAssurance servers={servers} sentinel={sentinel} proxyStatus={proxyStatus} />
    </div>
    <div className="grid grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] gap-4 max-xl:grid-cols-1"><RecentTasks tasks={tasks} onOpenTasks={onOpenTasks} /><RecentEvents audits={audits} onOpenAudit={onOpenAudit} /></div>
  </div>;
}
