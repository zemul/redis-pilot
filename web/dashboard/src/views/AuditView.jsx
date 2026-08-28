// 操作审计视图：按来源/结果快速分类，主表展示变更摘要，详情抽屉保留完整上下文。
import { useState, useEffect } from 'react';
import { SlidersHorizontal, List, AlertCircle, CheckCircle2, ChevronRight, X } from 'lucide-react';
import { EmptyState, StatusBadge } from '../components/common.jsx';
import {
  auditQuery, displayValue, formatDateTime, auditOperator, auditActionLabel,
  auditTargetDisplay, auditLevelLabel, auditResultMeta, auditSourceLabel,
  auditTriggerLabel, auditPhaseLabel,
} from '../lib/format.js';

const EMPTY_FILTERS = {
  from: '', to: '', q: '', actor_type: '', source: '', level: '', action: '', result: '', operator: '', group: '', instance: ''
};

const PRESETS = [
  { id: 'all', label: '全部变更', values: { actor_type: '', result: '', level: '' } },
  { id: 'human', label: '人工操作', values: { actor_type: 'user', result: '', level: '' } },
  { id: 'system', label: '系统动作', values: { actor_type: 'system', result: '', level: '' } },
  { id: 'failed', label: '失败操作', values: { actor_type: '', result: 'failed', level: '' } },
  { id: 'critical', label: '关键操作', values: { actor_type: '', result: '', level: 'critical' } },
];

const ACTIONS = [
  'instance.create', 'instance.delete', 'instance.start', 'instance.stop', 'instance.access_mode',
  'config.update', 'topology.failover', 'topology.replicate', 'backup.create', 'backup.restore',
  'backup.schedule', 'node.add', 'node.remove', 'node.update', 'user.create', 'user.delete',
  'reconcile.replica_drift', 'reconcile.role_drift',
];

export function AuditView({ filters, setFilters, request }) {
  const [records, setRecords] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState(null);
  const [preset, setPreset] = useState('all');
  const pageSize = 30;

  async function fetchAudit(p = page, nextFilters = filters) {
    setLoading(true);
    setError('');
    try {
      const data = await request(auditQuery(nextFilters, p, pageSize));
      setRecords(data?.records || data?.Records || []);
      setTotal(data?.total || 0);
    } catch (err) {
      setError(err.message || String(err));
      setRecords([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { fetchAudit(1); setPage(1); }, []);

  function updateFilter(key, value) {
    setPreset('custom');
    setFilters((current) => ({ ...current, [key]: value }));
  }

  function applyPreset(item) {
    const next = { ...filters, ...item.values };
    setPreset(item.id);
    setFilters(next);
    setPage(1);
    fetchAudit(1, next);
  }

  function resetFilters() {
    setPreset('all');
    setFilters(EMPTY_FILTERS);
    setPage(1);
    fetchAudit(1, EMPTY_FILTERS);
  }

  function handleApply() {
    setPage(1);
    fetchAudit(1, filters);
  }

  function goPage(p) {
    setPage(p);
    fetchAudit(p, filters);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="grid gap-4">
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="px-4 pt-3 border-b border-gray-200 flex gap-5 overflow-x-auto">
          {PRESETS.map((item) => (
            <button key={item.id} type="button" onClick={() => applyPreset(item)}
              className={`pb-3 text-[13px] font-medium whitespace-nowrap border-0 border-b-2 bg-transparent cursor-pointer ${preset === item.id ? 'border-gray-900 text-gray-900' : 'border-transparent text-gray-400 hover:text-gray-700'}`}>
              {item.label}
            </button>
          ))}
        </div>
        <div className="p-4">
          <div className="grid grid-cols-6 gap-3 max-xl:grid-cols-3 max-md:grid-cols-1">
            <Filter label="开始日期"><input type="date" className={inputClass} value={filters.from} onChange={(e) => updateFilter('from', e.target.value)} /></Filter>
            <Filter label="结束日期"><input type="date" className={inputClass} value={filters.to} onChange={(e) => updateFilter('to', e.target.value)} /></Filter>
            <Filter label="目标或操作者"><input className={inputClass} value={filters.q} onChange={(e) => updateFilter('q', e.target.value)} placeholder="实例、组、节点或用户" /></Filter>
            <Filter label="来源"><select className={inputClass} value={filters.source} onChange={(e) => updateFilter('source', e.target.value)}>
              <option value="">全部来源</option><option value="manual">人工操作</option><option value="ai">AI 助手</option>
              <option value="sentinel">Sentinel</option><option value="reconcile">状态协调</option><option value="scheduler">定时任务</option><option value="system">系统</option>
            </select></Filter>
            <Filter label="动作"><select className={inputClass} value={filters.action} onChange={(e) => updateFilter('action', e.target.value)}>
              <option value="">全部动作</option>{ACTIONS.map((action) => <option key={action} value={action}>{auditActionLabel(action)}</option>)}
            </select></Filter>
            <Filter label="结果"><select className={inputClass} value={filters.result} onChange={(e) => updateFilter('result', e.target.value)}>
              <option value="">全部结果</option><option value="accepted">已受理</option><option value="running">执行中</option>
              <option value="success">成功</option><option value="corrected">已修正</option><option value="failed">失败</option><option value="interrupted">已中断</option><option value="skipped">已跳过</option>
            </select></Filter>
          </div>
          <div className="mt-3.5 flex justify-end gap-2">
            <button className={secondaryButtonClass} onClick={resetFilters}>重置</button>
            <button className={primaryButtonClass} onClick={handleApply}><SlidersHorizontal size={15} /> 应用筛选</button>
          </div>
        </div>
      </section>

      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-[15px] font-semibold">审计记录</h3><p className="m-0 mt-0.5 text-xs text-gray-400">人工、AI 与系统执行的状态变更</p></div>
          <span className="text-xs text-gray-400">共 {total} 条</span>
        </div>
        {error && <div className="mx-4 mt-4 px-3 py-2 text-[13px] text-red-700 bg-red-50 border border-red-200 rounded-md">{error}</div>}
        <div className="overflow-x-auto">
          <table className="w-full min-w-[1040px] border-collapse">
            <thead><tr>{['时间', '来源 / 操作者', '动作', '目标', '触发原因', '级别', '结果', ''].map((label, index) => <th key={`${label}-${index}`} className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
            <tbody>
              {loading ? <tr><td colSpan="8" className="px-4 py-8 text-center text-gray-400 text-[13px]">加载中...</td></tr>
                : records.length === 0 ? <tr><td colSpan="8"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
                : records.map((log, index) => {
                  const id = log.id || log.ID || index;
                  const actorType = log.actor_type || log.ActorType;
                  const source = log.source || log.Source;
                  const target = { ...(log.target || log.Target || {}), group_name: log.group_name || log.GroupName, instance_name: log.instance_name || log.InstanceName, object_id: log.object_id || log.ObjectID };
                  return <tr key={id} onClick={() => setSelected(log)} className="border-b border-gray-200 last:border-b-0 hover:bg-gray-50 cursor-pointer transition-colors">
                    <td className="px-4 py-3 text-[13px] font-mono tabular-nums whitespace-nowrap">{formatDateTime(log.timestamp || log.Timestamp)}</td>
                    <td className="px-4 py-3"><span className="block text-[13px] text-gray-800">{auditSourceLabel(source, actorType)}</span><span className="block text-xs text-gray-400 mt-0.5">{auditOperator(log)}</span></td>
                    <td className="px-4 py-3 text-[13px] font-medium">{auditActionLabel(log.action || log.Action)}</td>
                    <td className="px-4 py-3 text-[13px] font-mono">{auditTargetDisplay(target)}</td>
                    <td className="px-4 py-3 text-[13px] text-gray-500">{auditTriggerLabel(log.trigger || log.Trigger)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditLevelLabel(log.level || log.Level)}</td>
                    <td className="px-4 py-3"><StatusBadge status={log.result || log.Result} metaFn={auditResultMeta} /></td>
                    <td className="px-4 py-3 text-gray-300"><ChevronRight size={16} /></td>
                  </tr>;
                })}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
          <span className="text-xs text-gray-400">第 {page}/{totalPages} 页</span><div className="flex items-center gap-1">
            <button className={pageButtonClass} disabled={page <= 1} onClick={() => goPage(page - 1)}>上一页</button>
            <button className={pageButtonClass} disabled={page >= totalPages} onClick={() => goPage(page + 1)}>下一页</button>
          </div>
        </div>}
      </section>
      {selected && <AuditDetail log={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}

function Filter({ label, children }) {
  return <label className="grid gap-1.5 text-xs font-medium text-gray-400"><span>{label}</span>{children}</label>;
}

function AuditDetail({ log, onClose }) {
  const target = log.target || log.Target || {};
  const params = log.params || log.Params || {};
  const before = log.before || log.Before || {};
  const after = log.after || log.After || {};
  const result = log.result || log.Result;
  const actorType = log.actor_type || log.ActorType;
  return <div className="fixed inset-0 z-50" role="dialog" aria-modal="true" aria-label="审计详情">
    <button className="absolute inset-0 w-full h-full border-0 bg-black/35 cursor-default" onClick={onClose} aria-label="关闭详情" />
    <aside className="absolute right-0 top-0 h-full w-full max-w-[560px] bg-white border-l border-gray-200 shadow-xl overflow-y-auto">
      <div className="sticky top-0 z-10 min-h-[64px] px-5 py-4 bg-white border-b border-gray-200 flex items-center justify-between">
        <div><h3 className="m-0 text-base font-semibold">{auditActionLabel(log.action || log.Action)}</h3><p className="m-0 mt-1 text-xs text-gray-400 font-mono">{log.id || log.ID}</p></div>
        <button className="w-8 h-8 inline-grid place-items-center border border-gray-200 rounded-md bg-white text-gray-500 cursor-pointer hover:bg-gray-50" onClick={onClose}><X size={16} /></button>
      </div>
      <div className="p-5 grid gap-6">
        <section className="grid grid-cols-2 gap-x-6 gap-y-4">
          <Detail label="时间" value={formatDateTime(log.timestamp || log.Timestamp)} mono />
          <Detail label="结果" value={auditResultMeta(result).label} />
          <Detail label="来源" value={auditSourceLabel(log.source || log.Source, actorType)} />
          <Detail label="操作者" value={auditOperator(log)} />
          <Detail label="触发原因" value={auditTriggerLabel(log.trigger || log.Trigger)} />
          <Detail label="执行阶段" value={auditPhaseLabel(log.phase || log.Phase)} />
          <Detail label="目标类型" value={displayValue(log.object_type || log.ObjectType)} />
          <Detail label="目标" value={displayValue(log.object_id || log.ObjectID)} mono />
          <Detail label="审计级别" value={auditLevelLabel(log.level || log.Level)} />
          <Detail label="耗时" value={`${Number(log.duration_ms || log.DurationMs || 0)} ms`} mono />
        </section>
        {(log.correlation_id || log.CorrelationID) && <CodeSection title="关联 ID" value={log.correlation_id || log.CorrelationID} />}
        <JSONSection title="目标上下文" value={target} />
        <JSONSection title="操作参数" value={params} />
        {(Object.keys(before).length > 0 || Object.keys(after).length > 0) && <div className="grid grid-cols-2 gap-3"><JSONSection title="变更前" value={before} /><JSONSection title="变更后" value={after} /></div>}
        {(log.detail || log.Detail) && <CodeSection title="执行详情" value={log.detail || log.Detail} />}
      </div>
    </aside>
  </div>;
}

function Detail({ label, value, mono = false }) {
  return <div><div className="text-xs text-gray-400 mb-1">{label}</div><div className={`text-[13px] text-gray-800 break-words ${mono ? 'font-mono' : ''}`}>{value || '-'}</div></div>;
}

function JSONSection({ title, value }) {
  if (!value || Object.keys(value).length === 0) return null;
  return <section><h4 className="m-0 mb-2 text-[13px] font-medium text-gray-700">{title}</h4><pre className="m-0 p-3 rounded-md bg-gray-50 border border-gray-200 text-xs text-gray-700 font-mono whitespace-pre-wrap break-words overflow-x-auto">{JSON.stringify(value, null, 2)}</pre></section>;
}

function CodeSection({ title, value }) {
  return <section><h4 className="m-0 mb-2 text-[13px] font-medium text-gray-700">{title}</h4><div className="p-3 rounded-md bg-gray-50 border border-gray-200 text-xs text-gray-700 font-mono whitespace-pre-wrap break-words">{value}</div></section>;
}

export function AuditCard({ log }) {
  const result = log.result || log.Result || 'success';
  const isBad = auditResultMeta(result).tone === 'bad';
  const Icon = isBad ? AlertCircle : CheckCircle2;
  return <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0 hover:bg-gray-50 transition-colors">
    <Icon size={18} className={isBad ? 'text-red-600 mt-px' : 'text-emerald-600 mt-px'} />
    <div className="min-w-0"><strong className="block text-[13px] font-medium truncate">{auditActionLabel(log.action || log.Action)}</strong><span className="block mt-0.5 text-xs text-gray-400">{auditTargetDisplay(log.target || log.Target)} · {auditOperator(log)}</span></div>
    <time className="text-xs text-gray-400 font-mono whitespace-nowrap">{formatDateTime(log.timestamp || log.Timestamp)}</time>
  </div>;
}

const inputClass = 'w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all';
const secondaryButtonClass = 'h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer';
const primaryButtonClass = 'h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer';
const pageButtonClass = 'h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors';
