// 操作审计视图：按来源/结果快速分类，主表展示变更摘要，详情抽屉保留完整上下文。
import { useState, useEffect } from 'react';
import { SlidersHorizontal, List, AlertCircle, CheckCircle2, ChevronRight, X, CalendarDays } from 'lucide-react';
import { EmptyState, StatusBadge, Filter, FilterSelect } from '../components/common.jsx';
import {
  auditQuery, displayValue, formatDateTime, auditOperator, auditActionLabel,
  auditTargetDisplay, auditLevelLabel, auditResultMeta, auditSourceLabel,
  auditTriggerLabel, auditPhaseLabel, collapseAuditRecords,
} from '../lib/format.js';
import { Input } from '../components/ui/input.jsx';
import { Button } from '../components/ui/button.jsx';
import { Popover, PopoverTrigger, PopoverContent } from '../components/ui/popover.jsx';
import { Calendar } from '../components/ui/calendar.jsx';

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
  'diagnostic.bigkeys',
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

  const visibleRecords = collapseAuditRecords(records);

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
            <Filter label="开始日期"><DatePickerInput value={filters.from} onChange={(v) => updateFilter('from', v)} placeholder="选择开始日期" /></Filter>
            <Filter label="结束日期"><DatePickerInput value={filters.to} onChange={(v) => updateFilter('to', v)} placeholder="选择结束日期" /></Filter>
            <Filter label="目标或操作者"><Input value={filters.q} onChange={(e) => updateFilter('q', e.target.value)} placeholder="实例、组、节点或用户" /></Filter>
            <Filter label="来源">
              <FilterSelect value={filters.source} onChange={(v) => updateFilter('source', v)} placeholder="全部来源"
                options={[
                  { value: '', label: '全部来源' },
                  { value: 'manual', label: '人工操作' },
                  { value: 'ai', label: 'AI 助手' },
                  { value: 'sentinel', label: 'Sentinel' },
                  { value: 'reconcile', label: '状态协调' },
                  { value: 'scheduler', label: '定时任务' },
                  { value: 'system', label: '系统' },
                ]} />
            </Filter>
            <Filter label="动作">
              <FilterSelect value={filters.action} onChange={(v) => updateFilter('action', v)} placeholder="全部动作"
                options={[{ value: '', label: '全部动作' }, ...ACTIONS.map((action) => ({ value: action, label: auditActionLabel(action) }))]} />
            </Filter>
            <Filter label="结果">
              <FilterSelect value={filters.result} onChange={(v) => updateFilter('result', v)} placeholder="全部结果"
                options={[
                  { value: '', label: '全部结果' },
                  { value: 'accepted', label: '已受理' },
                  { value: 'running', label: '执行中' },
                  { value: 'success', label: '成功' },
                  { value: 'corrected', label: '已修正' },
                  { value: 'failed', label: '失败' },
                  { value: 'interrupted', label: '已中断' },
                  { value: 'canceled', label: '已取消' },
                  { value: 'skipped', label: '已跳过' },
                ]} />
            </Filter>
          </div>
          <div className="mt-3.5 flex justify-end gap-2">
            <Button variant="outline" size="sm" onClick={resetFilters}>重置</Button>
            <Button size="sm" onClick={handleApply}><SlidersHorizontal size={15} /> 应用筛选</Button>
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
                : visibleRecords.length === 0 ? <tr><td colSpan="8"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
                : visibleRecords.map((log, index) => {
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

// 日期选择器：Popover + Calendar 替代原生 input[type=date]。
// value 为 ISO date string（"2026-08-28"），空为未选择。
function DatePickerInput({ value, onChange, placeholder = '选择日期' }) {
  const [open, setOpen] = useState(false);
  const selected = value ? new Date(value + 'T00:00:00') : undefined;
  const label = value || placeholder;
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className={`flex h-9 w-full items-center justify-between rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm ring-offset-background focus:outline-none focus:ring-1 focus:ring-ring ${value ? 'text-gray-900' : 'text-muted-foreground'}`}
        >
          <span className="truncate">{label}</span>
          <CalendarDays size={15} className="ml-1.5 opacity-50 shrink-0" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-3" align="start">
        <Calendar
          mode="single"
          selected={selected}
          onSelect={(day) => {
            if (day) {
              const iso = [day.getFullYear(), String(day.getMonth() + 1).padStart(2, '0'), String(day.getDate()).padStart(2, '0')].join('-');
              onChange(iso);
            } else {
              onChange('');
            }
            setOpen(false);
          }}
        />
      </PopoverContent>
    </Popover>
  );
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

const pageButtonClass = 'h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors';
