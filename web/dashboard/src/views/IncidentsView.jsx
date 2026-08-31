// 异常记录视图：只读展示 reconcile/topology/task 三类来源的异常生命周期。
import { useEffect, useState } from 'react';
import { AlertCircle, AlertTriangle, ChevronRight, RefreshCw, ShieldAlert, X } from 'lucide-react';
import { EmptyState } from '../components/common.jsx';
import { formatDateTime, formatDuration } from '../lib/format.js';

// incident_type → 中文标签。
const TYPE_LABELS = {
  instance_unexpected_down: '实例意外停止',
  node_agent_unreachable: '节点 Agent 不可达',
  master_mismatch: 'Master 视角不一致',
  failover_conflict: '故障转移冲突',
  no_running_replica: '无可用从节点',
  backup_task_failed: '备份失败',
  restore_task_failed: '恢复失败',
};

const OBJECT_LABELS = { instance: '实例', group: '实例组', node: '节点' };
const SOURCE_LABELS = { reconcile: '状态校验', topology: '拓扑', task: '任务' };

function typeLabel(t) { return TYPE_LABELS[t] || t; }
function objectLabel(o) { return OBJECT_LABELS[o] || o; }
function durationLabel(ms) { return ms > 0 ? formatDuration(Math.round(ms / 1000)) : '-'; }

const PRESETS = [
  { id: 'open', label: '当前异常', status: 'open', severity: '' },
  { id: 'resolved', label: '已恢复', status: 'resolved', severity: '' },
  { id: 'critical', label: '严重', status: '', severity: 'critical' },
  { id: 'all', label: '全部', status: '', severity: '' },
];

function SeverityBadge({ severity }) {
  const critical = severity === 'critical';
  return (
    <span className={`inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-[11px] font-medium ${critical ? 'border-red-200 bg-red-50 text-red-700' : 'border-amber-200 bg-amber-50 text-amber-700'}`}>
      {critical ? <ShieldAlert size={12} /> : <AlertTriangle size={12} />}
      {critical ? '严重' : '警告'}
    </span>
  );
}

function StatusPill({ status }) {
  const open = status === 'open';
  return (
    <span className={`inline-flex items-center gap-1.5 text-[12px] font-medium ${open ? 'text-red-600' : 'text-gray-400'}`}>
      <span className={`w-2 h-2 rounded-full ${open ? 'bg-red-500' : 'bg-gray-300'}`} />
      {open ? '未恢复' : '已恢复'}
    </span>
  );
}

export function IncidentsView({ request, initialInstance = '' }) {
  const [records, setRecords] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [preset, setPreset] = useState('open');
  const [filters, setFilters] = useState({ status: 'open', severity: '', type: '', instance: initialInstance });
  const [applied, setApplied] = useState({ status: 'open', severity: '', type: '', instance: initialInstance });
  const [selected, setSelected] = useState(null);
  const pageSize = 30;

  async function fetchIncidents(nextPage = page, f = applied, quiet = false) {
    if (!quiet) setLoading(true);
    setError('');
    const params = new URLSearchParams({ page: String(nextPage), page_size: String(pageSize) });
    if (f.status) params.set('status', f.status);
    if (f.severity) params.set('severity', f.severity);
    if (f.type) params.set('type', f.type);
    if (f.instance) params.set('instance', f.instance);
    try {
      const data = await request(`/incidents?${params.toString()}`);
      setRecords(data?.records || []);
      setTotal(data?.total || 0);
    } catch (err) {
      setError(err.message || String(err));
      if (!quiet) { setRecords([]); setTotal(0); }
    } finally {
      if (!quiet) setLoading(false);
    }
  }

  useEffect(() => { fetchIncidents(1); }, []);
  useEffect(() => {
    const timer = setInterval(() => fetchIncidents(page, applied, true), 10000);
    return () => clearInterval(timer);
  }, [page, applied]);

  function applyPreset(item) {
    const next = { ...filters, status: item.status, severity: item.severity };
    setPreset(item.id);
    setFilters(next);
    setApplied(next);
    setPage(1);
    fetchIncidents(1, next);
  }

  function applyFilters() {
    setApplied(filters);
    setPage(1);
    fetchIncidents(1, filters);
  }

  function goPage(next) { setPage(next); fetchIncidents(next, applied); }

  function clearInstance() {
    const next = { ...applied, instance: '' };
    setFilters((c) => ({ ...c, instance: '' }));
    setApplied(next);
    setPage(1);
    fetchIncidents(1, next);
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
        <div className="p-4 flex items-end gap-3 max-md:flex-col max-md:items-stretch">
          <label className="w-52 text-xs font-medium text-gray-400 max-md:w-full">异常类型
            <select className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 text-[13px]"
              value={filters.type} onChange={(e) => setFilters((c) => ({ ...c, type: e.target.value }))}>
              <option value="">全部类型</option>
              {Object.entries(TYPE_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
          <label className="w-40 text-xs font-medium text-gray-400 max-md:w-full">严重级别
            <select className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 text-[13px]"
              value={filters.severity} onChange={(e) => setFilters((c) => ({ ...c, severity: e.target.value }))}>
              <option value="">全部级别</option>
              <option value="critical">严重</option>
              <option value="warning">警告</option>
            </select>
          </label>
          <button type="button" onClick={applyFilters} className="h-9 px-3 rounded-md bg-gray-900 text-white text-[13px] font-medium border-0 cursor-pointer hover:bg-gray-700">应用筛选</button>
        </div>
        {applied.instance && (
          <div className="px-4 pb-4 -mt-1">
            <span className="inline-flex items-center gap-1.5 rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-[12px] text-gray-600">
              实例：<strong className="font-medium text-gray-900">{applied.instance}</strong>
              <button type="button" onClick={clearInstance} className="text-gray-400 hover:text-gray-700 cursor-pointer" aria-label="清除实例过滤"><X size={13} /></button>
            </span>
          </div>
        )}
      </section>

      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-[15px] font-semibold">异常记录</h3><p className="m-0 mt-0.5 text-xs text-gray-400">平台自动记录的异常发生与恢复历史</p></div>
          <button type="button" onClick={() => fetchIncidents(page, applied)} className="h-8 px-2.5 rounded-md border border-gray-200 bg-white text-gray-500 text-xs inline-flex items-center gap-1.5 hover:bg-gray-50 cursor-pointer"><RefreshCw size={14} /> 刷新</button>
        </div>
        {error && <div className="mx-4 mt-4 px-3 py-2 text-[13px] text-red-700 bg-red-50 border border-red-200 rounded-md"><AlertCircle size={15} className="inline mr-1.5" />{error}</div>}
        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] border-collapse">
            <thead><tr>{['状态', '级别', '异常', '对象', '检测原因', '发生时间', '持续时间', '恢复时间', ''].map((label, i) => <th key={`${label}-${i}`} className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
            <tbody>
              {loading ? <tr><td colSpan="9" className="px-4 py-8 text-center text-gray-400 text-[13px]">加载中...</td></tr>
                : records.length === 0 ? <tr><td colSpan="9"><EmptyState icon={ShieldAlert} title="暂无异常记录" detail="平台发现的异常会记录在这里。" /></td></tr>
                : records.map((inc) => <tr key={inc.id} onClick={() => setSelected(inc)} className="border-b border-gray-200 last:border-b-0 hover:bg-gray-50 cursor-pointer transition-colors">
                  <td className="px-4 py-3"><StatusPill status={inc.status} /></td>
                  <td className="px-4 py-3"><SeverityBadge severity={inc.severity} /></td>
                  <td className="px-4 py-3"><strong className="block text-[13px] font-medium">{typeLabel(inc.incident_type)}</strong><span className="block mt-0.5 text-[11px] text-gray-400">{SOURCE_LABELS[inc.source] || inc.source}</span></td>
                  <td className="px-4 py-3 text-[13px]"><span className="text-gray-400">{objectLabel(inc.object_type)}</span> <span className="font-mono">{inc.object_id}</span></td>
                  <td className="px-4 py-3 text-[13px] text-gray-600 max-w-64 truncate" title={inc.reason}>{inc.reason || '-'}</td>
                  <td className="px-4 py-3 text-[13px] font-mono whitespace-nowrap">{formatDateTime(inc.first_observed_at)}</td>
                  <td className="px-4 py-3 text-[13px] text-gray-500 tabular-nums whitespace-nowrap">{durationLabel(inc.duration_ms)}</td>
                  <td className="px-4 py-3 text-[13px] font-mono whitespace-nowrap text-gray-500">{inc.resolved_at ? formatDateTime(inc.resolved_at) : '-'}</td>
                  <td className="px-4 py-3 text-gray-300"><ChevronRight size={16} /></td>
                </tr>)}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
          <span className="text-xs text-gray-400">共 {total} 条，第 {page}/{totalPages} 页</span>
          <div className="flex gap-1"><button className={pageButtonClass} disabled={page <= 1} onClick={() => goPage(page - 1)}>上一页</button><button className={pageButtonClass} disabled={page >= totalPages} onClick={() => goPage(page + 1)}>下一页</button></div>
        </div>}
      </section>

      {selected && <IncidentDetail incident={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}

function IncidentDetail({ incident, onClose }) {
  const evidence = parseEvidence(incident.evidence);
  return <div className="fixed inset-0 z-40 bg-black/30" onMouseDown={onClose}>
    <aside className="absolute right-0 top-0 h-full w-[min(520px,100%)] bg-white shadow-xl flex flex-col" onMouseDown={(e) => e.stopPropagation()}>
      <div className="min-h-[64px] px-5 border-b border-gray-200 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <SeverityBadge severity={incident.severity} />
          <h3 className="m-0 text-base font-semibold">{typeLabel(incident.incident_type)}</h3>
        </div>
        <button type="button" onClick={onClose} className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-grid place-items-center hover:bg-gray-50"><X size={18} /></button>
      </div>
      <div className="p-5 overflow-auto grid gap-5">
        <section className="border border-gray-200 rounded-lg p-4 flex items-center justify-between gap-3">
          <div><span className="block text-xs text-gray-400 mb-1">当前状态</span><StatusPill status={incident.status} /></div>
          <div className="text-right"><span className="block text-xs text-gray-400 mb-1">持续时间</span><span className="text-sm font-mono text-gray-700">{durationLabel(incident.duration_ms)}</span></div>
        </section>
        <section className="grid grid-cols-2 gap-x-5 gap-y-4 max-sm:grid-cols-1">
          <Detail label="对象" value={`${objectLabel(incident.object_type)} ${incident.object_id}`} />
          <Detail label="来源" value={SOURCE_LABELS[incident.source] || incident.source} />
          <Detail label="实例组" value={incident.group_name || '-'} />
          <Detail label="实例" value={incident.instance_name || '-'} />
          <Detail label="节点" value={incident.node_name || '-'} />
          <Detail label="发生时间" value={formatDateTime(incident.first_observed_at)} />
          <Detail label="最近观测" value={formatDateTime(incident.last_seen_at)} />
          <Detail label="恢复时间" value={incident.resolved_at ? formatDateTime(incident.resolved_at) : '未恢复'} />
        </section>
        {incident.reason && <section className="border border-gray-200 rounded-lg p-4"><span className="block text-xs font-medium text-gray-400 mb-1.5">检测原因</span><p className="m-0 text-[13px] leading-relaxed text-gray-800">{incident.reason}</p></section>}
        {evidence && Object.keys(evidence).length > 0 && (
          <section className="border border-gray-200 rounded-lg overflow-hidden">
            <div className="px-4 py-2.5 border-b border-gray-200 text-xs font-medium text-gray-400">检测证据</div>
            <div className="p-4 grid gap-2">
              {Object.entries(evidence).map(([k, v]) => (
                <div key={k} className="grid grid-cols-[140px_1fr] gap-2 text-[13px] max-sm:grid-cols-1">
                  <span className="text-gray-400">{k}</span>
                  <span className="text-gray-800 font-mono break-all">{Array.isArray(v) ? v.join(', ') : String(v)}</span>
                </div>
              ))}
            </div>
          </section>
        )}
      </div>
    </aside>
  </div>;
}

function parseEvidence(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch { return null; }
}

function Detail({ label, value }) {
  return <div><span className="block text-xs text-gray-400 mb-1">{label}</span><span className="block text-[13px] text-gray-800 break-all">{value}</span></div>;
}

const pageButtonClass = 'h-8 px-2.5 rounded-md border border-gray-200 bg-white text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed';
