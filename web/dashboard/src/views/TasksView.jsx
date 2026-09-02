// 运维任务视图：任务只回答执行状态与结果，操作责任归属仍由审计页承载。
import { useEffect, useState } from 'react';
import { AlertCircle, ChevronRight, ClipboardList, RefreshCw, Search, X } from 'lucide-react';
import { EmptyState, StatusBadge, Filter, FilterSelect } from '../components/common.jsx';
import {
  auditSourceLabel, auditTriggerLabel, displayValue, formatDateTime,
  taskDuration, taskKindLabel, taskStatusMeta, taskTargetDisplay,
} from '../lib/format.js';

const PRESETS = [
  { id: 'all', label: '全部任务', status: '' },
  { id: 'active', label: '进行中', status: 'active' },
  { id: 'failed', label: '失败与中断', status: 'failed' },
  { id: 'success', label: '已成功', status: 'success' },
];

export function TasksView({ request, initialInstance = '' }) {
  const [records, setRecords] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [preset, setPreset] = useState('all');
  const [filters, setFilters] = useState({ q: '', kind: '', status: '', instance: initialInstance });
  const [appliedFilters, setAppliedFilters] = useState({ q: '', kind: '', status: '', instance: initialInstance });
  const [selected, setSelected] = useState(null);
  const [canceling, setCanceling] = useState(false);
  const [actionError, setActionError] = useState('');
  const pageSize = 30;

  async function fetchTasks(nextPage = page, nextFilters = appliedFilters, quiet = false) {
    if (!quiet) setLoading(true);
    setError('');
    const params = new URLSearchParams({ page: String(nextPage), page_size: String(pageSize) });
    if (nextFilters.q.trim()) params.set('q', nextFilters.q.trim());
    if (nextFilters.kind) params.set('kind', nextFilters.kind);
    if (nextFilters.status) params.set('status', nextFilters.status);
    if (nextFilters.instance) params.set('instance', nextFilters.instance);
    try {
      const data = await request(`/tasks?${params.toString()}`);
      setRecords(data?.records || []);
      setTotal(data?.total || 0);
    } catch (err) {
      setError(err.message || String(err));
      if (!quiet) {
        setRecords([]);
        setTotal(0);
      }
    } finally {
      if (!quiet) setLoading(false);
    }
  }

  useEffect(() => { fetchTasks(1); }, []);
  useEffect(() => {
    const timer = setInterval(() => fetchTasks(page, appliedFilters, true), 5000);
    return () => clearInterval(timer);
  }, [page, appliedFilters]);

  function applyPreset(item) {
    const next = { ...filters, status: item.status };
    setPreset(item.id);
    setFilters(next);
    setAppliedFilters(next);
    setPage(1);
    fetchTasks(1, next);
  }

  function applyFilters() {
    setPreset(filters.status ? 'custom' : 'all');
    setAppliedFilters(filters);
    setPage(1);
    fetchTasks(1, filters);
  }

  function goPage(nextPage) {
    setPage(nextPage);
    fetchTasks(nextPage, appliedFilters);
  }

  function clearInstance() {
    const next = { ...appliedFilters, instance: '' };
    setFilters((current) => ({ ...current, instance: '' }));
    setAppliedFilters(next);
    setPage(1);
    fetchTasks(1, next);
  }

  async function openDetail(task) {
    setSelected(task);
    try {
      const latest = await request(`/tasks/${encodeURIComponent(task.id)}`);
      setSelected(latest);
    } catch {
      // 列表数据足以展示，详情刷新失败时保留当前快照。
    }
  }

  useEffect(() => {
    if (!selected || !['pending', 'accepted', 'running'].includes(selected.status)) return undefined;
    const taskId = selected.id;
    const timer = setInterval(async () => {
      try {
        const latest = await request(`/tasks/${encodeURIComponent(taskId)}`);
        setSelected((current) => current?.id === taskId ? latest : current);
      } catch {
        // 列表轮询仍会继续，详情短暂刷新失败不关闭抽屉。
      }
    }, 2000);
    return () => clearInterval(timer);
  }, [selected?.id, selected?.status]);

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
          <label className="flex-1 text-xs font-medium text-gray-400">任务、实例、组、节点或操作者
            <div className="relative mt-1.5">
              <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input className="w-full h-9 border border-gray-200 rounded-md pl-9 pr-3 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]"
                value={filters.q} onChange={(event) => setFilters((current) => ({ ...current, q: event.target.value }))}
                onKeyDown={(event) => { if (event.key === 'Enter') applyFilters(); }} placeholder="输入关键词" />
            </div>
          </label>
          <div className="w-48 max-md:w-full">
            <Filter label="任务类型">
              <FilterSelect value={filters.kind} onChange={(v) => setFilters((current) => ({ ...current, kind: v }))} placeholder="全部类型"
                options={[
                  { value: '', label: '全部类型' },
                  { value: 'backup.create', label: '创建备份' },
                  { value: 'backup.restore', label: '恢复备份' },
                  { value: 'diagnostic.bigkeys', label: '大 Key 扫描' },
                ]} />
            </Filter>
          </div>
          <button type="button" onClick={applyFilters} className="h-9 px-3 rounded-md bg-gray-900 text-white text-[13px] font-medium border-0 cursor-pointer hover:bg-gray-700">应用筛选</button>
        </div>
        {appliedFilters.instance && (
          <div className="px-4 pb-4 -mt-1">
            <span className="inline-flex items-center gap-1.5 rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-[12px] text-gray-600">
              实例：<strong className="font-medium text-gray-900">{appliedFilters.instance}</strong>
              <button type="button" onClick={clearInstance} className="text-gray-400 hover:text-gray-700 cursor-pointer" aria-label="清除实例过滤"><X size={13} /></button>
            </span>
          </div>
        )}
      </section>

      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
          <h3 className="m-0 text-[15px] font-semibold">任务记录</h3>
          <button type="button" onClick={() => fetchTasks(page, appliedFilters)} className="h-8 px-2.5 rounded-md border border-gray-200 bg-white text-gray-500 text-xs inline-flex items-center gap-1.5 hover:bg-gray-50 cursor-pointer"><RefreshCw size={14} /> 刷新</button>
        </div>
        {error && <div className="mx-4 mt-4 px-3 py-2 text-[13px] text-red-700 bg-red-50 border border-red-200 rounded-md"><AlertCircle size={15} className="inline mr-1.5" />{error}</div>}
        <div className="overflow-x-auto">
          <table className="w-full min-w-[960px] border-collapse">
            <thead><tr>{['创建时间', '任务', '目标', '节点', '操作者', '状态', '耗时', ''].map((label, index) => <th key={`${label}-${index}`} className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">{label}</th>)}</tr></thead>
            <tbody>
              {loading ? <tr><td colSpan="8" className="px-4 py-8 text-center text-gray-400 text-[13px]">加载中...</td></tr>
                : records.length === 0 ? <tr><td colSpan="8"><EmptyState icon={ClipboardList} title="暂无任务" detail="备份和恢复任务会显示在这里。" /></td></tr>
                : records.map((task) => <tr key={task.id} onClick={() => openDetail(task)} className="border-b border-gray-200 last:border-b-0 hover:bg-gray-50 cursor-pointer transition-colors">
                  <td className="px-4 py-3 text-[13px] font-mono whitespace-nowrap">{formatDateTime(task.created_at)}</td>
                  <td className="px-4 py-3"><strong className="block text-[13px] font-medium">{taskKindLabel(task.kind)}</strong><span className="block mt-0.5 max-w-52 truncate text-[11px] font-mono text-gray-400">{task.id}</span></td>
                  <td className="px-4 py-3 text-[13px] font-mono">{taskTargetDisplay(task)}</td>
                  <td className="px-4 py-3 text-[13px] font-mono text-gray-500">{task.server_name || '-'}</td>
                  <td className="px-4 py-3 text-[13px]">{task.operator || '系统'}</td>
                  <td className="px-4 py-3"><StatusBadge status={task.status} metaFn={taskStatusMeta} />{task.message && <span className="block mt-0.5 text-[11px] text-gray-400">{task.message}{task.progress > 0 ? ` · ${task.progress} 个 Key` : ''}</span>}</td>
                  <td className="px-4 py-3 text-[13px] text-gray-500 tabular-nums">{taskDuration(task)}</td>
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
      {selected && <TaskDetail task={selected} canceling={canceling} actionError={actionError} onCancelTask={async () => {
        setCanceling(true); setActionError('');
        try {
          await request(`/tasks/${encodeURIComponent(selected.id)}/cancel`, { method: 'POST' });
          const latest = await request(`/tasks/${encodeURIComponent(selected.id)}`);
          setSelected(latest);
        } catch (err) { setActionError(err.message || String(err)); }
        finally { setCanceling(false); }
      }} onClose={() => { setSelected(null); setActionError(''); }} />}
    </div>
  );
}

function TaskDetail({ task, canceling, actionError, onCancelTask, onClose }) {
  const result = parseTaskResult(task.result);
  const bigKeys = task.kind === 'diagnostic.bigkeys' && Array.isArray(result?.keys) ? result.keys : [];
  return <div className="fixed inset-0 z-40 bg-black/30" onMouseDown={onClose}>
    <aside className="absolute right-0 top-0 h-full w-[min(520px,100%)] bg-white shadow-xl flex flex-col" onMouseDown={(event) => event.stopPropagation()}>
      <div className="min-h-[64px] px-5 border-b border-gray-200 flex items-center justify-between gap-3">
        <div><h3 className="m-0 text-base font-semibold">{taskKindLabel(task.kind)}</h3><p className="m-0 mt-0.5 text-[11px] font-mono text-gray-400 break-all">{task.id}</p></div>
        <button type="button" onClick={onClose} className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-grid place-items-center hover:bg-gray-50"><X size={18} /></button>
      </div>
      <div className="p-5 overflow-auto grid gap-5">
        <section className="border border-gray-200 rounded-lg p-4 flex items-center justify-between gap-3"><div><span className="block text-xs text-gray-400 mb-1">当前状态</span><StatusBadge status={task.status} metaFn={taskStatusMeta} />{task.message && <span className="block mt-1.5 text-xs text-gray-500">{task.message}</span>}</div><div className="text-right"><span className="block text-sm font-mono text-gray-500">{taskDuration(task)}</span>{task.progress > 0 && <span className="block mt-1 text-xs text-gray-400">已扫描 {task.progress} 个 Key</span>}</div></section>
        {task.cancelable && task.status === 'running' && <section className="flex items-center justify-between gap-3"><span className="text-xs text-gray-400">取消后保留已产生的部分结果。</span><button type="button" disabled={canceling || task.cancel_requested} onClick={onCancelTask} className="h-8 px-3 rounded-md border border-red-200 bg-white text-red-600 text-xs font-medium cursor-pointer hover:bg-red-50 disabled:opacity-50 disabled:cursor-not-allowed">{canceling || task.cancel_requested ? '正在取消…' : '取消任务'}</button></section>}
        {actionError && <div className="border border-red-200 bg-red-50 text-red-700 rounded-md px-3 py-2 text-[13px]">{actionError}</div>}
        <section className="grid grid-cols-2 gap-x-5 gap-y-4 max-sm:grid-cols-1">
          <Detail label="实例组" value={task.group_name} /><Detail label="实例" value={task.instance_name} />
          <Detail label="节点" value={task.server_name} /><Detail label="操作者" value={task.operator || '系统'} />
          <Detail label="来源" value={auditSourceLabel(task.source)} /><Detail label="触发原因" value={auditTriggerLabel(task.trigger)} />
          <Detail label="创建时间" value={formatDateTime(task.created_at)} /><Detail label="开始时间" value={formatDateTime(task.started_at)} />
          <Detail label="完成时间" value={formatDateTime(task.finished_at)} /><Detail label="最近更新" value={formatDateTime(task.updated_at)} />
        </section>
        {task.error && <section className="border border-red-200 rounded-lg bg-red-50 p-4"><span className="block text-xs font-medium text-red-600 mb-1.5">失败原因</span><p className="m-0 text-[13px] leading-relaxed text-red-800 whitespace-pre-wrap break-words">{task.error}</p></section>}
        {task.kind === 'diagnostic.bigkeys' && result && <section className="border border-gray-200 rounded-lg overflow-hidden"><div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between"><div><span className="block text-[13px] font-medium text-gray-800">扫描结果</span><span className="block mt-0.5 text-xs text-gray-400">扫描 {result.scanned || 0} 个 Key，命中 {bigKeys.length} 个{result.incomplete ? '，结果不完整' : ''}</span></div></div>{bigKeys.length === 0 ? <div className="p-6 text-center text-sm text-gray-400">未发现超过阈值的大 Key</div> : <div className="max-h-72 overflow-auto"><table className="w-full border-collapse text-[12px]"><thead><tr>{['Key', '类型', '大小'].map((label) => <th key={label} className="sticky top-0 px-3 py-2 text-left font-medium text-gray-400 bg-gray-50 border-b border-gray-200">{label}</th>)}</tr></thead><tbody>{bigKeys.map((item, index) => <tr key={`${item.key}-${index}`}><td className="px-3 py-2 border-b border-gray-100 font-mono max-w-64 truncate" title={item.key}>{item.key}</td><td className="px-3 py-2 border-b border-gray-100">{item.type}</td><td className="px-3 py-2 border-b border-gray-100 font-mono whitespace-nowrap">{item.size_desc}</td></tr>)}</tbody></table></div>}</section>}
        {task.result && task.kind !== 'diagnostic.bigkeys' && <section className="border border-gray-200 rounded-lg p-4"><span className="block text-xs font-medium text-gray-400 mb-1.5">执行结果</span><p className="m-0 text-[13px] font-mono text-gray-800 break-all">{displayValue(task.result)}</p></section>}
        <p className="m-0 text-xs text-gray-400">关联审计可通过任务 ID（correlation_id）查询；任务记录执行状态，审计记录操作责任与变更结果。</p>
      </div>
    </aside>
  </div>;
}

function parseTaskResult(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch { return null; }
}

function Detail({ label, value }) {
  return <div><span className="block text-xs text-gray-400 mb-1">{label}</span><span className="block text-[13px] text-gray-800 break-all">{displayValue(value)}</span></div>;
}

const pageButtonClass = 'h-8 px-2.5 rounded-md border border-gray-200 bg-white text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed';
