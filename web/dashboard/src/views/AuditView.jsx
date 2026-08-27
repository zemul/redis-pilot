// 操作审计视图：筛选条件表单 + 分页表格；AuditCard 是紧凑单条卡片，也被大盘看板复用。
import { useState, useEffect } from 'react';
import { SlidersHorizontal, List, AlertCircle, CheckCircle2 } from 'lucide-react';
import { EmptyState, StatusBadge } from '../components/common.jsx';
import {
  auditQuery,
  formatDateTime,
  auditOperator,
  auditActionLabel,
  auditTargetDisplay,
  auditLevelLabel,
  auditResultMeta,
} from '../lib/format.js';

export function AuditView({ filters, setFilters, request }) {
  const [records, setRecords] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const pageSize = 30;

  async function fetchAudit(p = page) {
    setLoading(true);
    try {
      const data = await request(auditQuery(filters, p, pageSize));
      setRecords(data?.records || data?.Records || []);
      setTotal(data?.total || 0);
    } catch { /* ignore */ }
    setLoading(false);
  }

  useEffect(() => { fetchAudit(1); setPage(1); }, []);

  function updateFilter(key, value) {
    setFilters((current) => ({ ...current, [key]: value }));
  }
  function resetFilters() {
    setFilters({ from: '', to: '', group: '', instance: '', level: '', action: '' });
  }
  function handleApply() {
    setPage(1);
    fetchAudit(1);
  }
  function goPage(p) {
    setPage(p);
    fetchAudit(p);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="grid gap-4">
      <section className="border border-gray-200 rounded-lg bg-white p-4">
        <div className="grid grid-cols-6 gap-3 max-lg:grid-cols-2 max-md:grid-cols-1">
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>开始日期</span>
            <input type="date" className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.from} onChange={(event) => updateFilter('from', event.target.value)} />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>结束日期</span>
            <input type="date" className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.to} onChange={(event) => updateFilter('to', event.target.value)} />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>实例组</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.group} onChange={(event) => updateFilter('group', event.target.value)} placeholder="group" />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>实例</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.instance} onChange={(event) => updateFilter('instance', event.target.value)} placeholder="instance" />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>级别</span>
            <select className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.level} onChange={(event) => updateFilter('level', event.target.value)}>
              <option value="">全部</option>
              <option value="normal">常规</option>
              <option value="important">重要</option>
              <option value="critical">关键</option>
            </select>
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>动作</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.action} onChange={(event) => updateFilter('action', event.target.value)} placeholder="instance.start" />
          </label>
        </div>
        <div className="mt-3.5 flex justify-end gap-2 max-md:flex-col max-md:items-stretch">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" onClick={() => { resetFilters(); setTimeout(handleApply, 0); }}>重置</button>
          <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer" onClick={handleApply}>
            <SlidersHorizontal size={16} /> 应用筛选
          </button>
        </div>
      </section>
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
          <h3 className="m-0 text-[15px] font-semibold">审计记录</h3>
          <span className="text-xs text-gray-400">共 {total} 条</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] border-collapse">
            <thead>
              <tr>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">时间</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">操作者</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">动作</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">目标</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">级别</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">结果</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">详情</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan="7" className="px-4 py-8 text-center text-gray-400 text-[13px]">加载中...</td></tr>
              ) : records.length === 0 ? (
                <tr><td colSpan="7"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
              ) : records.map((log, index) => {
                const id = log.id || log.ID || index;
                const params = log.params || log.Params;
                const target = log.target || log.Target;
                const detail = log.detail || log.Detail;
                const paramsText = params && Object.keys(params).length > 0
                  ? Object.entries(params).map(([k, v]) => `${k}=${v}`).join(', ')
                  : detail || '-';
                return (
                  <tr key={id} className="border-b border-gray-200 last:border-b-0">
                    <td className="px-4 py-3 text-[13px] font-mono tabular-nums">{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditOperator(log)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditActionLabel(log.action || log.Action)}</td>
                    <td className="px-4 py-3 text-[13px] font-mono">{auditTargetDisplay(target || log.instance || log.Instance)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditLevelLabel(log.level || log.Level)}</td>
                    <td className="px-4 py-3 text-[13px]"><StatusBadge status={log.result || log.Result || 'success'} metaFn={auditResultMeta} /></td>
                    <td className="px-4 py-3 text-[13px] text-gray-500 font-mono max-w-[280px] truncate" title={paramsText}>{paramsText}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
            <span className="text-xs text-gray-400">第 {page}/{totalPages} 页</span>
            <div className="flex items-center gap-1">
              <button
                className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
                disabled={page <= 1}
                onClick={() => goPage(page - 1)}
              >上一页</button>
              <button
                className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
                disabled={page >= totalPages}
                onClick={() => goPage(page + 1)}
              >下一页</button>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}

export function AuditCard({ log }) {
  const result = log.result || log.Result || 'success';
  const isBad = auditResultMeta(result).tone === 'bad';
  const Icon = isBad ? AlertCircle : CheckCircle2;
  return (
    <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0 hover:bg-gray-50 transition-colors">
      <Icon size={18} className={isBad ? 'text-red-600 mt-px' : 'text-emerald-600 mt-px'} />
      <div className="min-w-0">
        <strong className="block text-[13px] font-medium truncate">{auditActionLabel(log.action || log.Action)}</strong>
        <span className="block mt-0.5 text-xs text-gray-400">{auditTargetDisplay(log.target || log.Target)} · {auditOperator(log)}</span>
      </div>
      <time className="text-xs text-gray-400 font-mono whitespace-nowrap">{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</time>
    </div>
  );
}
