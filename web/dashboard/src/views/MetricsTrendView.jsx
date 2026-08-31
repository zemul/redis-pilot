// 实例指标趋势视图：按时间范围拉取历史采样，用 echarts 渲染多张折线图（懒加载 echarts）。
import { useState, useEffect, useRef } from 'react';
import { AlertCircle, CalendarDays } from 'lucide-react';
import { Popover, PopoverTrigger, PopoverContent } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';

// 起止时间选择：Popover 内 Calendar 选日期 + time input 选时分，组合成一个 Date。
function DateTimeField({ label, value, onChange, min, max }) {
  const [open, setOpen] = useState(false);
  const timeStr = value
    ? `${String(value.getHours()).padStart(2, '0')}:${String(value.getMinutes()).padStart(2, '0')}`
    : '00:00';

  function pickDate(d) {
    if (!d) return;
    const next = new Date(d);
    if (value) { next.setHours(value.getHours(), value.getMinutes(), 0, 0); }
    else { next.setHours(0, 0, 0, 0); }
    onChange(next);
  }
  function pickTime(e) {
    const [h, m] = e.target.value.split(':').map(Number);
    const next = value ? new Date(value) : new Date();
    next.setHours(h || 0, m || 0, 0, 0);
    onChange(next);
  }
  const display = value
    ? value.toLocaleString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).replace(/\//g, '-')
    : label;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button className={`h-7 px-2.5 rounded-md border text-xs font-medium cursor-pointer inline-flex items-center gap-1.5 transition-colors ${value ? 'border-gray-300 text-gray-900 bg-white' : 'border-gray-200 text-gray-400 bg-white'} hover:bg-gray-50`}>
          <CalendarDays size={14} className="text-gray-400" />
          {display}
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="flex flex-col gap-2" onOpenAutoFocus={(e) => e.preventDefault()}>
        <Calendar
          mode="single"
          selected={value || undefined}
          onSelect={pickDate}
          disabled={[min ? { before: min } : undefined, max ? { after: max } : undefined].filter(Boolean)}
          defaultMonth={value || undefined}
        />
        <div className="flex items-center gap-2 border-t border-gray-100 pt-2">
          <span className="text-xs text-gray-400">时间</span>
          <input type="time" value={timeStr} onChange={pickTime} className="h-7 px-2 rounded-md border border-gray-200 text-xs text-gray-900 outline-0 focus:border-gray-900" />
          <button type="button" onClick={() => setOpen(false)} className="ml-auto h-7 px-3 rounded-md bg-gray-900 text-white text-xs font-medium border-0 cursor-pointer hover:bg-gray-700 transition-colors">完成</button>
        </div>
      </PopoverContent>
    </Popover>
  );
}

export function MetricsTrendView({ instanceName, requestFn }) {
  const [range_, setRange] = useState('1h');
  const [customStart, setCustomStart] = useState(null); // Date
  const [customEnd, setCustomEnd] = useState(null);     // Date
  // applied 为点击“查询”后实际生效的自定义窗口，避免边输入边请求
  const [applied, setApplied] = useState(null); // { start, end } Unix 秒
  const [points, setPoints] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const chartRefs = useRef({});
  const chartInstances = useRef([]);

  const isCustom = range_ === 'custom';

  useEffect(() => {
    // 自定义模式下，只有 applied 有效时才发请求；否则等用户点“查询”
    if (isCustom && !applied) { setLoading(false); return; }
    let ignore = false;
    setLoading(true);
    setError('');
    const base = `/instance/metrics/history?name=${encodeURIComponent(instanceName)}`;
    const url = isCustom
      ? `${base}&start=${applied.start}&end=${applied.end}`
      : `${base}&range=${range_}`;
    requestFn(url)
      .then((data) => { if (!ignore) setPoints(data?.points || []); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName, range_, applied]);

  function applyCustom() {
    if (!customStart || !customEnd) { setError('请选择起止时间'); return; }
    const s = Math.floor(customStart.getTime() / 1000);
    const e = Math.floor(customEnd.getTime() / 1000);
    if (!(e > s)) { setError('结束时间必须晚于开始时间'); return; }
    setError('');
    setApplied({ start: s, end: e });
  }

  useEffect(() => {
    if (!points || points.length === 0) return;
    import('echarts').then((echarts) => {
      chartInstances.current.forEach((c) => c.dispose());
      chartInstances.current = [];

      const ts = points.map((p) => new Date(p.ts * 1000));
      // 长范围(7天/30天)或自定义跨度>24h 带上月-日,否则只看时分无法定位;短范围只显示时分。
      const spanSec = points.length > 1 ? points[points.length - 1].ts - points[0].ts : 0;
      const isLongRange = range_ === '7d' || range_ === '30d' || (isCustom && spanSec > 24 * 3600);
      const fmtAxis = (d) => isLongRange
        ? d.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).replace(/\//g, '-')
        : d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
      const axisLabels = ts.map(fmtAxis);
      const charts = [
        { id: 'mem', title: '内存使用率 (%)', data: points.map((p) => p.mem_max > 0 ? +(p.mem_used / p.mem_max * 100).toFixed(1) : 0), color: '#3b82f6', max: 100 },
        { id: 'qps', title: 'QPS (ops/s)', data: points.map((p) => p.ops_sec), color: '#3b82f6' },
        { id: 'hit', title: '命中率 (%)', data: points.map((p) => { const total = p.hits + p.misses; return total > 0 ? +((p.hits / total) * 100).toFixed(1) : 100; }), color: '#3b82f6', max: 100 },
        { id: 'conns', title: '连接数', data: points.map((p) => p.conns), color: '#3b82f6' },
        { id: 'frag', title: '内存碎片率', data: points.map((p) => p.frag_ratio ? +p.frag_ratio.toFixed(2) : 0), color: '#3b82f6' },
        { id: 'repl', title: '主从同步状态', data: points.map((p) => p.repl_link !== undefined ? p.repl_link : 1), color: '#3b82f6', max: 1 },
        { id: 'offset', title: '复制偏移量', data: points.map((p) => p.repl_offset || 0), color: '#3b82f6' },
        { id: 'keys', title: 'Key 数量', data: points.map((p) => p.total_keys || 0), color: '#3b82f6' }
      ];

      charts.forEach(({ id, title, data, color, max }) => {
        const el = chartRefs.current[id];
        if (!el) return;
        const chart = echarts.init(el);
        chart.setOption({
          title: { text: title, textStyle: { fontSize: 13, fontWeight: 500 }, left: 'center' },
          tooltip: { trigger: 'axis', formatter: (params) => { const p = params[0]; return `${p.axisValueLabel}<br/>${p.marker} ${p.value}`; } },
          grid: { left: 45, right: 12, top: 35, bottom: 28 },
          xAxis: { type: 'category', data: axisLabels, boundaryGap: false, axisLine: { lineStyle: { color: '#ddd' } }, axisLabel: { fontSize: 10, color: '#999' } },
          yAxis: { type: 'value', min: 0, max: max || undefined, splitLine: { lineStyle: { color: '#f0f0f0' } }, axisLabel: { fontSize: 10, color: '#999' } },
          series: [{ type: 'line', data, smooth: true, symbol: 'none', lineStyle: { width: 2, color }, areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: color + '30' }, { offset: 1, color: color + '05' }] } } }]
        });
        chartInstances.current.push(chart);
      });

      const handleResize = () => chartInstances.current.forEach((c) => c.resize());
      window.addEventListener('resize', handleResize);
      return () => window.removeEventListener('resize', handleResize);
    });

    return () => {
      chartInstances.current.forEach((c) => c.dispose());
      chartInstances.current = [];
    };
  }, [points]);

  const rangeOptions = [
    { value: '1h', label: '1小时' },
    { value: '6h', label: '6小时' },
    { value: '24h', label: '24小时' },
    { value: '7d', label: '7天' },
    { value: '30d', label: '30天' },
    { value: 'custom', label: '自定义' }
  ];

  return (
    <section>
      <div className="flex items-center gap-1.5 mb-3">
        {rangeOptions.map((opt) => (
          <button key={opt.value} className={`h-7 px-2.5 rounded-md border text-xs font-medium cursor-pointer transition-colors ${range_ === opt.value ? 'bg-gray-900 text-white border-gray-900' : 'bg-white text-gray-700 border-gray-200 hover:bg-gray-50'}`} onClick={() => setRange(opt.value)}>
            {opt.label}
          </button>
        ))}
      </div>
      {isCustom && (
        <div className="flex items-center gap-2 mb-3 flex-wrap">
          <DateTimeField label="开始时间" value={customStart} onChange={setCustomStart} max={customEnd} />
          <span className="text-xs text-gray-400">至</span>
          <DateTimeField label="结束时间" value={customEnd} onChange={setCustomEnd} min={customStart} />
          <button onClick={applyCustom} className="h-7 px-3 rounded-md bg-gray-900 text-white text-xs font-medium border-0 cursor-pointer hover:bg-gray-700 transition-colors">查询</button>
        </div>
      )}
      {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载趋势数据...</div> :
       error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> :
       (!points || points.length === 0) ? <div className="text-center p-8 text-gray-400 text-sm">{isCustom && !applied ? '请选择起止时间后点击「查询」' : '该时间范围暂无数据'}</div> : (
        <div className="grid grid-cols-2 gap-4 mt-3">
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.mem = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.qps = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.hit = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.conns = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.frag = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.repl = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.offset = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.keys = el; }}></div>
        </div>
      )}
    </section>
  );
}
