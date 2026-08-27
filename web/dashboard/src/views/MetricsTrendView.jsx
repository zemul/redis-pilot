// 实例指标趋势视图：按时间范围拉取历史采样，用 echarts 渲染多张折线图（懒加载 echarts）。
import { useState, useEffect, useRef } from 'react';
import { AlertCircle } from 'lucide-react';

export function MetricsTrendView({ instanceName, requestFn }) {
  const [range_, setRange] = useState('1h');
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
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
    const s = Math.floor(new Date(customStart).getTime() / 1000);
    const e = Math.floor(new Date(customEnd).getTime() / 1000);
    if (!(s > 0) || !(e > s)) { setError('结束时间必须晚于开始时间'); return; }
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
          <input type="datetime-local" value={customStart} max={customEnd || undefined} onChange={(e) => setCustomStart(e.target.value)} className="h-7 px-2 rounded-md border border-gray-200 text-xs text-gray-700 outline-0 focus:border-gray-900" />
          <span className="text-xs text-gray-400">至</span>
          <input type="datetime-local" value={customEnd} min={customStart || undefined} onChange={(e) => setCustomEnd(e.target.value)} className="h-7 px-2 rounded-md border border-gray-200 text-xs text-gray-700 outline-0 focus:border-gray-900" />
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
