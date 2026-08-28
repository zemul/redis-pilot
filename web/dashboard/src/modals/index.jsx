// 全部弹窗组件：实例详情/慢查询/大Key/接入信息/配置/备份/删除确认/电源确认/创建实例。
// 弹窗多为一次性交互流程，集中于此文件，通过 props(requestFn/onClose 等)与外层解耦。
import { useState, useEffect } from 'react';
import {
  X, Info, BarChart3, AlertCircle, RefreshCw, Play, HardDrive,
  Calendar, Save, CheckCircle2, Trash2, Power, Database, Server, SlidersHorizontal,
  AlertTriangle,
} from 'lucide-react';
import { StatusBadge, EmptyState, CopyableText } from '../components/common.jsx';
import { getField, displayValue, formatDateTime, statusMeta, formatBytes, formatDuration } from '../lib/format.js';
import { extractMetricsPayload, parseRedisInfo, metricValue, engineLabel, topologyReason, parseReplicationLinks, topologyLabel } from '../lib/instance.js';
import { MetricsTrendView } from '../views/MetricsTrendView.jsx';

export function InstanceDetailModal({ detail, requestFn, onClose }) {
  const { item, group, loading, error, metrics } = detail;
  const payload = extractMetricsPayload(metrics);
  const info = payload.info || payload.Info || '';
  const sections = parseRedisInfo(info);
  const reason = topologyReason(group);
  const isReplication = group.type === 'replication' || group.replicas.length > 0;
  const [activeTab, setActiveTab] = useState('trend');
  const metricsSummary = [
    ['Redis 版本', metricValue(sections, 'redis_version')],
    ['角色', metricValue(sections, 'role', getField(item, 'role', 'Role', '-'))],
    ['已用内存', metricValue(sections, 'used_memory_human')],
    ['连接数', metricValue(sections, 'connected_clients')],
    ['Keyspace', displayValue(sections.keyspace || sections.Keyspace)],
    ['运行时长', formatDuration(metricValue(sections, 'uptime_in_seconds', ''))],
  ];
  // 期望 = 人的意图（仅 running/stopped）；观测 = 最近一次实际状态。
  // statusMeta 覆盖的是 Phase 派生态，missing/空串等原始观测值需单独映射。
  const desiredRaw = getField(item, 'desired_state', 'DesiredState', '');
  const observedRaw = getField(item, 'observed_status', 'ObservedStatus', '');
  const desiredLabel = { running: '运行', stopped: '停止' };
  const observedLabel = {
    running: '运行中', stopped: '已停止', missing: '容器不存在',
    unreachable: '不可达', '': '未观测',
  };
  const fields = [
    ['实例', item.name],
    ['实例组', group.groupName],
    ['引擎版本', engineLabel(group)],
    ['角色', getField(item, 'role', 'Role', '-')],
    ['节点', getField(item, 'server', 'Server', '-')],
    ['Redis 端口', getField(item, 'port', 'Port', '-')],
    ['期望状态', desiredLabel[desiredRaw] || displayValue(desiredRaw)],
    ['观测状态', observedLabel[observedRaw] || displayValue(observedRaw)],
    ['资源', `${getField(item, 'memory', 'Memory', '-')} / ${getField(item, 'cpus', 'CPUs', '-')}C / 磁盘 ${displayValue(getField(item, 'disk', 'Disk'), '0Gi')}`],
    ['数据目录', getField(item, 'data_path', 'DataPath', '-')],
    ['配置目录', getField(item, 'config_path', 'ConfigPath', '-')],
    ['备份目录', getField(item, 'backup_path', 'BackupPath', '-')],
    ['创建时间', formatDateTime(getField(item, 'created_at', 'CreatedAt', '-'))]
  ];

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">{item.name}</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{group.groupName} · {engineLabel(group)}</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {reason && (
            <div className="flex items-start gap-2.5 rounded-md border border-amber-200 bg-amber-50 px-3 py-2.5 text-[13px] text-amber-800">
              <AlertTriangle size={16} className="mt-0.5 shrink-0 text-amber-500" />
              <div className="min-w-0">
                <strong className="font-semibold">{reason.title}</strong>
                <p className="m-0 mt-0.5 text-[12px] leading-relaxed text-amber-700">{reason.detail}</p>
              </div>
            </div>
          )}
          <div className={`flex gap-0 border-b border-gray-200 -mx-4 px-4 ${reason ? '' : '-mt-4'}`}>
            <button className={`px-4 py-2.5 text-[13px] border-0 bg-transparent cursor-pointer border-b-2 mb-[-1px] ${activeTab === 'trend' ? 'text-gray-900 border-gray-900 font-medium' : 'text-gray-400 border-transparent hover:text-gray-700'}`} onClick={() => setActiveTab('trend')}>趋势</button>
            <button className={`px-4 py-2.5 text-[13px] border-0 bg-transparent cursor-pointer border-b-2 mb-[-1px] ${activeTab === 'detail' ? 'text-gray-900 border-gray-900 font-medium' : 'text-gray-400 border-transparent hover:text-gray-700'}`} onClick={() => setActiveTab('detail')}>详情</button>
            {isReplication && (
              <button className={`px-4 py-2.5 text-[13px] border-0 bg-transparent cursor-pointer border-b-2 mb-[-1px] ${activeTab === 'group' ? 'text-gray-900 border-gray-900 font-medium' : 'text-gray-400 border-transparent hover:text-gray-700'}`} onClick={() => setActiveTab('group')}>实例组</button>
            )}
          </div>
          {activeTab === 'detail' && (
            <>
              <section>
                <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Info size={16} className="text-gray-400" /> 实例详情</h4>
                <div className="grid grid-cols-3 gap-2.5 max-lg:grid-cols-2 max-md:grid-cols-1">
                  {fields.map(([label, value]) => (
                    <div key={label} className="min-w-0 border border-gray-200 rounded-md p-3 bg-gray-50">
                      <span className="block text-xs font-medium text-gray-400">{label}</span>
                      <strong className="block mt-1 text-[13px] font-medium break-all">{displayValue(value)}</strong>
                    </div>
                  ))}
                </div>
              </section>
              <section>
                <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><BarChart3 size={16} className="text-gray-400" /> 运行指标</h4>
                {loading ? (
                  <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取 metrics...</div>
                ) : error ? (
                  <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>
                ) : (
                  <>
                    <div className="grid grid-cols-3 gap-2.5 max-lg:grid-cols-2 max-md:grid-cols-1">
                      {metricsSummary.map(([label, value]) => (
                        <div key={label} className="min-w-0 border border-gray-200 rounded-md p-3 bg-gray-50">
                          <span className="block text-xs font-medium text-gray-400">{label}</span>
                          <strong className="block mt-1 text-[13px] font-mono font-normal break-all">{displayValue(value)}</strong>
                        </div>
                      ))}
                    </div>
                    <details className="mt-3 border border-gray-200 rounded-md bg-gray-50 overflow-hidden">
                      <summary className="px-3 py-2.5 cursor-pointer text-[13px] font-medium text-gray-500 hover:text-gray-900">查看 INFO 原文</summary>
                      <pre className="max-h-[320px] m-0 px-3 pb-3 overflow-auto font-mono text-xs leading-relaxed text-gray-500 whitespace-pre-wrap">{info || '无 metrics 数据'}</pre>
                    </details>
                  </>
                )}
              </section>
            </>
          )}
          {activeTab === 'trend' && (
            <MetricsTrendView instanceName={item.name} requestFn={requestFn} />
          )}
          {activeTab === 'group' && (
            <GroupTopologyTab group={group} master={item} sections={sections} loading={loading} error={error} />
          )}
        </div>
      </section>
    </div>
  );
}

// 主从复制观测 tab：展示组内节点拓扑 + 从主节点 INFO 解析出的复制健康（延迟/落后/同步状态）。
function GroupTopologyTab({ group, master, sections, loading, error }) {
  const repl = parseReplicationLinks(sections);
  const masterName = getField(master, 'name', 'Name', '-');
  const nodes = [
    { item: master, role: 'master' },
    ...group.replicas.map((r) => ({ item: r, role: 'replica' })),
  ];

  const stateMeta = (state) => {
    if (state === 'online') return { label: '在线', cls: 'text-emerald-600' };
    if (state === 'send_bulk' || state === 'wait_bgsave') return { label: '全量同步中', cls: 'text-amber-600' };
    return { label: state || '-', cls: 'text-gray-500' };
  };

  return (
    <div className="grid gap-4">
      <section>
        <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500">
          <Server size={16} className="text-gray-400" /> 拓扑 · {topologyLabel(group, master)}
        </h4>
        <div className="grid gap-2">
          {nodes.map(({ item, role }) => (
            <div key={getField(item, 'name', 'Name')} className="flex items-center gap-3 border border-gray-200 rounded-md px-3 py-2.5 bg-gray-50">
              <span className={`inline-flex items-center rounded px-1.5 py-0.5 text-[11px] font-medium ${role === 'master' ? 'bg-blue-50 text-blue-700 border border-blue-100' : 'bg-slate-100 text-slate-600 border border-slate-200'}`}>
                {role === 'master' ? '主' : '从'}
              </span>
              <div className="min-w-0 flex-1">
                <strong className="block text-[13px] font-medium truncate">{getField(item, 'name', 'Name', '-')}</strong>
                <span className="block text-[11px] text-gray-400 truncate">
                  {getField(item, 'server', 'Server', '-')} · :{getField(item, 'port', 'Port', '-')}
                </span>
              </div>
              <StatusBadge status={getField(item, 'status', 'Status')} />
            </div>
          ))}
        </div>
      </section>

      <section>
        <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500">
          <RefreshCw size={16} className="text-gray-400" /> 主从复制
        </h4>
        {loading ? (
          <div className="min-h-[80px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取主节点 INFO...</div>
        ) : error ? (
          <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> 无法从主节点获取复制状态：{error}</div>
        ) : (
          <>
            <div className="flex flex-wrap gap-2 mb-3 text-[12px]">
              <span className="inline-flex items-center gap-1.5 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-gray-600">
                在线从库 <strong className="font-mono text-gray-900">{repl.connectedSlaves}</strong>
              </span>
              <span className="inline-flex items-center gap-1.5 rounded-md border border-gray-200 bg-gray-50 px-2.5 py-1 text-gray-600">
                主偏移量 <strong className="font-mono text-gray-900">{repl.masterOffset || '-'}</strong>
              </span>
              {repl.syncInProgress && (
                <span className="inline-flex items-center gap-1.5 rounded-md border border-amber-200 bg-amber-50 px-2.5 py-1 text-amber-700 font-medium">全量同步进行中</span>
              )}
            </div>
            {repl.slaves.length === 0 ? (
              <div className="border border-dashed border-gray-200 rounded-md px-3 py-4 text-center text-[13px] text-gray-400">
                主节点当前未观测到已连接的从库。
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[440px] border-collapse text-[12px]">
                  <thead>
                    <tr className="text-gray-400 text-left border-b border-gray-200">
                      <th className="font-medium py-2 pr-3">从库</th>
                      <th className="font-medium py-2 pr-3">同步状态</th>
                      <th className="font-medium py-2 pr-3">延迟</th>
                      <th className="font-medium py-2">落后量</th>
                    </tr>
                  </thead>
                  <tbody>
                    {repl.slaves.map((s) => {
                      const meta = stateMeta(s.state);
                      return (
                        <tr key={`${s.ip}:${s.port}`} className="border-b border-gray-100 last:border-b-0">
                          <td className="py-2 pr-3 font-mono text-gray-700">{s.ip}:{s.port}</td>
                          <td className={`py-2 pr-3 font-medium ${meta.cls}`}>{meta.label}</td>
                          <td className="py-2 pr-3 font-mono text-gray-700">{s.lag !== null ? `${s.lag}s` : '-'}</td>
                          <td className="py-2 font-mono text-gray-700">{s.byteLag !== null ? formatBytes(s.byteLag) : '-'}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  );
}

export function SlowlogModal({ instanceName, requestFn, onClose }) {
  const [entries, setEntries] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError('');
    requestFn(`/instance/slowlog?name=${encodeURIComponent(instanceName)}&count=50`)
      .then((data) => { if (!ignore) setEntries(data?.entries || []); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName]);

  function reload() {
    setLoading(true);
    setError('');
    requestFn(`/instance/slowlog?name=${encodeURIComponent(instanceName)}&count=50`)
      .then((data) => setEntries(data?.entries || []))
      .catch((err) => setError(err.message || String(err)))
      .finally(() => setLoading(false));
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">慢查询</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载慢查询...</div> :
           error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> :
           (!entries || entries.length === 0) ? <div className="text-center p-8 text-gray-400 text-sm">无慢查询记录</div> : (
            <section>
              <div className="flex items-center justify-between mb-3">
                <span className="text-xs text-gray-400">{entries.length} 条记录</span>
                <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors" onClick={reload}><RefreshCw size={14} /> 刷新</button>
              </div>
              <table className="w-full border-collapse text-[13px]">
                <thead>
                  <tr>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">时间</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">耗时</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">命令</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">客户端</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((e, i) => (
                    <tr key={i} className="hover:bg-gray-50">
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '-'}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.duration_us >= 1000 ? `${(e.duration_us / 1000).toFixed(1)}ms` : `${e.duration_us}µs`}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top max-w-[320px] overflow-hidden text-ellipsis whitespace-nowrap">{e.command || '-'}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.client || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </div>
      </section>
    </div>
  );
}

export function BigkeysModal({ instanceName, requestFn, onClose }) {
  const [keys, setKeys] = useState(null);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  function run() {
    setLoading(true);
    setError('');
    requestFn(`/instance/bigkeys?name=${encodeURIComponent(instanceName)}`)
      .then((data) => { setKeys(data?.keys || []); setMeta({ scanned: data?.scanned, timeout: data?.timeout, elapsed_ms: data?.elapsed_ms }); })
      .catch((err) => setError(err.message || String(err)))
      .finally(() => setLoading(false));
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">大 Key 分析</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          <div className="flex items-center justify-between">
            <span className="text-xs text-gray-400">阈值: string &gt; 100KB，集合类 &gt; 10000 元素</span>
            <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors disabled:opacity-50" onClick={run} disabled={loading}>
              {loading ? <><RefreshCw size={14} className="animate-spin" /> 扫描中...</> : <><Play size={14} /> 开始扫描</>}
            </button>
          </div>
          {error && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>}
          {meta && (
            <div className="text-xs text-gray-400">
              已扫描 {meta.scanned} 个 key，耗时 {meta.elapsed_ms}ms
              {meta.timeout && <span className="text-amber-500"> （达到 60s 上限，结果可能不完整）</span>}
            </div>
          )}
          {keys && keys.length > 0 && (
            <table className="w-full border-collapse text-[13px]">
              <thead>
                <tr>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">Key</th>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">类型</th>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">大小</th>
                </tr>
              </thead>
              <tbody>
                {keys.map((k, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 max-w-[320px] overflow-hidden text-ellipsis whitespace-nowrap">{k.key}</td>
                    <td className="px-2.5 py-1.5 border-b border-gray-100">{k.type}</td>
                    <td className="px-2.5 py-1.5 font-mono border-b border-gray-100">{k.size_desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {keys && keys.length === 0 && !loading && <div className="text-center p-8 text-gray-400 text-sm">未发现大 Key</div>}
        </div>
      </section>
    </div>
  );
}

export function AccessModeModal({ group, publicHost, sentinelPort, onConfirm, onClose }) {
  const [mode, setMode] = useState(group.access_mode || 'standard');
  const proxy = getField(group, 'proxy', 'Proxy', {}) || {};
  const masterPort = proxy.master_port || proxy.MasterPort;
  const autoPort = proxy.auto_port || proxy.AutoPort;
  const isRwSplit = group.access_mode === 'rw-split';
  const directEndpoint = publicHost ? `${publicHost}:${isRwSplit ? (autoPort || masterPort) : (masterPort || autoPort)}` : (isRwSplit ? autoPort : masterPort) || '-';
  const sentinelEndpoint = publicHost && sentinelPort ? `${publicHost}:${sentinelPort}` : null;

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(500px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">接入信息</h3><span className="block mt-0.5 text-xs text-gray-400">{group.groupName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="p-4 space-y-4">
          {/* 接入点信息 */}
          <div className="space-y-2.5">
            <h4 className="m-0 text-[13px] font-medium text-gray-700">Redis 直连</h4>
            <div className="bg-gray-50 rounded-md px-3 py-2">
              <CopyableText text={directEndpoint} className="text-[13px] font-mono text-gray-900" />
            </div>
          </div>

          {sentinelEndpoint && (
            <div className="space-y-2.5">
              <h4 className="m-0 text-[13px] font-medium text-gray-700">Sentinel 模式</h4>
              <div className="bg-gray-50 rounded-md px-3 py-2 space-y-2">
                <div>
                  <span className="text-[11px] text-gray-400 block mb-0.5">Sentinel 地址</span>
                  <CopyableText text={sentinelEndpoint} className="text-[13px] font-mono text-gray-900" />
                </div>
                <div>
                  <span className="text-[11px] text-gray-400 block mb-0.5">Master Name</span>
                  <CopyableText text={group.groupName} className="text-[13px] font-mono text-gray-900" />
                </div>
              </div>
            </div>
          )}

          {/* 接入方式切换 */}
          <div className="pt-3 border-t border-gray-100">
            <h4 className="m-0 mb-2.5 text-[13px] font-medium text-gray-700">接入方式</h4>
            <label className="flex items-center gap-2 mb-2 cursor-pointer">
              <input type="radio" name="access_mode" value="standard" checked={mode === 'standard'} onChange={() => setMode('standard')} />
              <span className="text-[13px]"><strong className="font-medium">标准</strong> — 所有流量走主库</span>
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="radio" name="access_mode" value="rw-split" checked={mode === 'rw-split'} onChange={() => setMode('rw-split')} />
              <span className="text-[13px]"><strong className="font-medium">读写分离</strong> — 读请求分发到从库</span>
            </label>
            {mode !== (group.access_mode || 'standard') && (() => {
              const newPort = mode === 'rw-split' ? (autoPort || masterPort) : (masterPort || autoPort);
              const newEndpoint = publicHost ? `${publicHost}:${newPort}` : newPort;
              return newPort ? (
                <p className="text-[12px] text-amber-600 bg-amber-50 border border-amber-200 rounded px-2.5 py-1.5 mt-2 mb-0">
                  切换后接入端口将变更为 <span className="font-mono font-medium">{newEndpoint}</span>
                </p>
              ) : null;
            })()}
            <div className="mt-3 flex justify-end gap-2">
              <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onClose}>关闭</button>
              <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50" disabled={mode === (group.access_mode || 'standard')} onClick={() => onConfirm(mode)}>切换模式</button>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export function ConfigModal({ instanceName, requestFn, onClose }) {
  const VISIBLE_KEYS = [
    'maxmemory', 'maxmemory-policy', 'hz', 'timeout', 'tcp-keepalive', 'tcp-backlog',
    'databases', 'maxclients', 'slowlog-log-slower-than', 'slowlog-max-len',
    'loglevel', 'latency-tracking', 'activedefrag', 'active-defrag-enabled',
    'active-defrag-ignore-bytes', 'active-defrag-threshold-lower', 'active-defrag-threshold-upper',
    'active-defrag-cycle-min', 'active-defrag-cycle-max',
    'lazyfree-lazy-eviction', 'lazyfree-lazy-expire', 'lazyfree-lazy-server-del',
    'no-appendfsync-on-rewrite', 'auto-aof-rewrite-percentage', 'auto-aof-rewrite-min-size',
    'aof-use-rdb-preamble', 'save', 'rdbcompression', 'rdbchecksum',
    'list-max-ziplist-size', 'list-max-listpack-size', 'zset-max-ziplist-entries', 'zset-max-ziplist-value',
    'zset-max-listpack-entries', 'zset-max-listpack-value',
    'hash-max-ziplist-entries', 'hash-max-ziplist-value', 'hash-max-listpack-entries', 'hash-max-listpack-value',
    'set-max-intset-entries', 'set-max-listpack-entries',
    'notify-keyspace-events', 'client-output-buffer-limit',
    'proto-max-bulk-len', 'lua-time-limit', 'min-replicas-to-write', 'min-replicas-max-lag',
    'io-threads', 'io-threads-do-reads',
    'rocksdb.compression', 'rocksdb.write_buffer_size', 'rocksdb.max_write_buffer_number',
    'rocksdb.target_file_size_base', 'rocksdb.max_background_compactions', 'rocksdb.max_background_flushes',
    'rocksdb.block_size', 'rocksdb.metadata_block_size', 'rocksdb.enable_blob_files',
  ];
  const visibleSet = new Set(VISIBLE_KEYS);

  const [config, setConfig] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [filter, setFilter] = useState('');
  const [edits, setEdits] = useState({});
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState('');

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError('');
    requestFn(`/instance/config?name=${encodeURIComponent(instanceName)}`)
      .then((data) => { if (!ignore) setConfig(data?.config || {}); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName]);

  const filteredKeys = config ? Object.keys(config).filter((k) => visibleSet.has(k) && (!filter || k.includes(filter))).sort() : [];
  const hasEdits = Object.keys(edits).length > 0;

  function handleEdit(key, value) {
    if (value === config[key]) {
      const next = { ...edits };
      delete next[key];
      setEdits(next);
    } else {
      setEdits({ ...edits, [key]: value });
    }
  }

  async function handleSave() {
    setSaving(true);
    setSaveResult('');
    try {
      await requestFn('/instance/config', {
        method: 'POST',
        body: JSON.stringify({ name: instanceName, config_overrides: edits })
      });
      setSaveResult(`已保存 ${Object.keys(edits).length} 项配置`);
      setConfig((prev) => ({ ...prev, ...edits }));
      setEdits({});
    } catch (err) {
      setSaveResult(`保存失败: ${err.message || String(err)}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(720px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">修改配置</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载配置...</div> :
           error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> : (
            <>
              <div className="flex items-center justify-between gap-3">
                <input className="flex-1 max-w-[300px] h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="搜索配置项..." />
                <span className="text-xs text-gray-400">{filteredKeys.length} 项</span>
                {hasEdits && (
                  <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors" onClick={handleSave} disabled={saving}>
                    {saving ? '保存中...' : `保存 ${Object.keys(edits).length} 项修改`}
                  </button>
                )}
              </div>
              {saveResult && <div className="border border-gray-200 rounded-md px-3 py-2 text-[13px] text-gray-600">{saveResult}</div>}
              <div className="overflow-auto max-h-[480px]">
                <table className="w-full border-collapse text-[13px]">
                  <thead>
                    <tr>
                      <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs w-[40%]">配置项</th>
                      <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">值</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredKeys.map((key) => {
                      const current = edits[key] !== undefined ? edits[key] : config[key];
                      const modified = edits[key] !== undefined;
                      return (
                        <tr key={key} className={modified ? 'bg-gray-900/5' : ''}>
                          <td className="px-2.5 py-1.5 font-mono border-b border-gray-100">{key}</td>
                          <td className="px-2.5 py-1.5 border-b border-gray-100">
                            <input
                              className={`w-full border bg-transparent px-1 py-0.5 font-mono text-xs rounded transition-colors focus:outline-none focus:border-gray-900 ${modified ? 'border-gray-900' : 'border-transparent'}`}
                              value={current}
                              onChange={(e) => handleEdit(key, e.target.value)}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </section>
    </div>
  );
}

export function BackupListModal({ detail, requestFn, onRefresh, onClose }) {
  const { item, group, loading, error, backups } = detail;
  const [execLoading, setExecLoading] = useState(false);
  const [execMsg, setExecMsg] = useState('');
  const [scheduleLoading, setScheduleLoading] = useState(true);
  const [schedule, setSchedule] = useState('');
  const [retention, setRetention] = useState(7);
  const [scheduleError, setScheduleError] = useState('');
  const [scheduleSaving, setScheduleSaving] = useState(false);
  const [scheduleSaved, setScheduleSaved] = useState(false);

  useEffect(() => {
    let ignore = false;
    async function loadSchedule() {
      try {
        const data = await requestFn(`/backup/schedule?name=${encodeURIComponent(item.name)}`);
        if (!ignore) {
          setSchedule(data?.schedule || '');
          setRetention(data?.retention || 7);
        }
      } catch (err) {
        if (!ignore) setScheduleError(err.message || String(err));
      } finally {
        if (!ignore) setScheduleLoading(false);
      }
    }
    loadSchedule();
    return () => { ignore = true; };
  }, [item.name]);

  const isMaster = getField(item, 'role', 'Role') === 'master';
  const hasReplicas = group.replicas && group.replicas.length > 0;

  async function handleExecBackup() {
    setExecLoading(true);
    setExecMsg('');
    try {
      const res = await requestFn('/backup/exec', {
        method: 'POST',
        body: JSON.stringify({ name: item.name })
      });
      const taskId = res?.task_id;
      if (!taskId) {
        setExecMsg('备份已触发');
        setExecLoading(false);
        return;
      }
      setExecMsg('备份执行中...');
      const poll = setInterval(async () => {
        try {
          const statusRes = await requestFn(`/backup/status?name=${encodeURIComponent(item.name)}&task_id=${encodeURIComponent(taskId)}`);
          const status = statusRes?.status;
          if (status === 'success') {
            clearInterval(poll);
            setExecMsg('备份完成');
            setExecLoading(false);
          } else if (status === 'failed' || status === 'interrupted') {
            clearInterval(poll);
            setExecMsg((status === 'interrupted' ? '备份中断: ' : '备份失败: ') + (statusRes?.error || '未知错误'));
            setExecLoading(false);
          }
        } catch {
          clearInterval(poll);
          setExecMsg('查询状态失败');
          setExecLoading(false);
        }
      }, 2000);
    } catch (err) {
      setExecMsg('备份触发失败: ' + (err.message || String(err)));
      setExecLoading(false);
    }
  }

  async function handleSaveSchedule() {
    setScheduleSaving(true);
    setScheduleError('');
    setScheduleSaved(false);
    try {
      await requestFn('/backup/schedule', {
        method: 'POST',
        body: JSON.stringify({ name: item.name, schedule, retention: Number(retention) || 7 })
      });
      setScheduleSaved(true);
    } catch (err) {
      setScheduleError(err.message || String(err));
    } finally {
      setScheduleSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">{item.name} 备份管理</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{group.groupName}</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          <section>
            <div className="flex items-center justify-between gap-3 mb-3">
              <h4 className="m-0 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><HardDrive size={16} className="text-gray-400" /> 可用备份</h4>
              <button
                className="h-7 px-2.5 rounded-md bg-gray-900 text-white text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50"
                onClick={handleExecBackup}
                disabled={execLoading}
              >
                {execLoading ? <><RefreshCw size={14} className="animate-spin" /> 备份中...</> : <><Play size={14} /> 立即备份</>}
              </button>
            </div>
            {execMsg && <div className={`border rounded-md px-3 py-2 flex items-center gap-2 text-[13px] mb-2 ${execMsg.includes('失败') ? 'border-red-200 bg-red-50 text-red-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'}`}><AlertCircle size={16} /> {execMsg}</div>}
            {isMaster && hasReplicas && <div className="text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2 mb-2">⚠️ 当前为主库，建议在从库上执行备份以避免影响主库性能。</div>}
            {loading ? (
              <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取备份列表...</div>
            ) : error ? (
              <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>
            ) : backups.length === 0 ? (
              <EmptyState icon={HardDrive} title="暂无备份" detail="该实例当前没有可用备份文件。" />
            ) : (
              <div className="grid gap-2">
                {backups.map((backup) => (
                  <div className="border border-gray-200 rounded-md p-3 grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 bg-gray-50" key={backup.name}>
                    <HardDrive size={18} className="text-gray-400" />
                    <div>
                      <strong className="block font-mono text-[13px] font-medium">{backup.time}</strong>
                      <span className="block mt-0.5 text-xs text-gray-400 font-mono">{backup.type} · {backup.size}</span>
                    </div>
                    <StatusBadge status={backup.status === '成功' ? 'running' : 'failed'}>
                      {backup.status}
                    </StatusBadge>
                  </div>
                ))}
              </div>
            )}
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Calendar size={16} className="text-gray-400" /> 定时备份策略</h4>
            {scheduleLoading ? (
              <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取策略...</div>
            ) : (
              <div className="grid gap-3">
                {scheduleError && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {scheduleError}</div>}
                {scheduleSaved && <div className="border border-emerald-200 rounded-md px-3 py-2 flex items-center gap-2 bg-emerald-50 text-emerald-700 text-[13px]"><CheckCircle2 size={16} /> 策略已保存</div>}
                <div className="grid grid-cols-2 gap-3">
                  <label className="block text-xs font-medium text-gray-400">Cron 表达式
                    <input
                      className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]"
                      value={schedule}
                      onChange={(e) => { setSchedule(e.target.value); setScheduleSaved(false); }}
                      placeholder="0 2 * * *（每天凌晨2点）"
                    />
                  </label>
                  <label className="block text-xs font-medium text-gray-400">保留天数
                    <input
                      className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]"
                      type="number"
                      min={1}
                      value={retention}
                      onChange={(e) => { setRetention(e.target.value); setScheduleSaved(false); }}
                    />
                  </label>
                </div>
                <div className="text-xs text-gray-400">
                  留空 Cron 表达式则关闭定时备份。常用: <code className="bg-gray-100 px-1 py-0.5 rounded text-[11px] font-mono">0 2 * * *</code>(每天2:00)、<code className="bg-gray-100 px-1 py-0.5 rounded text-[11px] font-mono">0 */6 * * *</code>(每6小时)
                </div>
                <div className="flex justify-end">
                  <button
                    className="h-7 px-2.5 rounded-md bg-gray-900 text-white text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50"
                    onClick={handleSaveSchedule}
                    disabled={scheduleSaving}
                  >
                    {scheduleSaving ? <><RefreshCw size={14} className="animate-spin" /> 保存中...</> : <><Save size={14} /> 保存策略</>}
                  </button>
                </div>
              </div>
            )}
          </section>
        </div>
      </section>
    </div>
  );
}

export function DeleteConfirmModal({ detail, loading, onCancel, onConfirm, onToggleClean }) {
  const { item, group, cleanData } = detail;
  const role = getField(item, 'role', 'Role', '-');
  const status = getField(item, 'status', 'Status', '-');

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onCancel}>
      <section className="w-[min(460px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="px-4 py-4 border-b border-gray-200 flex items-center gap-3">
          <div className="w-10 h-10 rounded-md grid place-items-center bg-red-50 text-red-600 shrink-0">
            <Trash2 size={20} />
          </div>
          <div>
            <h3 className="m-0 text-base font-semibold">确认删除实例</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="p-4">
          <p className="m-0 text-[13px] text-gray-900 leading-relaxed">
            将要删除实例 <strong className="font-semibold">{item.name}</strong>。此操作不可恢复，实例容器将被停止并移除。
          </p>
          <div className="mt-3 grid gap-1.5 text-[13px] text-gray-500">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
          <label className="flex items-center gap-2 mt-3.5 p-2.5 border border-gray-200 rounded-md bg-gray-50 text-[13px] text-gray-600 cursor-pointer">
            <input type="checkbox" checked={cleanData} onChange={(e) => onToggleClean(e.target.checked)} />
            同时删除数据文件（备份 + 数据目录）
          </label>
        </div>
        <div className="px-4 pb-4 pt-2 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onCancel} disabled={loading}>取消</button>
          <button className="h-[34px] rounded-md px-3 bg-red-600 text-white font-medium text-[13px] hover:bg-red-700 border-0 cursor-pointer transition-colors disabled:opacity-50" onClick={onConfirm} disabled={loading}>
            {loading ? '删除中...' : '确认删除'}
          </button>
        </div>
      </section>
    </div>
  );
}

export function ConfirmPowerModal({ detail, loading, onCancel, onConfirm }) {
  const { item, group, action } = detail;
  const isStop = action === 'stop';
  const title = isStop ? '确认停止实例' : '确认启动实例';
  const actionText = isStop ? '停止' : '启动';
  const role = getField(item, 'role', 'Role', '-');
  const status = getField(item, 'status', 'Status', '-');

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onCancel}>
      <section className="w-[min(460px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="px-4 py-4 border-b border-gray-200 flex items-center gap-3">
          <div className={`w-10 h-10 rounded-md grid place-items-center shrink-0 ${isStop ? 'bg-red-50 text-red-600' : 'bg-gray-100 text-gray-600'}`}>
            {isStop ? <Power size={20} /> : <Play size={20} />}
          </div>
          <div>
            <h3 className="m-0 text-base font-semibold">{title}</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="p-4">
          <p className="m-0 text-[13px] text-gray-900 leading-relaxed">
            将要{actionText}实例 <strong className="font-semibold">{item.name}</strong>。
            {isStop ? '停止后该实例上的 Redis 服务会不可用，副本冗余或读流量可能受影响。' : '启动会尝试恢复该实例容器。'}
          </p>
          <div className="mt-3 grid gap-1.5 text-[13px] text-gray-500">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
        </div>
        <div className="px-4 pb-4 pt-2 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onCancel} disabled={loading}>取消</button>
          <button className={`h-[34px] rounded-md px-3 font-medium text-[13px] border-0 cursor-pointer transition-colors disabled:opacity-50 ${isStop ? 'bg-red-600 text-white hover:bg-red-700' : 'bg-gray-900 text-white hover:bg-gray-700'}`} onClick={onConfirm} disabled={loading}>
            {loading ? '处理中...' : `确认${actionText}`}
          </button>
        </div>
      </section>
    </div>
  );
}

export function ResizeModal({ instanceName, currentSpecs, requestFn, onClose }) {
  const [memory, setMemory] = useState(currentSpecs?.memory || '');
  const [cpus, setCpus] = useState(currentSpecs?.cpus || '');
  const [disk, setDisk] = useState(currentSpecs?.disk || '');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState('');

  const hasChange = memory !== (currentSpecs?.memory || '') || cpus !== (currentSpecs?.cpus || '') || disk !== (currentSpecs?.disk || '');

  async function handleSubmit(e) {
    e.preventDefault();
    setSubmitting(true);
    setResult('');
    const body = { name: instanceName };
    if (memory && memory !== (currentSpecs?.memory || '')) body.memory = memory;
    if (cpus && cpus !== (currentSpecs?.cpus || '')) body.cpus = Number(cpus);
    if (disk && disk !== (currentSpecs?.disk || '')) body.disk = disk;
    if (!body.memory && !body.cpus && !body.disk) {
      setResult('至少修改一项规格');
      setSubmitting(false);
      return;
    }
    try {
      await requestFn('/instance/resize', { method: 'POST', body: JSON.stringify(body) });
      setResult('调整成功');
      setTimeout(onClose, 800);
    } catch (err) {
      setResult(`失败: ${err.message || String(err)}`);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(460px,100%)] rounded-xl border border-gray-200 overflow-hidden bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[56px] px-4 py-3.5 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">调整规格</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-4 grid gap-4">
          <label className="grid gap-1 text-[13px] font-medium text-gray-700">
            内存
            <input className="h-9 px-3 border border-gray-200 rounded-md text-[13px] focus:outline-none focus:border-gray-900 focus:ring-1 focus:ring-gray-900" placeholder="例如 2Gi" value={memory} onChange={(e) => setMemory(e.target.value)} />
          </label>
          <label className="grid gap-1 text-[13px] font-medium text-gray-700">
            CPU
            <input className="h-9 px-3 border border-gray-200 rounded-md text-[13px] focus:outline-none focus:border-gray-900 focus:ring-1 focus:ring-gray-900" type="number" step="0.1" min="0.1" placeholder="例如 2" value={cpus} onChange={(e) => setCpus(e.target.value)} />
          </label>
          <label className="grid gap-1 text-[13px] font-medium text-gray-700">
            磁盘
            <input className="h-9 px-3 border border-gray-200 rounded-md text-[13px] focus:outline-none focus:border-gray-900 focus:ring-1 focus:ring-gray-900" placeholder="例如 20Gi" value={disk} onChange={(e) => setDisk(e.target.value)} />
          </label>
          {result && (
            <div className={`text-[13px] px-3 py-2 rounded-md border ${result.startsWith('失败') ? 'border-red-200 bg-red-50 text-red-700' : 'border-green-200 bg-green-50 text-green-700'}`}>
              {result}
            </div>
          )}
          <div className="flex justify-end gap-2 pt-2">
            <button type="button" className="h-9 px-4 rounded-md border border-gray-200 bg-white text-[13px] text-gray-700 hover:bg-gray-50 transition-colors cursor-pointer" onClick={onClose}>取消</button>
            <button type="submit" disabled={submitting || !hasChange} className="h-9 px-4 rounded-md bg-gray-900 text-white text-[13px] font-medium hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors cursor-pointer">{submitting ? '提交中...' : '确认调整'}</button>
          </div>
        </form>
      </section>
    </div>
  );
}

export function CreateInstanceModal({ servers, groups, requestFn, onRefresh, onClose }) {
  const [form, setForm] = useState({
    group: '', engine: 'redis', engine_version: '', category: 'cache',
    joinExisting: false, replicaCount: 1, access_mode: 'standard',
    memory: '', memory_unit: 'Gi', cpus: '', disk: '', disk_unit: 'Gi',
    server: '', password: '', config_overrides: ''
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [progress, setProgress] = useState(null);
  const [engines, setEngines] = useState(null);

  useEffect(() => {
    requestFn('/server/info').then((data) => setEngines(data?.engines || null)).catch(() => {});
  }, []);

  const currentVersions = engines && engines[form.engine] ? engines[form.engine].versions || [] : [];
  const defaultVersion = engines && engines[form.engine] ? engines[form.engine].default || '' : '';

  const replicaGroups = groups.filter((g) => g.master);
  const isReplica = form.joinExisting;

  function update(key, value) {
    setForm((prev) => {
      const next = { ...prev, [key]: value };
      if (key === 'joinExisting') {
        next.group = '';
        next.replicaCount = 1;
      }
      if (key === 'group' && prev.joinExisting) {
        const g = replicaGroups.find((g) => g.groupName === value);
        if (g) {
          next.category = g.category || prev.category;
          next.engine = g.engine || prev.engine;
        }
      }
      return next;
    });
  }

  function parseOverrides(text) {
    const map = {};
    for (const line of String(text || '').split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const eq = trimmed.indexOf('=');
      if (eq < 0) continue;
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (key) map[key] = val;
    }
    return Object.keys(map).length ? map : undefined;
  }

  function buildInstances() {
    const overrides = parseOverrides(form.config_overrides);
    const base = {
      category: form.category,
      engine: form.engine,
      memory: form.memory.trim() + form.memory_unit,
      cpus: form.cpus ? Number(form.cpus) : 0,
      ...(form.server.trim() ? { server: form.server.trim() } : {}),
      ...(form.disk.trim() ? { disk: form.disk.trim() + form.disk_unit } : {}),
      ...(form.password ? { password: form.password } : {}),
      ...(overrides ? { config_overrides: overrides } : {})
    };

    if (isReplica) {
      const group = replicaGroups.find((g) => g.groupName === form.group);
      if (!group || !group.master) return [];
      const masterName = group.master.name;
      let maxN = -1;
      for (const inst of group.instances) {
        const m = String(inst.name).match(/-(\d+)$/);
        if (m) maxN = Math.max(maxN, parseInt(m[1]));
      }
      const list = [];
      for (let i = 0; i < form.replicaCount; i++) {
        const name = `${form.group}-${maxN + 1 + i}`;
        list.push({
          name, role: '副本',
          payload: { ...base, name, type: 'replication', replica_of: masterName }
        });
      }
      return list;
    }

    const isStandalone = form.replicaCount === 0;
    const masterName = isStandalone ? form.group : `${form.group}-0`;
    const list = [{
      name: masterName,
      role: isStandalone ? '单点' : '主库',
      payload: {
        ...base,
        name: masterName,
        type: isStandalone ? 'standalone' : 'replication',
        group: form.group,
        ...(form.engine_version.trim() ? { engine_version: form.engine_version.trim() } : {}),
        ...(!isStandalone && form.access_mode ? { access_mode: form.access_mode } : {})
      }
    }];
    for (let i = 1; i <= form.replicaCount; i++) {
      const name = `${form.group}-${i}`;
      list.push({
        name, role: '副本',
        payload: { ...base, name, type: 'replication', replica_of: masterName }
      });
    }
    return list;
  }

  const instances = buildInstances();

  async function handleSubmit() {
    setError('');
    if (!form.group.trim()) { setError(isReplica ? '请选择实例组' : '请填写名称'); return; }
    if (!form.memory.trim()) { setError('请填写内存'); return; }

    setSubmitting(true);
    const created = [];
    for (let i = 0; i < instances.length; i++) {
      const inst = instances[i];
      setProgress({ current: i + 1, total: instances.length, name: inst.name });
      try {
        await requestFn('/instance/create', { method: 'POST', body: JSON.stringify(inst.payload) });
        created.push(inst.name);
      } catch (err) {
        const msg = err.message || String(err);
        if (created.length > 0) {
          setError(`已创建 ${created.length}/${instances.length} 个（${created.join('、')}）。${inst.name} 失败：${msg}`);
          await onRefresh();
        } else {
          setError(`${inst.name} 创建失败：${msg}`);
        }
        setProgress(null);
        setSubmitting(false);
        return;
      }
    }
    setProgress(null);
    setSubmitting(false);
    await onRefresh();
    onClose();
  }

  const inputCls = "mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] text-[13px] transition-all disabled:opacity-50 disabled:cursor-not-allowed";
  const selectCls = inputCls;

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={submitting ? undefined : onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)_auto] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">创建实例</h3>
            <span className="block mt-0.5 text-xs text-gray-400">新建 Redis / Kvrocks 实例</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {error && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>}
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Info size={16} className="text-gray-400" /> 基本信息</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">{isReplica ? '实例组' : '名称'} <span className="text-red-500">*</span>
                {isReplica ? (
                  <select className={selectCls} value={form.group} onChange={(e) => update('group', e.target.value)}>
                    <option value="">选择组</option>
                    {replicaGroups.map((g) => <option key={g.groupName} value={g.groupName}>{g.groupName}</option>)}
                  </select>
                ) : (
                  <input className={inputCls} value={form.group} onChange={(e) => update('group', e.target.value)} placeholder="如 order" />
                )}
              </label>
              <label className="block text-xs font-medium text-gray-400">类别 <span className="text-red-500">*</span>
                <select className={selectCls} value={form.category} onChange={(e) => update('category', e.target.value)} disabled={isReplica}>
                  <option value="cache">缓存</option>
                  <option value="persistent">持久化</option>
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">引擎 <span className="text-red-500">*</span>
                <select className={selectCls} value={form.engine} onChange={(e) => update('engine', e.target.value)} disabled={isReplica}>
                  <option value="redis">Redis</option>
                  <option value="kvrocks">Kvrocks</option>
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">引擎版本
                <select className={selectCls} value={form.engine_version} onChange={(e) => update('engine_version', e.target.value)} disabled={isReplica}>
                  <option value="">默认{defaultVersion ? ` (${defaultVersion})` : ''}</option>
                  {currentVersions.map((v) => <option key={v} value={v}>{v}</option>)}
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">副本数量 <span className="text-red-500">*</span>
                <input className={inputCls} type="number" min={0} value={form.replicaCount} onChange={(e) => update('replicaCount', Math.max(0, Number(e.target.value) || 0))} />
              </label>
              <label className="block text-xs font-medium text-gray-400">接入方式
                <select className={selectCls} value={form.access_mode} onChange={(e) => update('access_mode', e.target.value)} disabled={isReplica || form.replicaCount === 0}>
                  <option value="standard">标准（全部走主库）</option>
                  <option value="rw-split">读写分离</option>
                </select>
              </label>
            </div>
            {replicaGroups.length > 0 && (
              <label className="flex items-center gap-2 mt-2.5 text-[13px] text-gray-500 cursor-pointer">
                <input type="checkbox" checked={form.joinExisting} onChange={(e) => update('joinExisting', e.target.checked)} disabled={submitting} />
                添加副本到已有组
              </label>
            )}
            {isReplica && <p className="mt-2 text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2">引擎 / 版本 / 类别将从主库自动继承。副本数量为本次新增的副本数。</p>}
            {!isReplica && <p className="mt-2 text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2">副本数量 0 = 单点，1+ = 主从。实例名自动生成为 {form.group || '组名'}-0、{form.group || '组名'}-1 …</p>}
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Database size={16} className="text-gray-400" /> 资源配置</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">内存 <span className="text-red-500">*</span>
                <div className="flex gap-1.5 mt-1.5">
                  <input className="flex-1 min-w-0 h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" type="number" value={form.memory} onChange={(e) => update('memory', e.target.value)} placeholder="如 2" />
                  <select className="shrink-0 w-16 h-9 border border-gray-200 rounded-md px-2 bg-white text-gray-900 outline-0 text-[13px]" value={form.memory_unit} onChange={(e) => update('memory_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                    <option value="Mi">Mi</option>
                  </select>
                </div>
              </label>
              <label className="block text-xs font-medium text-gray-400">CPU 核数
                <input className={inputCls} type="number" step="0.5" value={form.cpus} onChange={(e) => update('cpus', e.target.value)} placeholder="如 1" />
              </label>
              <label className="block text-xs font-medium text-gray-400">磁盘
                <div className="flex gap-1.5 mt-1.5">
                  <input className="flex-1 min-w-0 h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" type="number" value={form.disk} onChange={(e) => update('disk', e.target.value)} placeholder="如 10" />
                  <select className="shrink-0 w-16 h-9 border border-gray-200 rounded-md px-2 bg-white text-gray-900 outline-0 text-[13px]" value={form.disk_unit} onChange={(e) => update('disk_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                  </select>
                </div>
              </label>
            </div>
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Server size={16} className="text-gray-400" /> 部署选项</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">目标节点
                <select className={selectCls} value={form.server} onChange={(e) => update('server', e.target.value)}>
                  <option value="">自动调度</option>
                  {servers.map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">密码
                <input className={inputCls} type="password" value={form.password} onChange={(e) => update('password', e.target.value)} placeholder="留空则无密码" />
              </label>
            </div>
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><SlidersHorizontal size={16} className="text-gray-400" /> 高级</h4>
            <div className="grid grid-cols-1 gap-3 mt-3">
              <label className="block text-xs font-medium text-gray-400">配置覆盖
                <textarea className="mt-1.5 w-full border border-gray-200 rounded-md px-2.5 py-2 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px] font-inherit resize-y min-h-[80px]" rows="4" value={form.config_overrides} onChange={(e) => update('config_overrides', e.target.value)} placeholder={'每行一个，格式 key=value\n如 maxmemory-policy=allkeys-lru'} />
              </label>
            </div>
          </section>
          {instances.length > 0 && !submitting && (
            <section>
              <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><CheckCircle2 size={16} className="text-gray-400" /> 将创建以下实例</h4>
              <div className="grid gap-1.5">
                {instances.map((inst) => (
                  <div key={inst.name} className="flex items-center gap-2.5 px-3 py-2 border border-gray-200 rounded-md bg-gray-50">
                    <code className="font-mono text-[13px] text-gray-900">{inst.name}</code>
                    <span className="ml-auto text-xs text-gray-400">{inst.role}</span>
                  </div>
                ))}
              </div>
            </section>
          )}
          {progress && (
            <div className="border border-gray-200 rounded-md px-3 py-2.5 flex items-center gap-2 text-[13px] text-gray-600"><RefreshCw size={16} className="animate-spin" /> 正在创建 {progress.name}（{progress.current}/{progress.total}）…</div>
          )}
        </div>
        <div className="px-4 pb-4 pt-3 border-t border-gray-200 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onClose} disabled={submitting}>取消</button>
          <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50" onClick={handleSubmit} disabled={submitting || instances.length === 0}>
            {submitting ? '创建中…' : `创建 ${instances.length} 个实例`}
          </button>
        </div>
      </section>
    </div>
  );
}
