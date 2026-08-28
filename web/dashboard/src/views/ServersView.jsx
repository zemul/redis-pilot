// 服务器节点视图：紧凑表格展示每台节点的状态、实例数、资源分配与容量。
import { Server } from 'lucide-react';
import { EmptyState, StatusBadge } from '../components/common.jsx';
import { getField, parseGi } from '../lib/format.js';

/**
 * @param {Object} props
 * @param {Array} props.servers - 服务器列表（来自 /node/list）
 * @param {Array} props.groups - 实例组列表（来自 buildInstanceGroups），用于动态统计各节点实例数
 */
export function ServersView({ servers, groups = [] }) {
  if (servers.length === 0) {
    return <section className="border border-gray-200 rounded-lg bg-white"><EmptyState icon={Server} title="暂无服务器" detail="通过 CLI 添加节点后在此展示。" /></section>;
  }

  // 从实例组动态统计每台节点承载的实例数（与大盘保持一致）
  const instancesPerServer = {};
  for (const group of groups) {
    for (const inst of group.instances || []) {
      const s = getField(inst, 'server', 'Server', '');
      if (s) instancesPerServer[s] = (instancesPerServer[s] || 0) + 1;
    }
  }

  return (
    <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50 text-left text-xs font-medium text-gray-400">
              <th className="px-4 py-2.5 font-medium">节点</th>
              <th className="px-4 py-2.5 font-medium">状态</th>
              <th className="px-4 py-2.5 font-medium">实例数</th>
              <th className="px-4 py-2.5 font-medium">CPU（已分配 / 容量）</th>
              <th className="px-4 py-2.5 font-medium">内存（已分配 / 容量）</th>
              <th className="px-4 py-2.5 font-medium">磁盘（已分配 / 容量）</th>
              <th className="px-4 py-2.5 font-medium">标签</th>
              <th className="px-4 py-2.5 font-medium">最后心跳</th>
            </tr>
          </thead>
          <tbody>
            {servers.map((server) => (
              <ServerRow key={server.name} server={server} instanceCount={instancesPerServer[server.name] || 0} />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ServerRow({ server, instanceCount }) {
  const capacity = getField(server, 'capacity', 'Capacity', {}) || {};
  const allocated = getField(server, 'allocated', 'Allocated', {}) || {};
  const labels = getField(server, 'labels', 'Labels', {}) || {};
  const lastHeartbeat = getField(server, 'last_heartbeat', 'LastHeartbeat', '');

  const capCpu = Number(capacity.cpu_cores || capacity.CPUCores || 0);
  const allocCpu = Number(allocated.cpu_cores || allocated.CPUCores || 0);
  const capMem = parseGi(capacity.memory || capacity.Memory);
  const allocMem = parseGi(allocated.memory || allocated.Memory);
  const capDisk = parseGi(capacity.disk || capacity.Disk);
  const allocDisk = parseGi(allocated.disk || allocated.Disk);

  return (
    <tr className="border-b border-gray-100 last:border-b-0 hover:bg-gray-50/60">
      <td className="px-4 py-3">
        <div className="font-semibold text-gray-900">{server.name}</div>
        <code className="text-gray-400 font-mono text-[11px]">{getField(server, 'endpoint', 'Endpoint', '-')}</code>
      </td>
      <td className="px-4 py-3">
        <StatusBadge status={getField(server, 'status', 'Status')} />
      </td>
      <td className="px-4 py-3 font-mono tabular-nums">
        {instanceCount}
      </td>
      <td className="px-4 py-3">
        <ResourceCell allocated={allocCpu} capacity={capCpu} unit=" Core" />
      </td>
      <td className="px-4 py-3">
        <ResourceCell allocated={allocMem} capacity={capMem} unit=" Gi" />
      </td>
      <td className="px-4 py-3">
        <ResourceCell allocated={allocDisk} capacity={capDisk} unit=" Gi" />
      </td>
      <td className="px-4 py-3">
        <Labels labels={labels} />
      </td>
      <td className="px-4 py-3 text-xs text-gray-400 whitespace-nowrap">
        {formatHeartbeat(lastHeartbeat)}
      </td>
    </tr>
  );
}

/** 资源单元格：「已分配 / 容量」+ 百分比色条 */
function ResourceCell({ allocated, capacity, unit }) {
  const pct = capacity > 0 ? Math.round((allocated / capacity) * 100) : 0;
  const color = pct >= 90 ? 'bg-red-500' : pct >= 80 ? 'bg-amber-500' : 'bg-gray-900';
  return (
    <div className="min-w-[100px]">
      <span className="font-mono tabular-nums text-gray-700 text-[12px]">
        {allocated}{unit} <span className="text-gray-300">/</span> {capacity}{unit}
      </span>
      <div className="mt-1 h-1 rounded-full bg-gray-100 overflow-hidden">
        <span className={`block h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function Labels({ labels }) {
  const entries = Object.entries(labels || {});
  if (entries.length === 0) return <span className="text-xs text-gray-300">—</span>;
  return (
    <div className="flex flex-wrap gap-1">
      {entries.map(([k, v]) => (
        <span key={k} className="inline-block rounded px-1.5 py-0.5 bg-gray-100 text-[11px] text-gray-500 border border-gray-200">
          {k}: {v}
        </span>
      ))}
    </div>
  );
}

function formatHeartbeat(ts) {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    const now = Date.now();
    const diffMs = now - d.getTime();
    if (diffMs < 60_000) return '刚刚';
    if (diffMs < 3600_000) return `${Math.floor(diffMs / 60_000)} 分钟前`;
    if (diffMs < 86400_000) return `${Math.floor(diffMs / 3600_000)} 小时前`;
    return d.toLocaleDateString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  } catch {
    return ts;
  }
}
