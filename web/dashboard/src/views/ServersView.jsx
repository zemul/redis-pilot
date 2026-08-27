// 服务器节点视图：卡片网格展示每台节点的容量/已分配资源与承载实例。
import { Server, Cpu, Database, HardDrive } from 'lucide-react';
import { EmptyState, StatusBadge, ProgressBar } from '../components/common.jsx';
import { getField, parseGi } from '../lib/format.js';

export function ServersView({ servers }) {
  if (servers.length === 0) {
    return <section className="border border-gray-200 rounded-lg bg-white"><EmptyState icon={Server} title="暂无服务器" detail="节点会从 pool-state.yaml 展示。" /></section>;
  }

  return (
    <div className="grid grid-cols-3 gap-4 max-lg:grid-cols-2 max-md:grid-cols-1">
      {servers.map((server) => <ServerCard key={server.name} server={server} />)}
    </div>
  );
}

function ServerCard({ server }) {
  const capacity = getField(server, 'capacity', 'Capacity', {}) || {};
  const allocated = getField(server, 'allocated', 'Allocated', {}) || {};
  const labels = getField(server, 'labels', 'Labels', {}) || {};
  const instances = getField(server, 'instances', 'Instances', []) || [];

  return (
    <section className="border border-gray-200 rounded-lg bg-white p-4">
      <div className="flex justify-between items-start gap-3">
        <div>
          <h3 className="m-0 mb-1 text-base font-semibold">{server.name}</h3>
          <code className="text-gray-400 font-mono text-xs">{getField(server, 'endpoint', 'Endpoint', '-')}</code>
        </div>
        <StatusBadge status={getField(server, 'status', 'Status')} />
      </div>
      <div className="mt-3 flex flex-wrap gap-1.5">
        {Object.keys(labels).length === 0 ? <span className="rounded-md px-2 py-0.5 bg-gray-100 text-gray-500 text-xs font-medium border border-gray-200">无标签</span> :
          Object.entries(labels).map(([key, value]) => <span className="rounded-md px-2 py-0.5 bg-gray-100 text-gray-500 text-xs font-medium border border-gray-200" key={key}>{key}: {value}</span>)}
      </div>
      <div className="mt-4 border-t border-gray-200 pt-4 grid gap-3.5">
        <ProgressBar
          label={<><Cpu size={13} /> CPU Core</>}
          current={Number(allocated.cpu_cores || allocated.CPUCores || 0)}
          max={Number(capacity.cpu_cores || capacity.CPUCores || 0)}
        />
        <ProgressBar
          label={<><Database size={13} /> Memory</>}
          current={parseGi(allocated.memory || allocated.Memory)}
          max={parseGi(capacity.memory || capacity.Memory)}
          unit="Gi"
        />
        <ProgressBar
          label={<><HardDrive size={13} /> 磁盘</>}
          current={parseGi(allocated.disk || allocated.Disk)}
          max={parseGi(capacity.disk || capacity.Disk)}
          unit="Gi"
        />
      </div>
      <div className="mt-3 text-xs text-gray-400">实例: {instances.length ? instances.join(', ') : '暂无'}</div>
    </section>
  );
}
