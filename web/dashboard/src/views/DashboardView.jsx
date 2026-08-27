// 大盘看板视图：顶部汇总条 + 实例运行状态表 + 右侧服务器/Sentinel/最近操作列。
import { Fragment, useState } from 'react';
import { ChevronRight, ChevronDown, Database, Server, ShieldCheck, Clock, GitMerge } from 'lucide-react';
import { EmptyState, StatusBadge, StatusDot, ResourceValue, SummaryCell } from '../components/common.jsx';
import { getField, parseGi } from '../lib/format.js';
import { engineLabel, topologyLabel, instanceStatusTone } from '../lib/instance.js';
import { AuditCard } from './AuditView.jsx';

// 大盘看板的实例运行状态表：按组分层，组头行汇总组信息，实例行展示每个实例的
// 运行状态/角色/所在服务器/内存水位，异常实例整行着色便于一眼定位。
function DashboardInstanceTable({ groups, onOpenDetail }) {
  const [collapsed, setCollapsed] = useState(() => new Set());
  const toggleGroup = (name) => setCollapsed((prev) => {
    const next = new Set(prev);
    if (next.has(name)) next.delete(name); else next.add(name);
    return next;
  });

  if (groups.length === 0) {
    return <div className="p-4"><EmptyState title="暂无实例组" detail="创建实例后会在这里展示运行状态。" /></div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] border-collapse">
        <thead>
          <tr>
            <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例</th>
            <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">角色</th>
            <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">服务器</th>
            <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">内存</th>
            <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">状态</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => {
            const running = group.instances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
            const total = group.instances.length;
            const isCollapsed = collapsed.has(group.groupName);
            const master = group.master;
            const ordered = [...group.instances].sort((a, b) => {
              const wa = ['master', 'standalone'].includes(getField(a, 'role', 'Role')) ? 0 : 1;
              const wb = ['master', 'standalone'].includes(getField(b, 'role', 'Role')) ? 0 : 1;
              return wa - wb;
            });
            return (
              <Fragment key={group.groupName}>
                <tr className="bg-gray-50/80 border-y border-gray-200">
                  <td colSpan={5} className="px-4 py-2.5">
                    <div className="flex items-center gap-3 flex-wrap">
                      <button
                        type="button"
                        className="inline-flex items-center gap-1.5 m-0 p-0 bg-transparent border-none text-[13px] font-semibold text-gray-900 cursor-pointer hover:opacity-80"
                        onClick={() => toggleGroup(group.groupName)}
                      >
                        {isCollapsed ? <ChevronRight size={14} className="text-gray-400" /> : <ChevronDown size={14} className="text-gray-400" />}
                        {group.groupName}
                      </button>
                      <span className="text-xs text-gray-400">{engineLabel(group)} · {topologyLabel(group, master)}</span>
                      <span className="flex-1" />
                      <span className={`font-mono tabular-nums text-xs ${running === total ? 'text-gray-500' : 'text-amber-600'}`}>{running}/{total}</span>
                      <StatusBadge status={group.status} />
                    </div>
                  </td>
                </tr>
                {!isCollapsed && ordered.map((inst) => {
                  const status = getField(inst, 'status', 'Status');
                  const tone = instanceStatusTone(status);
                  const role = getField(inst, 'role', 'Role');
                  const isMaster = role === 'master' || role === 'standalone';
                  const used = Number(getField(inst, 'memory_used', 'MemoryUsed', 0) || 0);
                  const totalGi = parseGi(getField(inst, 'memory', 'Memory'));
                  const rowTint = tone === 'bad' ? 'bg-red-50/40' : tone === 'warn' ? 'bg-amber-50/30' : '';
                  return (
                    <tr
                      key={inst.name}
                      className={`border-b border-gray-100 last:border-b-0 cursor-pointer hover:bg-gray-50 ${rowTint}`}
                      onClick={() => onOpenDetail(inst, group)}
                    >
                      <td className="px-4 py-2 text-[13px]">
                        <div className="flex items-center gap-2.5">
                          <StatusDot status={status} />
                          {!isMaster && <span className="w-3.5 h-3.5 border-l border-b border-gray-300 rounded-bl shrink-0 -translate-y-0.5 ml-1" />}
                          <span className="font-medium">{inst.name}</span>
                        </div>
                      </td>
                      <td className="px-4 py-2 text-[13px]">
                        <span className={`inline-flex items-center rounded px-1.5 py-0.5 text-xs font-medium ${isMaster ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-500'}`}>
                          {role === 'standalone' ? '单点' : isMaster ? '主库' : '从库'}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-[13px] font-mono text-xs text-gray-500 whitespace-nowrap">
                        {getField(inst, 'server', 'Server', '-')}:{getField(inst, 'port', 'Port', '-')}
                      </td>
                      <td className="px-4 py-2 text-[13px]">
                        <ResourceValue memory={totalGi} memoryUsed={used} />
                      </td>
                      <td className="px-4 py-2 text-[13px]">
                        <StatusBadge status={status} />
                      </td>
                    </tr>
                  );
                })}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function DashboardView({ groups, servers, audits, sentinel, publicHost, onOpenDetail }) {
  const allInstances = groups.flatMap((g) => g.instances);
  const totalInstances = allInstances.length;
  const runningInstances = allInstances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
  const failedInstances = allInstances.filter((inst) => instanceStatusTone(getField(inst, 'status', 'Status')) === 'bad').length;

  const totalGroups = groups.length;
  const healthyGroups = groups.filter((g) => g.status === 'running' || g.status === 'healthy').length;

  const totalServers = servers.length;
  const healthyServers = servers.filter((s) => {
    const st = getField(s, 'status', 'Status');
    return st === 'healthy' || st === 'drain';
  }).length;

  const instanceTone = totalInstances === 0 ? 'neutral' : failedInstances > 0 ? 'alert' : runningInstances < totalInstances ? 'warn' : 'neutral';
  const groupTone = totalGroups === 0 ? 'neutral' : healthyGroups < totalGroups ? 'warn' : 'neutral';
  const serverTone = totalServers === 0 ? 'neutral' : healthyServers < totalServers ? 'warn' : 'neutral';

  const instancesPerServer = {};
  for (const inst of allInstances) {
    const s = getField(inst, 'server', 'Server', '-');
    instancesPerServer[s] = (instancesPerServer[s] || 0) + 1;
  }

  return (
    <div className="grid gap-4">
      {/* Summary strip */}
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1">
        <SummaryCell label="实例状态" value={totalInstances ? `${runningInstances}/${totalInstances}` : '0'} icon={Database} tone={instanceTone} />
        <SummaryCell label="拓扑健康" value={totalGroups ? `${healthyGroups}/${totalGroups}` : '0'} icon={GitMerge} tone={groupTone} />
        <SummaryCell label="服务器" value={totalServers ? `${healthyServers}/${totalServers}` : '0'} icon={Server} tone={serverTone} />
        {sentinel && sentinel.enabled && (() => {
          const onlineNodes = sentinel.nodes ? sentinel.nodes.filter((n) => n.status?.running).length : 0;
          const totalNodes = sentinel.nodes?.length || 0;
          const sentinelTone = totalNodes === 0 ? 'neutral' : onlineNodes < sentinel.quorum ? 'alert' : onlineNodes < totalNodes ? 'warn' : 'neutral';
          return <SummaryCell label="Sentinel" value={`${onlineNodes}/${totalNodes}`} icon={ShieldCheck} tone={sentinelTone} />;
        })()}
      </section>

      <div className="grid grid-cols-[minmax(0,1.8fr)_minmax(300px,0.8fr)] gap-4 max-lg:grid-cols-1">
        {/* Instance groups table */}
        <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
          <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
            <h3 className="m-0 text-[15px] font-semibold">实例运行状态</h3>
            <Database size={18} className="text-gray-400" />
          </div>
          <DashboardInstanceTable groups={groups} onOpenDetail={onOpenDetail} />
        </section>

        {/* Right column */}
        <div className="grid gap-4">
          {/* Server status */}
          <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
            <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
              <h3 className="m-0 text-[15px] font-semibold">服务器状态</h3>
              <Server size={18} className="text-gray-400" />
            </div>
            <div>
              {servers.length === 0 ? <EmptyState icon={Server} title="暂无服务器" /> :
                servers.map((s) => (
                  <div key={s.name} className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0">
                    <StatusBadge status={getField(s, 'status', 'Status')} />
                    <div className="flex flex-col gap-0.5 flex-1 min-w-0">
                      <strong className="text-[13px] font-medium">{s.name}</strong>
                      <code className="text-gray-400 font-mono text-xs">{getField(s, 'endpoint', 'Endpoint', '-')}</code>
                    </div>
                    <span className="text-gray-400 text-xs">{instancesPerServer[s.name] || 0} 个实例</span>
                  </div>
                ))
              }
            </div>
          </section>

          {/* Sentinel status */}
          {sentinel && sentinel.enabled && (
            <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
              <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <h3 className="m-0 text-[15px] font-semibold">Sentinel 集群</h3>
                <ShieldCheck size={18} className="text-gray-400" />
              </div>
              <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
                <span className="text-[13px] text-gray-500">节点状态</span>
                <div className="flex items-center gap-3">
                  <span className="text-[13px] font-mono font-medium">
                    在线 {sentinel.nodes ? sentinel.nodes.filter((n) => n.status?.running).length : 0}/{sentinel.nodes?.length || 0}
                  </span>
                  <span className="text-[12px] text-gray-400">quorum={sentinel.quorum}</span>
                </div>
              </div>
              {(sentinel.nodes || []).map((node) => {
                const isOnline = node.status?.running;
                const masters = node.status?.masters || [];
                return (
                  <div key={node.name} className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0">
                    <span className={`inline-block w-2 h-2 rounded-full shrink-0 ${isOnline ? 'bg-emerald-500' : 'bg-red-500'}`} />
                    <div className="flex flex-col gap-0.5 flex-1 min-w-0">
                      <strong className="text-[13px] font-medium">{node.name}</strong>
                      {isOnline ? (
                        <span className="text-gray-400 text-xs">监控 {masters.length} 个 master</span>
                      ) : (
                        <span className="text-red-500 text-xs">{node.error || '离线'}</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </section>
          )}

          {/* Recent audit */}
          <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
            <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
              <h3 className="m-0 text-[15px] font-semibold">最近操作</h3>
              <Clock size={18} className="text-gray-400" />
            </div>
            <div>
              {(audits || []).slice(0, 6).map((log, index) => (
                <AuditCard key={log.id || log.ID || index} log={log} />
              ))}
              {(!audits || audits.length === 0) && <EmptyState icon={Clock} title="暂无操作记录" />}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
