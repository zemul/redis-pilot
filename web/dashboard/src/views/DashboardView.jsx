// 大盘看板视图：平台汇总 + 实例组态势 + 基础设施/最近操作。
import { ChevronRight, Database, Server, ShieldCheck, ShieldAlert, Clock, GitMerge, MemoryStick } from 'lucide-react';
import { EmptyState, EngineIcon, MemoryUsage, StatusBadge, SummaryCell } from '../components/common.jsx';
import { formatBytes, formatDuration, getField } from '../lib/format.js';
import { engineLabel, topologyLabel, instanceStatusTone, topologyReason } from '../lib/instance.js';
import { AuditCard } from './AuditView.jsx';

// 大盘只展示实例组态势；节点级明细留在实例管理页和详情弹窗中。
function DashboardInstanceOverview({ groups, onOpenDetail }) {
  if (groups.length === 0) {
    return <div className="p-4"><EmptyState title="暂无实例组" detail="创建实例后会在这里展示运行状态。" /></div>;
  }

  const weight = (status) => {
    const tone = instanceStatusTone(status);
    return tone === 'bad' ? 0 : tone === 'warn' ? 1 : 2;
  };
  const orderedGroups = [...groups].sort((a, b) => weight(a.status) - weight(b.status) || a.groupName.localeCompare(b.groupName));

  return (
    <div className="overflow-x-auto">
      <div className="min-w-[760px]">
        <div className="grid grid-cols-[minmax(190px,1.3fr)_150px_100px_120px_108px] gap-4 px-4 py-2.5 border-b border-gray-200 bg-gray-50 text-xs font-medium text-gray-400">
          <span>实例组</span>
          <span>接入 / 拓扑</span>
          <span>运行节点</span>
          <span>内存使用</span>
          <span>状态</span>
        </div>
        {orderedGroups.map((group) => {
          const running = group.instances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
          const total = group.instances.length;
          const tone = instanceStatusTone(group.status);
          const isRwSplit = group.access_mode === 'rw-split';
          const reason = topologyReason(group);
          const rowTint = tone === 'bad' ? 'bg-red-50/40' : tone === 'warn' ? 'bg-amber-50/30' : 'bg-white';
          return (
            <button
              key={group.groupName}
              type="button"
              disabled={!group.master}
              aria-label={`查看实例组 ${group.groupName}`}
              className={`w-full grid grid-cols-[minmax(190px,1.3fr)_150px_100px_120px_108px] items-center gap-4 px-4 py-3 border-0 border-b border-gray-100 last:border-b-0 text-left transition-colors enabled:cursor-pointer enabled:hover:bg-blue-50/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-blue-200 disabled:cursor-default ${rowTint}`}
              onClick={() => group.master && onOpenDetail(group.master, group)}
            >
              <span className="flex items-center gap-2.5 min-w-0">
                <EngineIcon engine={group.engine} size={30} />
                <span className="min-w-0">
                  <strong className="block text-[13px] font-semibold text-gray-900 truncate">{group.groupName}</strong>
                  <span className="block mt-0.5 text-[11px] text-gray-400 truncate">{engineLabel(group)}</span>
                </span>
              </span>
              <span className="min-w-0">
                <span className={`inline-flex rounded-md border px-1.5 py-0.5 text-[11px] font-medium ${isRwSplit ? 'border-blue-100 bg-blue-50 text-blue-700' : 'border-slate-300 bg-white text-slate-600'}`}>
                  {isRwSplit ? '读写分离' : '标准'}
                </span>
                <span className="block mt-1 text-[11px] text-gray-400">{topologyLabel(group, group.master)}</span>
              </span>
              <span className={`font-mono tabular-nums text-[13px] font-medium ${running === total ? 'text-gray-700' : 'text-amber-600'}`}>
                {running}<span className="text-gray-300 mx-1">/</span>{total}
              </span>
              <MemoryUsage memory={group.resource.memory} memoryUsed={group.resource.memoryUsed} />
              <span className="flex items-center justify-between gap-2 min-w-0">
                <span className="min-w-0">
                  <StatusBadge status={group.status} title={reason?.detail} />
                  {reason && (
                    <span
                      className={`block mt-0.5 text-[11px] truncate ${tone === 'bad' ? 'text-red-500' : 'text-amber-600'}`}
                      title={reason.detail}
                    >
                      {reason.title}
                    </span>
                  )}
                </span>
                <ChevronRight size={15} className="text-gray-300 shrink-0" />
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// 当前异常卡片：只展示未恢复(open)异常，严重优先、持续时间倒序，最多 5 条。
// 数据来自 /incidents/summary，不在前端自行聚合全量列表。
const INCIDENT_TYPE_LABELS = {
  instance_unexpected_down: '实例意外停止',
  node_agent_unreachable: '节点 Agent 不可达',
  master_mismatch: 'Master 视角不一致',
  failover_conflict: '故障转移冲突',
  no_running_replica: '无可用从节点',
  backup_task_failed: '备份失败',
  restore_task_failed: '恢复失败',
};

function CurrentIncidentsCard({ summary, onOpenIncidents }) {
  const total = summary?.total || 0;
  const top = summary?.top || [];
  return (
    <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
      <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
        <h3 className="m-0 text-[15px] font-semibold">当前异常</h3>
        <span className={`inline-flex items-center gap-1.5 text-[13px] font-semibold ${total > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
          <ShieldAlert size={18} className={total > 0 ? 'text-red-500' : 'text-emerald-500'} />
          {total}
        </span>
      </div>
      {total === 0 ? (
        <div className="px-4 py-3 text-[13px] text-gray-400">当前无异常</div>
      ) : (
        <div>
          {top.map((inc) => {
            const critical = inc.severity === 'critical';
            return (
              <button
                key={inc.id}
                type="button"
                onClick={() => onOpenIncidents && onOpenIncidents(inc.instance_name || '')}
                className="w-full flex items-center gap-3 px-4 py-3 border-0 border-b border-gray-100 last:border-b-0 text-left bg-white hover:bg-gray-50 cursor-pointer transition-colors"
              >
                <span className={`w-2 h-2 rounded-full shrink-0 ${critical ? 'bg-red-500' : 'bg-amber-500'}`} />
                <span className="flex-1 min-w-0">
                  <strong className="block text-[13px] font-medium truncate">{inc.object_id} · {INCIDENT_TYPE_LABELS[inc.incident_type] || inc.incident_type}</strong>
                  <span className={`block mt-0.5 text-[11px] ${critical ? 'text-red-500' : 'text-amber-600'}`}>{critical ? '严重' : '警告'}</span>
                </span>
                <span className="text-[12px] text-gray-400 tabular-nums shrink-0">{inc.duration_ms > 0 ? formatDuration(Math.round(inc.duration_ms / 1000)) : '-'}</span>
                <ChevronRight size={15} className="text-gray-300 shrink-0" />
              </button>
            );
          })}
          <button
            type="button"
            onClick={() => onOpenIncidents && onOpenIncidents('')}
            className="w-full px-4 py-2.5 border-0 border-t border-gray-100 text-right text-[12px] font-medium text-gray-500 bg-white hover:bg-gray-50 hover:text-gray-900 cursor-pointer"
          >
            查看全部
          </button>
        </div>
      )}
    </section>
  );
}

export function DashboardView({ groups, servers, audits, sentinel, incidentSummary, publicHost, onOpenDetail, onOpenIncidents }) {
  const allInstances = groups.flatMap((g) => g.instances);
  const totalInstances = allInstances.length;
  const runningInstances = allInstances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
  const failedInstances = allInstances.filter((inst) => instanceStatusTone(getField(inst, 'status', 'Status')) === 'bad').length;
  const warningInstances = allInstances.filter((inst) => instanceStatusTone(getField(inst, 'status', 'Status')) === 'warn').length;

  const totalGroups = groups.length;
  const healthyGroups = groups.filter((g) => g.status === 'running' || g.status === 'healthy').length;
  const replicatedGroups = groups.filter((g) => g.replicas.length > 0).length;

  const totalServers = servers.length;
  const healthyServers = servers.filter((s) => {
    const st = getField(s, 'status', 'Status');
    return st === 'healthy' || st === 'drain';
  }).length;

  const instanceTone = totalInstances === 0 ? 'neutral' : failedInstances > 0 ? 'alert' : runningInstances < totalInstances ? 'warn' : 'neutral';
  const groupTone = totalGroups === 0 ? 'neutral' : healthyGroups < totalGroups ? 'warn' : 'neutral';
  const serverTone = totalServers === 0 ? 'neutral' : healthyServers < totalServers ? 'warn' : 'neutral';
  const totalMemoryGi = groups.reduce((sum, group) => sum + group.resource.memory, 0);
  const totalMemoryBytes = totalMemoryGi * 1024 * 1024 * 1024;
  const usedMemoryBytes = groups.reduce((sum, group) => sum + group.resource.memoryUsed, 0);
  const memoryPct = totalMemoryBytes > 0 ? (usedMemoryBytes / totalMemoryBytes) * 100 : 0;
  const memoryPctLabel = memoryPct >= 10 ? memoryPct.toFixed(0) : memoryPct > 0 ? memoryPct.toFixed(2) : '0';
  const memoryTone = memoryPct > 80 ? 'alert' : memoryPct > 60 ? 'warn' : 'neutral';

  const sentinelNodes = sentinel?.nodes || [];
  const onlineSentinels = sentinelNodes.filter((node) => node.status?.running).length;
  const sentinelHealthy = !sentinel?.enabled || (sentinelNodes.length > 0 && onlineSentinels >= sentinel.quorum);
  const unhealthySentinels = sentinelNodes.filter((node) => !node.status?.running);

  const instancesPerServer = {};
  for (const inst of allInstances) {
    const s = getField(inst, 'server', 'Server', '-');
    instancesPerServer[s] = (instancesPerServer[s] || 0) + 1;
  }

  return (
    <div className="grid gap-4">
      {/* Summary strip */}
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1">
        <SummaryCell label="实例组" value={totalGroups} icon={GitMerge} tone={groupTone} detail={`${replicatedGroups} 个主从 · ${totalGroups - replicatedGroups} 个单点`} />
        <SummaryCell label="运行实例" value={totalInstances ? `${runningInstances}/${totalInstances}` : '0'} icon={Database} tone={instanceTone} detail={failedInstances > 0 ? `${failedInstances} 个异常` : warningInstances > 0 ? `${warningInstances} 个状态变化` : '全部运行正常'} />
        <SummaryCell label="内存使用" value={`${memoryPctLabel}%`} icon={MemoryStick} tone={memoryTone} detail={`${formatBytes(usedMemoryBytes)} / ${formatBytes(totalMemoryBytes)}`} />
        <SummaryCell label="服务器节点" value={totalServers ? `${healthyServers}/${totalServers}` : '0'} icon={Server} tone={serverTone} detail={`承载 ${totalInstances} 个实例`} />
      </section>

      <div className="grid grid-cols-[minmax(0,1.65fr)_minmax(300px,0.75fr)] items-start gap-4 max-lg:grid-cols-1">
        {/* Instance group overview */}
        <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
          <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
            <div>
              <h3 className="m-0 text-[15px] font-semibold">实例运行状况</h3>
            </div>
            <span className={`inline-flex items-center gap-1.5 text-[12px] font-medium ${failedInstances > 0 ? 'text-red-600' : warningInstances > 0 ? 'text-amber-600' : 'text-emerald-600'}`}>
              <span className={`w-2 h-2 rounded-full ${failedInstances > 0 ? 'bg-red-500' : warningInstances > 0 ? 'bg-amber-500' : 'bg-emerald-500'}`} />
              {failedInstances > 0 ? `${failedInstances} 个异常` : warningInstances > 0 ? `${warningInstances} 个状态变化` : '全部正常'}
            </span>
          </div>
          <DashboardInstanceOverview groups={groups} onOpenDetail={onOpenDetail} />
        </section>

        {/* Right column */}
        <div className="grid gap-4">
          {/* Redis/Agent server nodes */}
          <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
            <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
              <h3 className="m-0 text-[15px] font-semibold">服务器节点</h3>
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

          {/* Sentinel is an independent cluster and may run on different machines. */}
          {sentinel && sentinel.enabled && (
            <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
              <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <h3 className="m-0 text-[15px] font-semibold">Sentinel 集群</h3>
                <ShieldCheck size={18} className={sentinelHealthy ? 'text-emerald-500' : 'text-red-500'} />
              </div>
              <div className="flex items-center gap-3 px-4 py-3">
                <span className={`w-2 h-2 rounded-full shrink-0 ${sentinelHealthy ? 'bg-emerald-500' : 'bg-red-500'}`} />
                <div className="flex-1 min-w-0">
                  <strong className="block text-[13px] font-medium">节点在线 {onlineSentinels}/{sentinelNodes.length}</strong>
                  <span className="text-[11px] text-gray-400">选举仲裁数 quorum={sentinel.quorum}</span>
                </div>
                <span className={`text-[12px] font-medium ${sentinelHealthy ? 'text-gray-500' : 'text-red-600'}`}>{sentinelHealthy ? '运行正常' : '需要处理'}</span>
              </div>
              {unhealthySentinels.map((node) => (
                <div key={node.name} className="flex items-center gap-2 px-4 py-2.5 border-t border-red-100 bg-red-50/40 text-[12px]">
                  <span className="w-2 h-2 rounded-full bg-red-500" />
                  <span className="font-medium text-red-700">{node.name}</span>
                  <span className="text-red-500 truncate">{node.error || '离线'}</span>
                </div>
              ))}
            </section>
          )}

          {/* Current incidents */}
          <CurrentIncidentsCard summary={incidentSummary} onOpenIncidents={onOpenIncidents} />

          {/* Recent audit */}
          <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
            <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
              <h3 className="m-0 text-[15px] font-semibold">最近操作</h3>
              <Clock size={18} className="text-gray-400" />
            </div>
            <div>
              {(audits || []).slice(0, 4).map((log, index) => (
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
