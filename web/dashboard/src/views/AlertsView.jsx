// 异常总览视图：汇总实例异常、拓扑降级、节点异常三类告警。
import { AlertCircle, GitMerge, Server, ShieldCheck } from 'lucide-react';
import { SummaryCell, EmptyState, StatusBadge } from '../components/common.jsx';
import { getField } from '../lib/format.js';
import { instanceStatusTone, topologyLabel } from '../lib/instance.js';

export function AlertsView({ groups, servers, publicHost }) {
  const badTone = (status) => instanceStatusTone(status) === 'bad';

  const instanceAlerts = [];
  for (const group of groups) {
    for (const inst of group.instances) {
      const status = getField(inst, 'status', 'Status');
      if (badTone(status) || status === 'unreachable') {
        instanceAlerts.push({
          instance: inst.name,
          group: group.groupName,
          server: getField(inst, 'server', 'Server', '-'),
          status,
          desired: getField(inst, 'desired_state', 'DesiredState', '-'),
          observed: getField(inst, 'observed_status', 'ObservedStatus', '-'),
        });
      }
    }
  }

  const groupAlerts = groups.filter(
    (g) => g.status === 'degraded' || g.status === 'failover_conflict'
  );

  const serverAlerts = servers.filter(
    (s) => getField(s, 'status', 'Status') === 'unhealthy'
  );

  const total = instanceAlerts.length + groupAlerts.length + serverAlerts.length;

  return (
    <div className="grid gap-4">
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden grid grid-cols-4 max-lg:grid-cols-2 max-md:grid-cols-1">
        <SummaryCell label="实例异常" value={instanceAlerts.length} icon={AlertCircle} tone={instanceAlerts.length > 0 ? 'alert' : 'neutral'} />
        <SummaryCell label="拓扑降级" value={groupAlerts.length} icon={GitMerge} tone={groupAlerts.length > 0 ? 'alert' : 'neutral'} />
        <SummaryCell label="节点异常" value={serverAlerts.length} icon={Server} tone={serverAlerts.length > 0 ? 'alert' : 'neutral'} />
      </section>

      {total === 0 ? (
        <section className="border border-gray-200 rounded-lg bg-white">
          <EmptyState icon={ShieldCheck} title="一切正常" detail="当前没有检测到异常。" />
        </section>
      ) : (
        <>
          {instanceAlerts.length > 0 && (
            <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
              <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <h3 className="m-0 text-[15px] font-semibold">实例异常</h3>
                <AlertCircle size={18} className="text-gray-400" />
              </div>
              <div className="overflow-x-auto">
                <table className="w-full min-w-[920px] border-collapse">
                  <thead>
                    <tr>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例</th>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例组</th>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">节点</th>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">状态</th>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">期望</th>
                      <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">观测</th>
                    </tr>
                  </thead>
                  <tbody>
                    {instanceAlerts.map((a, i) => (
                      <tr key={i} className="border-b border-gray-200 last:border-b-0">
                        <td className="px-4 py-3 text-[13px] font-mono">{a.instance}</td>
                        <td className="px-4 py-3 text-[13px]">{a.group}</td>
                        <td className="px-4 py-3 text-[13px]">{a.server}</td>
                        <td className="px-4 py-3 text-[13px]"><StatusBadge status={a.status} /></td>
                        <td className="px-4 py-3 text-[13px] text-gray-400">{a.desired}</td>
                        <td className="px-4 py-3 text-[13px] text-gray-400">{a.observed}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {groupAlerts.length > 0 && (
            <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
              <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <h3 className="m-0 text-[15px] font-semibold">拓扑异常</h3>
                <GitMerge size={18} className="text-gray-400" />
              </div>
              <div>
                {groupAlerts.map((g) => (
                  <div key={g.groupName} className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0">
                    <StatusBadge status={g.status} />
                    <div className="flex flex-col gap-0.5">
                      <strong className="text-[13px] font-medium">{g.groupName}</strong>
                      <span className="text-xs text-gray-400">{topologyLabel(g)}</span>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {serverAlerts.length > 0 && (
            <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
              <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <h3 className="m-0 text-[15px] font-semibold">节点异常</h3>
                <Server size={18} className="text-gray-400" />
              </div>
              <div>
                {serverAlerts.map((s) => (
                  <div key={s.name} className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0">
                    <StatusBadge status={getField(s, 'status', 'Status')} />
                    <div className="flex flex-col gap-0.5">
                      <strong className="text-[13px] font-medium">{s.name}</strong>
                      <code className="text-xs text-gray-400 font-mono">{getField(s, 'endpoint', 'Endpoint', '-')}</code>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}
