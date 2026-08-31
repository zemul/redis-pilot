// 实例管理视图：搜索栏 + 实例组卡片列表；组卡片可展开显示主/从节点行。
import { Search, Plus, ChevronDown, ChevronRight } from 'lucide-react';
import {
  EmptyState,
  CategoryBadge,
  EngineIcon,
  MemoryUsage,
  StatusBadge,
  CopyableText,
  CopyableEndpoint,
  MoreMenu,
} from '../components/common.jsx';
import { getField, parseGi } from '../lib/format.js';
import { topologyLabel, engineLabel, powerActionFor } from '../lib/instance.js';

// 实例列表统一的网格列模板：实例组/节点 | 类型 | 接入/拓扑 | 架构/版本 | 节点/接入点 | 内存使用 | 状态 | 操作
const INST_GRID = 'grid items-center gap-4 grid-cols-[280px_84px_120px_130px_minmax(220px,1fr)_90px_92px_112px] px-4';

export function InstancesView({ groups, publicHost, sentinelPort, query, setQuery, expandedGroups, setExpandedGroups, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onResize, onToggleAccessMode, onEditLabels, onCreate, pendingAction }) {
  const filtered = groups.filter((group) => {
    const text = [
      group.groupName,
      group.engine,
      group.engineVersion,
      group.category,
      group.type,
      group.status,
      ...Object.entries(group.labels || {}).flatMap(([key, value]) => [key, value, `${key}=${value}`]),
      ...group.instances.flatMap((item) => [
        item.name,
        getField(item, 'server', 'Server'),
        getField(item, 'port', 'Port')
      ])
    ].join(' ').toLowerCase();
    return text.includes(query.trim().toLowerCase());
  });

  function toggleGroup(groupName) {
    setExpandedGroups((current) => ({ ...current, [groupName]: !current[groupName] }));
  }

  return (
    <div className="grid gap-4">
      <div className="border border-gray-200 rounded-lg bg-white min-h-[52px] px-4 py-2.5 flex items-center justify-between gap-3 max-md:flex-col max-md:items-stretch">
        <div className="min-w-[240px] w-[min(380px,100%)] h-[34px] border border-gray-200 rounded-md flex items-center gap-2 px-2.5 bg-white text-gray-400 focus-within:border-gray-900 focus-within:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all max-md:w-full max-md:min-w-0">
          <Search size={15} />
          <input className="w-full border-0 outline-0 bg-transparent text-[13px] text-gray-900 placeholder:text-gray-400" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索实例组、节点或标签..." />
        </div>
        <button className="h-[32px] rounded-md px-3 bg-gray-900 text-white font-medium text-[12px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer" onClick={onCreate}>
          <Plus size={14} /> 创建实例
        </button>
      </div>

      <div className="overflow-x-auto">
        <div className="min-w-[1220px] grid gap-2.5">
          <div className={`${INST_GRID} py-1 text-xs font-medium text-gray-400`}>
            <div>实例组 / 节点</div>
            <div>类型</div>
            <div>接入 / 拓扑</div>
            <div>架构 / 版本</div>
            <div>节点 / 接入点</div>
            <div>内存使用</div>
            <div>状态</div>
            <div className="text-right">操作</div>
          </div>

          {filtered.length === 0 ? (
            <div className="border border-gray-200 rounded-xl bg-white"><EmptyState title="暂无实例" /></div>
          ) : filtered.map((group) => (
            <InstanceGroupCard
              key={group.groupName}
              group={group}
              expanded={expandedGroups[group.groupName] === true}
              onToggle={() => toggleGroup(group.groupName)}
              publicHost={publicHost}
              sentinelPort={sentinelPort}
              onOpenDetail={onOpenDetail}
              onOpenBackups={onOpenBackups}
              onPowerAction={onPowerAction}
              onDelete={onDelete}
              onSlowlog={onSlowlog}
              onBigkeys={onBigkeys}
              onConfig={onConfig}
              onResize={onResize}
              onToggleAccessMode={onToggleAccessMode}
              onEditLabels={onEditLabels}
              pendingAction={pendingAction}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function InstanceGroupCard({ group, expanded, onToggle, publicHost, sentinelPort, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onResize, onToggleAccessMode, onEditLabels, pendingAction }) {
  const master = group.master;
  const hasReplicas = group.replicas.length > 0;
  const topology = topologyLabel(group, master);
  const accessLabel = group.access_mode === 'rw-split' ? '读写分离' : '标准';
  const isRwSplit = group.access_mode === 'rw-split';
  const powerAction = master ? powerActionFor(master, group) : null;

  const menuItems = master ? [
    { label: '备份列表', onClick: () => onOpenBackups(master, group) },
    { label: '接入信息', onClick: () => onToggleAccessMode(group, null) },
    { label: '修改配置', onClick: () => onConfig(master) },
    { label: '调整规格', onClick: () => onResize(master) },
    { label: '编辑标签', onClick: () => onEditLabels(group) },
    { label: '慢查询', onClick: () => onSlowlog(master) },
    { label: '大 Key 分析', onClick: () => onBigkeys(master) },
    powerAction && {
      label: powerAction.label,
      disabled: powerAction.disabledReason || pendingAction === `${powerAction.action}:${master.name}`,
      onClick: () => { if (!powerAction.disabledReason) onPowerAction(master, group, powerAction.action); }
    },
    {
      label: '删除',
      danger: true,
      disabled: hasReplicas ? '主库存在从库时不可删除，请先删除从库' : pendingAction === `delete:${master.name}`,
      onClick: () => { if (!hasReplicas) onDelete(master, group); }
    }
  ] : [];

  return (
    <div className={`border rounded-xl bg-white transition-all ${expanded ? 'border-blue-200 shadow-sm' : 'border-gray-200 hover:border-gray-300 hover:shadow-sm'}`}>
      <div className={`${INST_GRID} py-3.5 ${hasReplicas ? 'cursor-pointer' : ''}`} onClick={hasReplicas ? onToggle : undefined}>
        <div className="flex items-center gap-2.5 min-w-0">
          {hasReplicas ? (
            <span className={`w-5 h-5 rounded inline-flex items-center justify-center shrink-0 transition-colors ${expanded ? 'text-blue-500 bg-blue-50' : 'text-gray-400'}`}>
              {expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
            </span>
          ) : (
            <span className="w-5 shrink-0" />
          )}
          <EngineIcon engine={group.engine} size={32} />
          <div className="min-w-0">
            <CopyableText text={group.groupName} className="font-semibold text-gray-900 text-[14px]" />
            {master ? (
              <span className="text-[11px] text-gray-400 block mt-0.5 truncate">主库: {master.name}</span>
            ) : (
              <span className="text-[11px] text-red-500 block mt-0.5">主库状态异常</span>
            )}
            {Object.keys(group.labels || {}).length > 0 && (
              <div className="flex flex-wrap gap-1 mt-1">
                {Object.entries(group.labels).sort(([a], [b]) => a.localeCompare(b)).slice(0, 3).map(([key, value]) => (
                  <span key={key} title={`${key}=${value}`} className="max-w-[88px] truncate rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 text-[10px] text-gray-500">{key}={value}</span>
                ))}
                {Object.keys(group.labels).length > 3 && <span className="text-[10px] text-gray-400">+{Object.keys(group.labels).length - 3}</span>}
              </div>
            )}
          </div>
        </div>
        <div><CategoryBadge category={group.category} /></div>
        <div className="text-[13px]">
          <span className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[12px] font-medium ${isRwSplit ? 'border-blue-100 bg-blue-50 text-blue-700' : 'border-slate-200 bg-slate-50 text-slate-700'}`}>
            {accessLabel}
          </span>
          <div className="text-[11px] text-gray-400 mt-0.5">{topology}</div>
        </div>
        <div className="text-[13px] text-gray-700 min-w-0">
          <span className="truncate block">{engineLabel(group)}</span>
        </div>
        <div className="text-[13px]"><ProxyPorts group={group} publicHost={publicHost} /></div>
        <div>{master ? <MemoryUsage memory={group.resource.memory} memoryUsed={group.resource.memoryUsed} /> : <span className="text-gray-300">-</span>}</div>
        <div><StatusBadge status={group.status} /></div>
        <div className="flex justify-end items-center gap-1.5" onClick={(e) => e.stopPropagation()}>
          {master && (
            <button type="button" className="h-7 px-3 rounded-md border border-gray-200 bg-white text-[12px] text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" onClick={() => onOpenDetail(master, group)}>查看</button>
          )}
          <MoreMenu items={menuItems} />
        </div>
      </div>

      {expanded && (
        <div className="relative border-t border-blue-100 rounded-b-xl overflow-hidden bg-slate-50/80">
          <span className="absolute left-[26px] top-0 bottom-6 w-px bg-blue-200/80 pointer-events-none" aria-hidden="true" />
          {master && (
            <InstanceNodeRow
              item={master}
              group={group}
              role="主库"
              first
              onOpenDetail={onOpenDetail}
              onOpenBackups={onOpenBackups}
              onPowerAction={onPowerAction}
              onDelete={onDelete}
              onSlowlog={onSlowlog}
              onBigkeys={onBigkeys}
              onConfig={onConfig}
              onResize={onResize}
              onToggleAccessMode={onToggleAccessMode}
              pendingAction={pendingAction}
            />
          )}
          {group.replicas.map((replica) => (
            <InstanceNodeRow
              key={replica.name}
              item={replica}
              group={group}
              role="从库"
              onOpenDetail={onOpenDetail}
              onOpenBackups={onOpenBackups}
              onPowerAction={onPowerAction}
              onDelete={onDelete}
              onSlowlog={onSlowlog}
              onBigkeys={onBigkeys}
              onConfig={onConfig}
              onResize={onResize}
              onToggleAccessMode={onToggleAccessMode}
              pendingAction={pendingAction}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function ProxyPorts({ group, publicHost }) {
  const proxy = getField(group, 'proxy', 'Proxy', {}) || {};
  const masterPort = proxy.master_port || proxy.MasterPort;
  const autoPort = proxy.auto_port || proxy.AutoPort;
  const isRwSplit = group.access_mode === 'rw-split';
  if (!masterPort && !autoPort) return <span className="text-gray-400">-</span>;
  // 只展示当前接入方式对应的端口
  const port = isRwSplit ? (autoPort || masterPort) : (masterPort || autoPort);
  return <CopyableEndpoint host={publicHost} port={port} className="text-gray-700 text-[13px]" />;
}

function InstanceNodeRow({ item, group, role, first, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onResize, onToggleAccessMode, pendingAction }) {
  const powerAction = powerActionFor(item, group);
  const isReplica = role === '从库';
  const isMasterWithReplicas = role === '主库' && group.replicas.length > 0;
  const server = getField(item, 'server', 'Server', '');
  const port = getField(item, 'port', 'Port', '');
  const cpus = Number(getField(item, 'cpus', 'CPUs', 0) || 0);
  const used = Number(getField(item, 'memory_used', 'MemoryUsed', 0) || 0);
  const totalGi = parseGi(getField(item, 'memory', 'Memory'));
  const specs = [
    cpus > 0 ? `${Number.isInteger(cpus) ? cpus : cpus.toFixed(1)}C` : '',
    totalGi > 0 ? `${Number.isInteger(totalGi) ? totalGi : totalGi.toFixed(1)}G` : '',
  ].filter(Boolean).join(' · ');

  const menuItems = [
    { label: '备份列表', onClick: () => onOpenBackups(item, group) },
    !isReplica && { label: '接入信息', onClick: () => onToggleAccessMode(group, null) },
    { label: '修改配置', onClick: () => onConfig(item) },
    { label: '调整规格', onClick: () => onResize(item) },
    { label: '慢查询', onClick: () => onSlowlog(item) },
    { label: '大 Key 分析', onClick: () => onBigkeys(item) },
    powerAction && {
      label: powerAction.label,
      disabled: powerAction.disabledReason || pendingAction === `${powerAction.action}:${item.name}`,
      onClick: () => { if (!powerAction.disabledReason) onPowerAction(item, group, powerAction.action); }
    },
    {
      label: '删除',
      danger: true,
      disabled: isMasterWithReplicas ? '主库存在从库时不可删除，请先删除从库' : pendingAction === `delete:${item.name}`,
      onClick: () => { if (!isMasterWithReplicas) onDelete(item, group); }
    }
  ];

  return (
    <div className={`${INST_GRID} py-3 bg-transparent hover:bg-blue-50/60 transition-colors ${first ? '' : 'border-t border-slate-200/70'}`}>
      <div className="relative flex items-center gap-2.5 min-w-0 pl-[30px]">
        <span className="absolute left-[10px] top-1/2 w-4 h-px bg-blue-200/80 pointer-events-none" aria-hidden="true" />
        <span className={`text-[11px] font-medium px-1.5 py-0.5 rounded border shrink-0 ${isReplica ? 'bg-slate-100 text-slate-600 border-slate-200' : 'bg-blue-50 text-blue-700 border-blue-100'}`}>{role}</span>
        <div className="min-w-0">
          <CopyableText text={item.name} className="font-medium text-gray-800 text-[13px]" />
          {specs && <span className="block mt-0.5 text-[11px] text-slate-400 tabular-nums">{specs}</span>}
        </div>
      </div>
      <div />
      <div />
      <div />
      <div className="text-[13px]">
        {server ? <CopyableEndpoint host={server} port={port} className="text-gray-600" /> : <span className="text-gray-300">-</span>}
      </div>
      <div><MemoryUsage memory={totalGi} memoryUsed={used} /></div>
      <div><StatusBadge status={getField(item, 'status', 'Status')} /></div>
      <div className="flex justify-end items-center gap-1.5">
        <button type="button" className="h-7 px-3 rounded-md border border-gray-200 bg-white text-[12px] text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" onClick={() => onOpenDetail(item, group)}>查看</button>
        <MoreMenu items={menuItems} />
      </div>
    </div>
  );
}
