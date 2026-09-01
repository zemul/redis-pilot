// 实例管理视图：搜索栏 + 实例组卡片列表。
// 作用域分层（见 docs/architecture/instance-management-ui.md）：
//   standalone 组 → 单行、平铺菜单（组即节点，不分区）；
//   replication 组 → 父行只放组级操作，可展开显示主/从节点行，节点行只放高频运维项。
// 主按钮统一为「详情」，层级靠角色徽标区分。
import { useMemo, useState } from 'react';
import { Search, Plus, ChevronDown, ChevronRight, X } from 'lucide-react';
import {
  EmptyState,
  CategoryBadge,
  EngineIcon,
  MemoryUsage,
  StatusBadge,
  CopyableText,
  CopyableEndpoint,
  MoreMenu,
  FilterSelect,
} from '../components/common.jsx';
import { getField, parseGi, statusMeta } from '../lib/format.js';
import {
  topologyLabel,
  engineLabel,
  categoryLabel,
  isStandaloneGroup,
  canExpandGroup,
  groupActionsFor,
  nodeRowActionsFor,
  standaloneActionsFor,
} from '../lib/instance.js';

// 实例列表统一的网格列模板：实例组/节点 | 类型 | 接入 | 拓扑 | 架构/版本 | 标签 | 节点/接入点 | 内存使用 | 状态 | 操作
const INST_GRID = 'grid items-center gap-4 grid-cols-[280px_84px_100px_110px_130px_190px_minmax(220px,1fr)_90px_92px_112px] px-4';

export function InstancesView({ groups, publicHost, sentinelPort, query, setQuery, expandedGroups, setExpandedGroups, callbacks, onCreate, pendingAction }) {
  // 轻量筛选：引擎 / 类别 / 状态（单选）+ 标签（多选，AND）。纯前端过滤，数据已全量在手。
  const [engineFilter, setEngineFilter] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [labelFilter, setLabelFilter] = useState([]);

  // 从当前数据派生可选项，保证下拉里只出现真实存在的值。
  const { engineOptions, categoryOptions, statusOptions, labelOptions } = useMemo(() => {
    const engines = new Set();
    const categories = new Set();
    const statuses = new Set();
    const labels = new Set();
    for (const group of groups) {
      if (group.engine) engines.add(group.engine);
      if (group.category) categories.add(group.category);
      if (group.status) statuses.add(group.status);
      for (const label of group.labels || []) labels.add(label);
    }
    return {
      engineOptions: [...engines].sort(),
      categoryOptions: [...categories].sort(),
      statusOptions: [...statuses].sort(),
      labelOptions: [...labels].sort((a, b) => a.localeCompare(b)),
    };
  }, [groups]);

  function toggleLabelFilter(label) {
    setLabelFilter((current) =>
      current.includes(label) ? current.filter((item) => item !== label) : [...current, label]
    );
  }

  function clearFilters() {
    setEngineFilter('');
    setCategoryFilter('');
    setStatusFilter('');
    setLabelFilter([]);
  }

  const hasActiveFilters = engineFilter || categoryFilter || statusFilter || labelFilter.length > 0;

  const filtered = groups.filter((group) => {
    if (engineFilter && group.engine !== engineFilter) return false;
    if (categoryFilter && group.category !== categoryFilter) return false;
    if (statusFilter && group.status !== statusFilter) return false;
    // 标签多选按 AND：组必须包含所有选中的标签。
    if (labelFilter.length > 0) {
      const groupLabels = new Set(group.labels || []);
      if (!labelFilter.every((label) => groupLabels.has(label))) return false;
    }
    const text = [
      group.groupName,
      group.engine,
      group.engineVersion,
      group.category,
      group.type,
      group.status,
      ...(group.labels || []),
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
      <div className="border border-gray-200 rounded-lg bg-white min-h-[52px] px-4 py-2.5 flex flex-col gap-2.5">
        <div className="flex items-center justify-between gap-3 max-md:flex-col max-md:items-stretch">
          <div className="min-w-[240px] w-[min(380px,100%)] h-[34px] border border-gray-200 rounded-md flex items-center gap-2 px-2.5 bg-white text-gray-400 focus-within:border-gray-900 focus-within:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all max-md:w-full max-md:min-w-0">
            <Search size={15} />
            <input className="w-full border-0 outline-0 bg-transparent text-[13px] text-gray-900 placeholder:text-gray-400" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索实例组、节点或标签..." />
          </div>
          <button className="h-[32px] rounded-md px-3 bg-gray-900 text-white font-medium text-[12px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer shrink-0" onClick={onCreate}>
            <Plus size={14} /> 创建实例
          </button>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <FilterSelect className="w-[130px]" ariaLabel="按引擎筛选" value={engineFilter} onChange={setEngineFilter} placeholder="全部引擎"
            options={[{ value: '', label: '全部引擎' }, ...engineOptions.map((engine) => ({ value: engine, label: engineLabel({ engine, engineVersion: '' }) }))]} />
          <FilterSelect className="w-[120px]" ariaLabel="按类别筛选" value={categoryFilter} onChange={setCategoryFilter} placeholder="全部类别"
            options={[{ value: '', label: '全部类别' }, ...categoryOptions.map((category) => ({ value: category, label: categoryLabel(category) }))]} />
          <FilterSelect className="w-[130px]" ariaLabel="按状态筛选" value={statusFilter} onChange={setStatusFilter} placeholder="全部状态"
            options={[{ value: '', label: '全部状态' }, ...statusOptions.map((status) => ({ value: status, label: statusMeta(status).label }))]} />

          {labelOptions.length > 0 && (
            <div className="flex items-center gap-1.5 flex-wrap">
              <span className="text-[11px] text-gray-400">标签:</span>
              {labelOptions.map((label) => {
                const active = labelFilter.includes(label);
                return (
                  <button
                    key={label}
                    type="button"
                    title={label}
                    onClick={() => toggleLabelFilter(label)}
                    className={`max-w-[120px] truncate rounded-full border px-2 py-0.5 text-[11px] transition-colors cursor-pointer ${active ? 'border-blue-300 bg-blue-50 text-blue-700' : 'border-gray-200 bg-white text-gray-500 hover:border-gray-300'}`}
                    aria-pressed={active}
                  >{label}</button>
                );
              })}
            </div>
          )}

          {hasActiveFilters && (
            <button type="button" onClick={clearFilters} className="inline-flex items-center gap-1 text-[11px] text-gray-400 hover:text-gray-700 transition-colors border-0 bg-transparent cursor-pointer">
              <X size={12} /> 清除筛选
            </button>
          )}
        </div>
      </div>

      <div className="overflow-x-auto">
        <div className="min-w-[1490px] grid gap-2.5">
          <div className={`${INST_GRID} py-1 text-xs font-medium text-gray-400`}>
            <div>实例组 / 节点</div>
            <div>类型</div>
            <div>接入</div>
            <div>拓扑</div>
            <div>架构 / 版本</div>
            <div>标签</div>
            <div>节点 / 接入点</div>
            <div>内存使用</div>
            <div>状态</div>
            <div className="text-right">操作</div>
          </div>

          {filtered.length === 0 ? (
            <div className="border border-gray-200 rounded-xl bg-white"><EmptyState title={hasActiveFilters || query.trim() ? '没有符合筛选条件的实例' : '暂无实例'} /></div>
          ) : filtered.map((group) => (
            <InstanceGroupCard
              key={group.groupName}
              group={group}
              expanded={expandedGroups[group.groupName] === true}
              onToggle={() => toggleGroup(group.groupName)}
              publicHost={publicHost}
              sentinelPort={sentinelPort}
              callbacks={callbacks}
              pendingAction={pendingAction}
              activeLabels={labelFilter}
              onToggleLabelFilter={toggleLabelFilter}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function InstanceGroupCard({ group, expanded, onToggle, publicHost, sentinelPort, callbacks, pendingAction, activeLabels = [], onToggleLabelFilter }) {
  const master = group.master;
  const standalone = isStandaloneGroup(group);
  const expandable = canExpandGroup(group);
  const topology = topologyLabel(group, master);
  const accessLabel = group.access_mode === 'rw-split' ? '读写分离' : '标准';
  const isRwSplit = group.access_mode === 'rw-split';

  // standalone：平铺菜单（组+节点操作合并，不分区）；主按钮打开节点详情。
  // replication：父行只放组级操作；主按钮打开组详情。
  const menuItems = standalone
    ? standaloneActionsFor(group, callbacks, { pendingAction })
    : groupActionsFor(group, callbacks, { pendingAction });
  const ariaLabel = standalone
    ? `${group.groupName} 更多操作`
    : `实例组 ${group.groupName} 更多操作`;
  const openDetail = standalone
    ? () => master && callbacks.onOpenNodeDetail(master, group)
    : () => callbacks.onOpenGroupDetail(group);
  const detailAria = standalone ? `${group.groupName} 详情` : `实例组 ${group.groupName} 详情`;

  return (
    <div className={`border rounded-xl bg-white transition-all ${expanded ? 'border-blue-200 shadow-sm' : 'border-gray-200 hover:border-gray-300 hover:shadow-sm'}`}>
      <div className={`${INST_GRID} py-3.5 ${expandable ? 'cursor-pointer' : ''}`} onClick={expandable ? onToggle : undefined}>
        <div className="flex items-center gap-2.5 min-w-0">
          {expandable ? (
            <button
              type="button"
              className={`w-5 h-5 rounded inline-flex items-center justify-center shrink-0 transition-colors border-0 bg-transparent cursor-pointer ${expanded ? 'text-blue-500 bg-blue-50' : 'text-gray-400'}`}
              aria-expanded={expanded}
              aria-label={`${expanded ? '收起' : '展开'}实例组 ${group.groupName}`}
              onClick={(e) => { e.stopPropagation(); onToggle(); }}
            >
              {expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
            </button>
          ) : (
            <span className="w-5 shrink-0" />
          )}
          <EngineIcon engine={group.engine} size={32} />
          <div className="min-w-0">
            <CopyableText text={group.groupName} className="font-semibold text-gray-900 text-[14px]" />
            {standalone ? (
              <span className="text-[11px] text-gray-400 block mt-0.5 truncate">节点: {master ? master.name : '-'}</span>
            ) : master ? (
              <span className="text-[11px] text-gray-400 block mt-0.5 truncate">{topology}</span>
            ) : (
              <span className="text-[11px] text-red-500 block mt-0.5">主库状态异常</span>
            )}
          </div>
        </div>
        <div><CategoryBadge category={group.category} /></div>
        <div className="text-[13px]">
          <span className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[12px] font-medium ${isRwSplit ? 'border-blue-100 bg-blue-50 text-blue-700' : 'border-slate-200 bg-slate-50 text-slate-700'}`}>
            {accessLabel}
          </span>
        </div>
        <div className="text-[13px] text-gray-500">{topology}</div>
        <div className="text-[13px] text-gray-700 min-w-0">
          <span className="truncate block">{engineLabel(group)}</span>
        </div>
        <div className="min-w-0">
          {(group.labels || []).length > 0 ? (
            <div className="flex flex-wrap gap-1" onClick={(e) => e.stopPropagation()}>
              {[...group.labels].sort((a, b) => a.localeCompare(b)).slice(0, 3).map((label) => {
                const active = activeLabels.includes(label);
                return (
                  <button
                    key={label}
                    type="button"
                    title={onToggleLabelFilter ? `按标签「${label}」筛选` : label}
                    onClick={onToggleLabelFilter ? () => onToggleLabelFilter(label) : undefined}
                    className={`inline-flex items-center max-w-[96px] truncate rounded-md border px-2 py-0.5 text-[12px] font-medium transition-colors ${onToggleLabelFilter ? 'cursor-pointer' : 'cursor-default'} ${active ? 'border-blue-100 bg-blue-50 text-blue-700' : 'border-slate-200 bg-slate-50 text-slate-700 hover:border-slate-300'}`}
                  >{label}</button>
                );
              })}
              {group.labels.length > 3 && <span className="text-[12px] text-gray-400">+{group.labels.length - 3}</span>}
            </div>
          ) : (
            <span className="text-gray-300">-</span>
          )}
        </div>
        <div className="text-[13px]"><ProxyPorts group={group} publicHost={publicHost} /></div>
        <div>{master ? <MemoryUsage memory={group.resource.memory} memoryUsed={group.resource.memoryUsed} /> : <span className="text-gray-300">-</span>}</div>
        <div className="flex items-center gap-1.5">
          <StatusBadge status={group.status} />
          {group.incident && (
            <span
              title={`该组有 ${group.incident.count} 条未处理异常，详见异常记录`}
              className={`inline-flex items-center justify-center min-w-[16px] h-4 px-1 rounded-full text-[10px] font-semibold ${group.incident.severity === 'critical' ? 'bg-red-100 text-red-700' : 'bg-amber-100 text-amber-700'}`}
            >{group.incident.count}</span>
          )}
        </div>
        <div className="flex justify-end items-center gap-1.5" onClick={(e) => e.stopPropagation()}>
          <button type="button" className="h-7 px-3 rounded-md border border-gray-200 bg-white text-[12px] text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" aria-label={detailAria} onClick={openDetail}>详情</button>
          <MoreMenu items={menuItems} ariaLabel={ariaLabel} />
        </div>
      </div>

      {expanded && expandable && (
        <div className="relative border-t border-blue-100 rounded-b-xl overflow-hidden bg-slate-50/80">
          <span className="absolute left-[26px] top-0 bottom-6 w-px bg-blue-200/80 pointer-events-none" aria-hidden="true" />
          {master && (
            <InstanceNodeRow
              item={master}
              group={group}
              role="主库"
              first
              callbacks={callbacks}
              pendingAction={pendingAction}
            />
          )}
          {group.replicas.map((replica) => (
            <InstanceNodeRow
              key={replica.name}
              item={replica}
              group={group}
              role="从库"
              callbacks={callbacks}
              pendingAction={pendingAction}
            />
          ))}
          {group.replicas.length === 0 && (
            <div className={`${INST_GRID} py-3 ${master ? 'border-t border-slate-200/70' : ''}`}>
              <div className="pl-[30px] text-[12px] text-slate-400">当前没有从库</div>
              <div /><div /><div /><div /><div /><div /><div /><div />
              <div className="flex justify-end">
                <button
                  type="button"
                  className="h-7 px-3 rounded-md border border-blue-200 bg-white text-[12px] text-blue-600 hover:bg-blue-50 transition-colors cursor-pointer"
                  onClick={() => callbacks.onAddReplica(group)}
                >添加从库</button>
              </div>
            </div>
          )}
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

function InstanceNodeRow({ item, group, role, first, callbacks, pendingAction }) {
  const isReplica = role === '从库';
  const server = getField(item, 'server', 'Server', '');
  const port = getField(item, 'port', 'Port', '');
  const cpus = Number(getField(item, 'cpus', 'CPUs', 0) || 0);
  const used = Number(getField(item, 'memory_used', 'MemoryUsed', 0) || 0);
  const totalGi = parseGi(getField(item, 'memory', 'Memory'));
  const specs = [
    cpus > 0 ? `${Number.isInteger(cpus) ? cpus : cpus.toFixed(1)}C` : '',
    totalGi > 0 ? `${Number.isInteger(totalGi) ? totalGi : totalGi.toFixed(1)}G` : '',
  ].filter(Boolean).join(' · ');

  // 节点行只放高频运维项（启停 + 删除）；修改配置/调整规格下沉到详情弹窗。
  const menuItems = nodeRowActionsFor(item, group, role, callbacks, { pendingAction });

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
      <div />
      <div />
      <div className="text-[13px]">
        {server ? <CopyableEndpoint host={server} port={port} className="text-gray-600" /> : <span className="text-gray-300">-</span>}
      </div>
      <div><MemoryUsage memory={totalGi} memoryUsed={used} /></div>
      <div><StatusBadge status={getField(item, 'status', 'Status')} /></div>
      <div className="flex justify-end items-center gap-1.5">
        <button type="button" className="h-7 px-3 rounded-md border border-gray-200 bg-white text-[12px] text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" aria-label={`${role} ${item.name} 详情`} onClick={() => callbacks.onOpenNodeDetail(item, group)}>详情</button>
        <MoreMenu items={menuItems} ariaLabel={`${role} ${item.name} 更多操作`} />
      </div>
    </div>
  );
}
