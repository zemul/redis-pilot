// 业务派生纯函数：从后端状态计算实例组拓扑、状态、标签等。依赖 lib/format 的字段读取工具。
import { Play, Power } from 'lucide-react';
import { instanceMemoryUsed } from './memory.js';
import {
  normalizeMap,
  normalizeNamedMap,
  getField,
  displayValue,
  parseGi,
  statusMeta,
  incidentTypeLabel,
} from './format.js';

export function instanceStatusTone(status) {
  return statusMeta(status).tone;
}

// 展示层有效色调：有 open 异常时按异常严重度（critical→bad, warning→warn），
// 否则回退到运行态色调。用于列表/大盘行着色与状态徽标，保证与异常表口径一致。
export function groupDisplayTone(group) {
  const incident = group.incident;
  if (incident && incident.count) return incident.severity === 'critical' ? 'bad' : 'warn';
  return instanceStatusTone(group.status);
}

export function groupStatus(instances) {
  if (instances.length === 1) return getField(instances[0], 'status', 'Status', 'unknown');
  if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'bad')) return 'failed';
  if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'warn')) return 'creating';
  if (instances.length > 0 && instances.every((item) => getField(item, 'status', 'Status') === 'running')) return 'running';
  return instances[0] ? getField(instances[0], 'status', 'Status') : 'unknown';
}

// 运行态：只反映实例进程的真实状态（运行中/已停止/异常停止/创建中等），
// 不做任何拓扑健康推算。拓扑/复制类健康判定统一交给后端异常表（/incidents）。
export function runtimeStatus(instances) {
  return groupStatus(instances);
}

// 实例是否处于运行态。
function instanceRunning(item) {
  return getField(item, 'status', 'Status') === 'running';
}

// 状态说明块（详情弹窗用）：完全以异常表为来源。
// 有 open 异常 → 返回按严重度排序的异常清单；无异常 → null（无需额外说明）。
export function topologyReason(group) {
  const incident = group.incident;
  if (!incident || !incident.count) return null;
  const types = incident.types || [];
  const critical = incident.severity === 'critical';
  return {
    critical,
    title: types.length ? incidentTypeLabel(types[0]) : '存在未处理异常',
    detail: types.length
      ? `该实例组存在 ${incident.count} 条未处理异常：${types.map(incidentTypeLabel).join('、')}。详见异常记录。`
      : `该实例组存在 ${incident.count} 条未处理异常。详见异常记录。`,
    types,
    count: incident.count,
  };
}

export function buildInstanceGroups(instanceState, incidentsByGroup = {}) {
  const instances = normalizeMap(instanceState, 'Instances', 'instances');
  const groupStates = normalizeNamedMap(instanceState, 'Groups', 'groups');
  const groups = new Map();
  for (const item of instances) {
    const groupName = getField(item, 'group', 'Group', item.name) || item.name;
    const groupState = groupStates.get(groupName);
    if (!groups.has(groupName)) {
      groups.set(groupName, {
        groupName,
        groupState,
        engine: getField(groupState, 'engine', 'Engine', getField(item, 'engine', 'Engine', '-')),
        engineVersion: getField(groupState, 'engine_version', 'EngineVersion', ''),
        category: getField(groupState, 'category', 'Category', ''),
        access_mode: getField(groupState, 'access_mode', 'AccessMode', 'standard'),
        labels: normalizeGroupLabels(getField(groupState, 'labels', 'Labels', [])),
        type: getField(groupState, 'type', 'Type', ''),
        currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
        sentinelMaster: getField(groupState, 'sentinel_master', 'SentinelMaster', ''),
        runtimeMaster: getField(groupState, 'runtime_master', 'RuntimeMaster', ''),
        proxy: getField(groupState, 'proxy', 'Proxy', null),
        master: null,
        replicas: [],
        instances: []
      });
    }
    const group = groups.get(groupName);
    group.instances.push(item);
    const role = getField(item, 'role', 'Role');
    if (role === 'master' || role === 'standalone') {
      group.master = item;
      group.engine = getField(item, 'engine', 'Engine', group.engine);
    } else {
      group.replicas.push(item);
    }
  }
  for (const [groupName, groupState] of groupStates.entries()) {
    if (!groups.has(groupName)) {
      groups.set(groupName, {
        groupName,
        groupState,
        engine: getField(groupState, 'engine', 'Engine', '-'),
        engineVersion: getField(groupState, 'engine_version', 'EngineVersion', ''),
        category: getField(groupState, 'category', 'Category', ''),
        access_mode: getField(groupState, 'access_mode', 'AccessMode', 'standard'),
        labels: normalizeGroupLabels(getField(groupState, 'labels', 'Labels', [])),
        type: getField(groupState, 'type', 'Type', ''),
        currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
        sentinelMaster: getField(groupState, 'sentinel_master', 'SentinelMaster', ''),
        runtimeMaster: getField(groupState, 'runtime_master', 'RuntimeMaster', ''),
        proxy: getField(groupState, 'proxy', 'Proxy', null),
        master: null,
        replicas: [],
        instances: []
      });
    }
  }
  return Array.from(groups.values()).map((group) => {
    const primary = group.currentMaster
      ? group.instances.find((item) => item.name === group.currentMaster) || group.master || group.instances[0] || null
      : group.master || group.instances[0] || null;
    const memory = group.instances.reduce((sum, item) => sum + parseGi(getField(item, 'memory', 'Memory')), 0);
    const values = group.instances.map(item => instanceMemoryUsed(item, group.engine));
    const memoryUsed = values.every(v => v != null) ? values.reduce((sum, v) => sum + Number(v), 0) : null;
    const disk = group.instances.reduce((sum, item) => sum + parseGi(getField(item, 'disk', 'Disk')), 0);
    const cpus = group.instances.reduce((sum, item) => sum + Number(getField(item, 'cpus', 'CPUs', 0) || 0), 0);
    const runStatus = runtimeStatus(group.instances);
    const incident = incidentsByGroup[group.groupName] || null;
    return {
      ...group,
      master: primary,
      // 只按“不是当前主”过滤，不要用 role 反推：故障转移未收敛时旧主 role 仍是
      // master、但已不等于 currentMaster，若再用 role!=='master' 排除，它会既不入
      // master 又落不进 replicas，导致该节点行/拓扑/操作入口在最需要观察的时刻整体消失。
      replicas: group.instances.filter((item) => item.name !== primary?.name),
      // status 只反映运行态；健康/异常判定交给 incident（来自异常表），由展示层合成。
      status: runStatus,
      incident,
      engine: getField(group.groupState, 'engine', 'Engine', primary ? getField(primary, 'engine', 'Engine', group.engine) : group.engine),
      engineVersion: getField(group.groupState, 'engine_version', 'EngineVersion', group.engineVersion),
      category: getField(group.groupState, 'category', 'Category', group.category),
      type: getField(group.groupState, 'type', 'Type', group.type),
      resource: { memory, memoryUsed, disk, cpus }
    };
  });
}

function normalizeGroupLabels(raw) {
  if (Array.isArray(raw)) return raw.filter((label) => typeof label === 'string' && label.trim()).map((label) => label.trim());
  if (!raw || typeof raw !== 'object') return [];
  return Object.entries(raw).map(([key, value]) => value ? `${key}=${value}` : key);
}

export function engineLabel(group) {
  const engine = displayValue(group.engine, '-');
  const version = displayValue(group.engineVersion, '');
  const label = engine.charAt(0).toUpperCase() + engine.slice(1).toLowerCase();
  return version ? `${label} ${version}` : label;
}

export function categoryLabel(category) {
  const labels = { cache: '缓存', persistent: '持久化' };
  return labels[category] || category || '-';
}

export function extractMetricsPayload(metrics) {
  return metrics?.data || metrics?.Data || metrics || {};
}

export function parseRedisInfo(info) {
  const sections = {};
  let current = 'default';
  for (const rawLine of String(info || '').split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line.startsWith('#')) {
      current = line.slice(1).trim().toLowerCase() || 'default';
      if (!sections[current]) sections[current] = {};
      continue;
    }
    const index = line.indexOf(':');
    if (index < 0) continue;
    if (!sections[current]) sections[current] = {};
    sections[current][line.slice(0, index)] = line.slice(index + 1);
  }
  return sections;
}

export function metricValue(sections, key, fallback = '-') {
  for (const section of Object.values(sections)) {
    if (section && section[key] !== undefined) return section[key];
  }
  return fallback;
}

// 从主节点 INFO 的 replication 段解析主从复制观测：连接的从库数、复制偏移量，
// 以及每个从库的 state / lag(秒) / offset / 落后字节数（master_repl_offset - slave offset）。
// slaveN 行形如：ip=10.0.1.11,port=6380,state=online,offset=12345,lag=0
export function parseReplicationLinks(sections) {
  const repl = sections.replication || sections.Replication || {};
  const masterOffset = Number(repl.master_repl_offset || 0);
  const slaves = [];
  for (const [key, raw] of Object.entries(repl)) {
    if (!/^slave\d+$/.test(key)) continue;
    const parts = {};
    for (const seg of String(raw).split(',')) {
      const i = seg.indexOf('=');
      if (i > 0) parts[seg.slice(0, i)] = seg.slice(i + 1);
    }
    const offset = Number(parts.offset || 0);
    slaves.push({
      ip: parts.ip || '-',
      port: parts.port || '-',
      state: parts.state || '-',
      offset,
      lag: parts.lag !== undefined && parts.lag !== '' ? Number(parts.lag) : null,
      byteLag: masterOffset > 0 && offset > 0 ? Math.max(0, masterOffset - offset) : null,
    });
  }
  slaves.sort((a, b) => `${a.ip}:${a.port}`.localeCompare(`${b.ip}:${b.port}`));
  return {
    role: repl.role || '',
    connectedSlaves: repl.connected_slaves !== undefined ? Number(repl.connected_slaves) : slaves.length,
    masterOffset,
    syncInProgress: repl.master_sync_in_progress === '1',
    slaves,
  };
}

export function topologyLabel(group, master) {
  const type = group.type || (getField(master, 'role', 'Role') === 'standalone' ? 'standalone' : '');
  return type === 'standalone' ? '单点' : `1主 ${group.replicas.length}从`;
}

export function powerActionFor(item, group) {
  const status = getField(item, 'status', 'Status', '');
  const role = getField(item, 'role', 'Role', '');
  const groupType = getField(group, 'type', 'Type', '');
  if (status === 'running' || status === 'healthy') {
    if (role === 'master' && groupType !== 'standalone') {
      return {
        action: 'stop',
        label: '停止实例',
        icon: Power,
        disabledReason: '当前主库不可直接停止，请先完成故障转移。'
      };
    }
    return { action: 'stop', label: '停止实例', icon: Power };
  }
  if (status === 'stopped' || status === 'unexpected_stopped' || status === 'failed') {
    return { action: 'start', label: '启动实例', icon: Play };
  }
  return null;
}

// ── 作用域与展开判定 ─────────────────────────────────────────────
// standalone 组：无主从拓扑，只有一个节点，组即节点。
export function isStandaloneGroup(group) {
  const type = getField(group, 'type', 'Type', '');
  if (type) return type === 'standalone';
  // 老数据可能缺 type：以是否有从库 + master 角色近似判断
  const role = group.master ? getField(group.master, 'role', 'Role', '') : '';
  if (role === 'standalone') return true;
  return group.replicas.length === 0 && role !== 'master';
}

export function isReplicationGroup(group) {
  return !isStandaloneGroup(group);
}

// 展开由"是否主从组"决定，不再依赖从库数量。standalone 不展开；
// replication（含 1主0从、master 异常）都可展开以暴露节点层级。
export function canExpandGroup(group) {
  return isReplicationGroup(group);
}

// 一个节点是否处于运行/健康态。
function nodeRunning(item) {
  const status = getField(item, 'status', 'Status', '');
  return status === 'running' || status === 'healthy';
}

// 组是否存在至少一个健康运行的从库（故障转移前提）。
export function hasHealthyReplica(group) {
  return group.replicas.some(nodeRunning);
}

// ── 组级操作 builder ─────────────────────────────────────────────
// 返回组父行「更多」菜单项（不含主按钮"详情"）。
// callbacks: { onEditLabels, onOpenBackups, onOpenAccess, onAddReplica, onFailover }
// context: { pendingAction }
export function groupActionsFor(group, callbacks = {}, context = {}) {
  const { onEditLabels, onOpenBackups, onOpenAccess, onAddReplica, onFailover } = callbacks;
  const items = [
    { label: '编辑标签', onClick: () => onEditLabels?.(group) },
    { label: '备份管理', onClick: () => onOpenBackups?.(group) },
  ];
  if (isReplicationGroup(group)) {
    const groupBusy = context.pendingAction && String(context.pendingAction).endsWith(`:${group.groupName}`);
    items.push({ label: '接入设置', onClick: () => onOpenAccess?.(group) });
    items.push({
      label: '添加从库',
      disabled: groupBusy ? '实例组正在执行其他操作，请稍后再试' : false,
      onClick: () => { if (!groupBusy) onAddReplica?.(group); },
    });
    const canFailover = hasHealthyReplica(group);
    items.push({
      label: '故障转移',
      disabled: canFailover ? false : '没有可用从库，无法故障转移',
      onClick: () => { if (canFailover) onFailover?.(group); },
    });
  }
  return items;
}

// ── 节点级操作 builder ───────────────────────────────────────────
// 节点行「更多」菜单只放高频运维项：启停 + 删除。低频（配置/规格）下沉到详情弹窗。
// callbacks: { onPowerAction, onDelete }
// context: { pendingAction }
export function nodeRowActionsFor(item, group, role, callbacks = {}, context = {}) {
  const { onPowerAction, onDelete } = callbacks;
  const power = powerActionFor(item, group);
  const items = [];
  if (power) {
    const pendingKey = `${power.action}:${item.name}`;
    items.push({
      label: power.label,
      disabled: power.disabledReason || context.pendingAction === pendingKey,
      onClick: () => { if (!power.disabledReason) onPowerAction?.(item, group, power.action); },
    });
  }
  const deleteReason = nodeDeleteDisabledReason(item, group, role);
  const delPending = context.pendingAction === `delete:${item.name}`;
  items.push({
    label: role === '主库' ? '删除节点' : role === '从库' ? '删除从库' : '删除实例',
    danger: true,
    separatorBefore: true,
    disabled: deleteReason || delPending,
    onClick: () => { if (!deleteReason) onDelete?.(item, group, role); },
  });
  return items;
}

// 主库存在从库时不可删除；其余可删。standalone 删除即删组，交由确认弹窗提示。
export function nodeDeleteDisabledReason(item, group, role) {
  if (role === '主库' && group.replicas.length > 0) {
    return '主库存在从库，请先删除从库或完成拓扑调整';
  }
  return false;
}

// 详情弹窗内「操作区」承载的低频节点操作：修改配置、调整规格。
// callbacks: { onConfig, onResize }
export function nodeDetailActionsFor(item, group, callbacks = {}) {
  const { onConfig, onResize } = callbacks;
  return [
    { label: '修改配置', onClick: () => onConfig?.(item) },
    { label: '调整规格', onClick: () => onResize?.(item) },
  ];
}

// standalone 平铺菜单：组操作 + 节点操作合并，不分区；删除置末。
// callbacks 合并 group 与 node 两类：{ onEditLabels, onOpenBackups, onConfig, onResize, onPowerAction, onDelete }
export function standaloneActionsFor(group, callbacks = {}, context = {}) {
  const item = group.master;
  const { onEditLabels, onOpenBackups, onConfig, onResize, onPowerAction, onDelete } = callbacks;
  const items = [
    { label: '编辑标签', onClick: () => onEditLabels?.(group) },
    { label: '备份管理', onClick: () => onOpenBackups?.(group) },
    { label: '修改配置', onClick: () => onConfig?.(item) },
    { label: '调整规格', onClick: () => onResize?.(item) },
  ];
  const power = item ? powerActionFor(item, group) : null;
  if (power) {
    const pendingKey = `${power.action}:${item.name}`;
    items.push({
      label: power.label,
      disabled: power.disabledReason || context.pendingAction === pendingKey,
      onClick: () => { if (!power.disabledReason) onPowerAction?.(item, group, power.action); },
    });
  }
  if (item) {
    const delPending = context.pendingAction === `delete:${item.name}`;
    items.push({
      label: '删除实例',
      danger: true,
      separatorBefore: true,
      disabled: delPending,
      onClick: () => onDelete?.(item, group, 'standalone'),
    });
  }
  return items;
}
