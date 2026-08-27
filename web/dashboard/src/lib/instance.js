// 业务派生纯函数：从后端状态计算实例组拓扑、状态、标签等。依赖 lib/format 的字段读取工具。
import { Play, Power } from 'lucide-react';
import {
  normalizeMap,
  normalizeNamedMap,
  getField,
  displayValue,
  parseGi,
  statusMeta,
} from './format.js';

export function instanceStatusTone(status) {
  return statusMeta(status).tone;
}

export function groupStatus(instances) {
  if (instances.length === 1) return getField(instances[0], 'status', 'Status', 'unknown');
  if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'bad')) return 'failed';
  if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'warn')) return 'creating';
  if (instances.length > 0 && instances.every((item) => getField(item, 'status', 'Status') === 'running')) return 'running';
  return instances[0] ? getField(instances[0], 'status', 'Status') : 'unknown';
}

export function topologyStatus(groupState, instances) {
  if (getField(groupState, 'failover_conflict', 'FailoverConflict', false)) return 'failover_conflict';
  const topology = getField(groupState, 'topology_status', 'TopologyStatus', '');
  const instanceStatus = groupStatus(instances);
  if (instanceStatusTone(instanceStatus) === 'bad') return instanceStatus;
  if (instanceStatus && instanceStatus !== 'running') return instanceStatus;
  if (topology) return topology;
  return instanceStatus;
}

export function buildInstanceGroups(instanceState) {
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
        type: getField(groupState, 'type', 'Type', ''),
        currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
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
        type: getField(groupState, 'type', 'Type', ''),
        currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
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
    const memoryUsed = group.instances.reduce((sum, item) => sum + Number(getField(item, 'memory_used', 'MemoryUsed', 0) || 0), 0);
    const disk = group.instances.reduce((sum, item) => sum + parseGi(getField(item, 'disk', 'Disk')), 0);
    const cpus = group.instances.reduce((sum, item) => sum + Number(getField(item, 'cpus', 'CPUs', 0) || 0), 0);
    return {
      ...group,
      master: primary,
      replicas: group.instances.filter((item) => item.name !== primary?.name && getField(item, 'role', 'Role') !== 'master' && getField(item, 'role', 'Role') !== 'standalone'),
      status: topologyStatus(group.groupState, group.instances),
      engine: getField(group.groupState, 'engine', 'Engine', primary ? getField(primary, 'engine', 'Engine', group.engine) : group.engine),
      engineVersion: getField(group.groupState, 'engine_version', 'EngineVersion', group.engineVersion),
      category: getField(group.groupState, 'category', 'Category', group.category),
      type: getField(group.groupState, 'type', 'Type', group.type),
      resource: { memory, memoryUsed, disk, cpus }
    };
  });
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
