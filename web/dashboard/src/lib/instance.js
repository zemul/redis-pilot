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

// 实例是否处于对外服务态（与后端 IsServing 语义对齐，前端以派生 status 近似）。
function instanceRunning(item) {
  return getField(item, 'status', 'Status') === 'running';
}

// 把 degraded / failover_conflict 这类"只有颜色没有原因"的状态，
// 从组拓扑数据里派生成可读的原因 + 处置建议，供列表 tooltip 与详情弹窗展示。
// 返回 null 表示无需额外说明（健康/运行中等）。
export function topologyReason(group) {
  const status = group.status;
  if (status === 'failover_conflict') {
    return {
      title: '故障转移冲突',
      detail: 'Sentinel 记录的主节点与平台状态不一致，读写路由可能异常。请执行 sentinel sync 或检查主从拓扑。',
    };
  }
  if (status !== 'degraded') return null;
  const isReplication = group.type === 'replication' || group.replicas.length > 0;
  const masterRunning = group.master && instanceRunning(group.master);
  if (!masterRunning) {
    return {
      title: '主节点不可用',
      detail: isReplication
        ? '主从组的主节点当前未在运行，读写可能中断，需尽快恢复或触发故障转移。'
        : '单点实例当前未在运行。',
    };
  }
  if (isReplication) {
    const runningReplicas = group.replicas.filter(instanceRunning).length;
    if (group.replicas.length === 0) {
      return {
        title: '缺少从节点',
        detail: '主从组当前没有从节点，无法进行故障转移与读写分离，建议补充从节点。',
      };
    }
    if (runningReplicas === 0) {
      return {
        title: '从节点未运行',
        detail: `${group.replicas.length} 个从节点均未在运行，主从复制已中断。`,
      };
    }
    return {
      title: '部分从节点异常',
      detail: `${group.replicas.length - runningReplicas}/${group.replicas.length} 个从节点未在运行。`,
    };
  }
  return { title: '拓扑降级', detail: '实例组拓扑与预期状态不符。' };
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
        labels: getField(groupState, 'labels', 'Labels', {}) || {},
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
        labels: getField(groupState, 'labels', 'Labels', {}) || {},
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
