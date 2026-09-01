// 纯工具/格式化函数：无 React 依赖，负责数据规整、字段读取、格式化与文案映射。

export function normalizeMap(value, upperKey, lowerKey) {
  const data = value?.[upperKey] || value?.[lowerKey] || {};
  return Object.entries(data).map(([name, item]) => ({ ...item, name }));
}

export function normalizeNamedMap(value, upperKey, lowerKey) {
  const data = value?.[upperKey] || value?.[lowerKey] || {};
  return new Map(Object.entries(data).map(([name, item]) => [name, { ...item, name }]));
}

export function getField(item, lower, upper, fallback = '') {
  return item?.[lower] ?? item?.[upper] ?? fallback;
}

export function displayValue(value, fallback = '-') {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => displayValue(item, '')).filter(Boolean).join(', ') || fallback;
  }
  if (typeof value === 'object') {
    const pairs = Object.entries(value)
      .filter(([, item]) => item !== null && item !== undefined && item !== '')
      .map(([key, item]) => `${key}: ${displayValue(item, '')}`);
    return pairs.join(', ') || fallback;
  }
  return String(value);
}

export function formatDateTime(value, fallback = '-') {
  if (!value) return fallback;
  const text = String(value);
  const match = text.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/);
  return match ? `${match[1]} ${match[2]}` : displayValue(value, fallback);
}

export function backupRows(payload) {
  const backups = payload?.data?.backups || payload?.data?.Backups || payload?.backups || payload?.Backups || [];
  const list = Array.isArray(backups) ? backups : [];
  return list
    .map((item) => ({
      name: item.name || '',
      time: formatDateTime(item.timestamp, item.timestamp || '-'),
      type: item.type === 'auto' ? '自动' : '手动',
      status: item.status === 'success' ? '成功' : (item.status || '成功'),
      size: item.size ? formatBytes(item.size) : '-'
    }))
    .sort((a, b) => (b.time || '').localeCompare(a.time || ''));
}

export function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '-';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

export function formatDuration(seconds) {
  const s = Number(seconds);
  if (!s || s <= 0) return '-';
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}天 ${h}小时`;
  if (h > 0) return `${h}小时 ${m}分钟`;
  if (m > 0) return `${m}分钟`;
  return `${s}秒`;
}

export function auditOperator(log) {
  return displayValue(log?.operator || log?.Operator, '系统');
}

export function auditActionLabel(action) {
  const labels = {
    'instance.create': '创建实例',
    'instance.delete': '删除实例',
    'instance.start': '启动实例',
    'instance.stop': '停止实例',
    'instance.access_mode': '切换访问模式',
    'config.update': '更新配置',
    'topology.failover': '故障转移',
    'topology.replicate': '建立复制',
    'backup.create': '创建备份',
    'backup.restore': '恢复备份',
    'diagnostic.bigkeys': '大 Key 扫描',
    'node.add': '注册节点',
    'node.remove': '移除节点',
    'node.update': '更新节点',
    'user.create': '创建用户',
    'user.delete': '删除用户',
    'backup.schedule': '修改备份策略',
    'reconcile': '状态协调',
    'reconcile.replica_drift': '副本漂移修复',
    'reconcile.role_drift': '角色漂移修复',
  };
  return labels[action] || action || '-';
}

export function auditLevelLabel(level) {
  const labels = { normal: '常规', important: '重要', critical: '关键' };
  return labels[level] || level || '-';
}

export function auditTargetDisplay(target) {
  if (!target) return '-';
  if (typeof target === 'string') return target;
  const group = target.group_name || target.group || target.Group || target.instance_group;
  const instance = target.instance_name || target.instance || target.Instance || target.name || target.Name || target.new_master;
  if (group && instance) return `${group} / ${instance}`;
  if (instance) return instance;
  if (group) return group;
  if (target.node) return target.node;
  if (target.username) return target.username;
  if (target.object_id) return target.object_id;
  return '-';
}

export function auditSourceLabel(source, actorType) {
  const labels = { manual: '人工操作', dashboard: '控制台', cli: 'CLI', api: 'API', ai: 'AI 助手', sentinel: 'Sentinel', reconcile: '状态协调', scheduler: '定时任务', system: '系统' };
  return labels[source] || (actorType === 'system' ? '系统' : source) || '-';
}

export function auditTriggerLabel(trigger) {
  const labels = { manual: '手动操作', automatic: '自动处理', instance_down: '实例不可达', role_drift: '角色漂移', replica_drift: '复制偏移', scheduled: '定时策略', sentinel_event: 'Sentinel 事件' };
  return labels[trigger] || trigger || '-';
}

export function auditPhaseLabel(phase) {
  const labels = { requested: '已请求', accepted: '已受理', running: '执行中', completed: '已完成', failed: '失败', skipped: '已跳过' };
  return labels[phase] || phase || '-';
}

export function formatAuditDate(value) {
  if (!value) return '';
  return String(value).replaceAll('-', '');
}

export function auditQuery(filters, page = 1, pageSize = 30) {
  const params = new URLSearchParams();
  if (filters.from) params.set('from', formatAuditDate(filters.from));
  if (filters.to) params.set('to', formatAuditDate(filters.to));
  if (filters.group) params.set('group', filters.group.trim());
  if (filters.instance) params.set('instance', filters.instance.trim());
  if (filters.q) params.set('q', filters.q.trim());
  if (filters.operator) params.set('operator', filters.operator.trim());
  if (filters.actor_type) params.set('actor_type', filters.actor_type);
  if (filters.source) params.set('source', filters.source);
  if (filters.level) params.set('level', filters.level);
  if (filters.action) params.set('action', filters.action.trim());
  if (filters.result) params.set('result', filters.result);
  params.set('page', String(page));
  params.set('page_size', String(pageSize));
  return `/audit/query?${params.toString()}`;
}

// 同一异步任务已有终态时，列表只展示终态；尚未完成的 accepted/running 仍保留。
export function collapseAuditRecords(records = []) {
  const terminalResults = new Set(['success', 'failed', 'corrected', 'interrupted', 'canceled', 'skipped']);
  const intermediateResults = new Set(['accepted', 'running']);
  const terminalCorrelationIds = new Set();

  for (const record of records) {
    const correlationId = record.correlation_id || record.CorrelationID;
    const result = record.result || record.Result;
    if (correlationId && terminalResults.has(result)) terminalCorrelationIds.add(correlationId);
  }

  return records.filter((record) => {
    const correlationId = record.correlation_id || record.CorrelationID;
    const result = record.result || record.Result;
    return !(correlationId && intermediateResults.has(result) && terminalCorrelationIds.has(correlationId));
  });
}

export function parseGi(value) {
  if (!value) return 0;
  const match = String(value).trim().match(/^([0-9.]+)\s*(Gi|G|Mi|M|Ti|T)?/i);
  if (!match) return 0;
  const amount = Number(match[1]);
  const unit = (match[2] || 'Gi').toLowerCase();
  if (unit === 'mi' || unit === 'm') return amount / 1024;
  if (unit === 'ti' || unit === 't') return amount * 1024;
  return amount;
}

export function formatGi(value) {
  const amount = Number(value || 0);
  return `${amount.toFixed(amount >= 10 || amount === 0 ? 0 : 1)}Gi`;
}

export function statusMeta(status) {
  status = displayValue(status, '');
  const labels = {
    running: '运行中',
    creating: '创建中',
    stopped: '已停止',
    stopping: '停止中',
    failed: '异常',
    unexpected_stopped: '异常停止',
    unreachable: '不可达',
    healthy: '健康',
    unhealthy: '异常',
    degraded: '拓扑降级',
    failover_conflict: '故障转移冲突',
    drain: '下线中',
    attention: '需关注',
    abnormal: '异常',
  };
  const tone = {
    running: 'ok',
    healthy: 'ok',
    creating: 'warn',
    stopping: 'warn',
    drain: 'warn',
    stopped: 'warn',
    unreachable: 'bad',
    failed: 'bad',
    unexpected_stopped: 'bad',
    unhealthy: 'bad',
    degraded: 'warn',
    failover_conflict: 'bad',
    attention: 'warn',
    abnormal: 'bad',
  };
  return { label: labels[status] || status || '未知', tone: tone[status] || 'neutral' };
}

export function auditResultMeta(result) {
  result = displayValue(result, 'success');
  const labels = {
    success: '成功',
    failed: '失败',
    corrected: '已修正',
    alert: '告警',
    updated: '已更新',
    skipped: '已跳过',
    accepted: '已受理',
    running: '执行中',
    interrupted: '已中断',
    canceled: '已取消',
  };
  const tone = {
    success: 'ok',
    corrected: 'ok',
    updated: 'ok',
    alert: 'warn',
    skipped: 'warn',
    accepted: 'warn',
    running: 'warn',
    interrupted: 'bad',
    canceled: 'neutral',
    failed: 'bad',
  };
  return { label: labels[result] || result, tone: tone[result] || 'neutral' };
}

export function taskKindLabel(kind) {
  const labels = {
    'backup.create': '创建备份',
    'backup.restore': '恢复备份',
    'diagnostic.bigkeys': '大 Key 扫描',
  };
  return labels[kind] || kind || '-';
}

export function taskStatusMeta(status) {
  const labels = {
    pending: '等待受理',
    accepted: '已受理',
    running: '执行中',
    success: '成功',
    failed: '失败',
    interrupted: '已中断',
    canceled: '已取消',
  };
  const tones = {
    pending: 'warn',
    accepted: 'warn',
    running: 'warn',
    success: 'ok',
    failed: 'bad',
    interrupted: 'bad',
    canceled: 'neutral',
  };
  return { label: labels[status] || status || '未知', tone: tones[status] || 'neutral' };
}

export function taskTargetDisplay(task) {
  const group = task?.group_name || task?.GroupName;
  const instance = task?.instance_name || task?.InstanceName;
  if (group && instance) return `${group} / ${instance}`;
  return instance || group || '-';
}

export function taskDuration(task) {
  const started = task?.started_at || task?.StartedAt || task?.created_at || task?.CreatedAt;
  const status = task?.status || task?.Status;
  const terminal = ['success', 'failed', 'interrupted', 'canceled'].includes(status);
  const ended = task?.finished_at || task?.FinishedAt || (terminal ? (task?.updated_at || task?.UpdatedAt) : new Date().toISOString());
  if (!started || !ended) return '-';
  const ms = new Date(ended).getTime() - new Date(started).getTime();
  if (!Number.isFinite(ms) || ms < 0) return '-';
  if (ms < 1000) return '< 1秒';
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}秒`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return `${minutes}分${rest ? `${rest}秒` : ''}`;
}
