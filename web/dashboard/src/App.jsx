import { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  AlertTriangle,
  BarChart3,
  Calendar,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  Clock,
  Cpu,
  Database,
  GitMerge,
  HardDrive,
  Info,
  Key,
  LayoutDashboard,
  List,
  LogOut,
  Menu,
  MoreVertical,
  Play,
  Plus,
  Power,
  RefreshCw,
  Save,
  Search,
  Server,
  SlidersHorizontal,
  ShieldCheck,
  Trash2,
  Users,
  X
} from 'lucide-react';
import { LoginPage, UserManagement, TokenManagement, getStoredAuth, clearStoredAuth, authHeaders } from './Auth.jsx';

const TABS = [
  { id: 'dashboard', label: '大盘看板', icon: LayoutDashboard },
  { id: 'instances', label: '实例管理', icon: Database },
  { id: 'alerts', label: '异常总览', icon: AlertTriangle },
  { id: 'servers', label: '服务器节点', icon: Server },
  { id: 'audit', label: '操作审计', icon: List },
  { id: 'users', label: '用户管理', icon: Users },
  { id: 'tokens', label: '密钥管理', icon: Key }
];

function normalizeMap(value, upperKey, lowerKey) {
  const data = value?.[upperKey] || value?.[lowerKey] || {};
  return Object.entries(data).map(([name, item]) => ({ ...item, name }));
}

function normalizeNamedMap(value, upperKey, lowerKey) {
	const data = value?.[upperKey] || value?.[lowerKey] || {};
	return new Map(Object.entries(data).map(([name, item]) => [name, { ...item, name }]));
}

function getField(item, lower, upper, fallback = '') {
	return item?.[lower] ?? item?.[upper] ?? fallback;
}

function displayValue(value, fallback = '-') {
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

function formatDateTime(value, fallback = '-') {
	if (!value) return fallback;
	const text = String(value);
	const match = text.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/);
	return match ? `${match[1]} ${match[2]}` : displayValue(value, fallback);
}

function backupRows(payload) {
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

function formatBytes(bytes) {
	if (!bytes || bytes <= 0) return '-';
	if (bytes < 1024) return bytes + ' B';
	if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
	if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
	return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

function auditOperator(log) {
	return displayValue(log?.operator || log?.Operator, '系统');
}

function auditActionLabel(action) {
	const labels = {
		'instance.create': '创建实例',
		'instance.delete': '删除实例',
		'instance.start': '启动实例',
		'instance.stop': '停止实例',
		'config.update': '更新配置',
		'topology.failover': '故障转移',
		'topology.replicate': '建立复制',
		'backup.create': '创建备份',
		'backup.restore': '恢复备份',
		'node.add': '注册节点',
		'node.remove': '移除节点',
		'node.update': '更新节点',
		'reconcile': '状态协调',
		'reconcile.replica_drift': '副本漂移修复',
		'reconcile.role_drift': '角色漂移修复',
	};
	return labels[action] || action || '-';
}

function auditLevelLabel(level) {
	const labels = { normal: '常规', important: '重要', critical: '关键' };
	return labels[level] || level || '-';
}

function auditTargetDisplay(target) {
	if (!target) return '-';
	if (typeof target === 'string') return target;
	const group = target.group || target.Group || target.instance_group;
	const instance = target.instance || target.Instance || target.name || target.Name || target.new_master;
	if (group && instance) return `${group} / ${instance}`;
	if (instance) return instance;
	if (group) return group;
	return '-';
}

function formatAuditDate(value) {
	if (!value) return '';
	return String(value).replaceAll('-', '');
}

function auditQuery(filters) {
	const params = new URLSearchParams();
	if (filters.from) params.set('from', formatAuditDate(filters.from));
	if (filters.to) params.set('to', formatAuditDate(filters.to));
	if (filters.group) params.set('group', filters.group.trim());
	if (filters.instance) params.set('instance', filters.instance.trim());
	if (filters.level) params.set('level', filters.level);
	if (filters.action) params.set('action', filters.action.trim());
	const query = params.toString();
	return query ? `/audit/query?${query}` : '/audit/query';
}

function parseGi(value) {
	if (!value) return 0;
	const match = String(value).trim().match(/^([0-9.]+)\s*(Gi|G|Mi|M|Ti|T)?/i);
  if (!match) return 0;
  const amount = Number(match[1]);
  const unit = (match[2] || 'Gi').toLowerCase();
  if (unit === 'mi' || unit === 'm') return amount / 1024;
  if (unit === 'ti' || unit === 't') return amount * 1024;
  return amount;
}

function formatGi(value) {
	const amount = Number(value || 0);
	return `${amount.toFixed(amount >= 10 || amount === 0 ? 0 : 1)}Gi`;
}

function statusMeta(status) {
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
  };
  return { label: labels[status] || status || '未知', tone: tone[status] || 'neutral' };
}

function auditResultMeta(result) {
	result = displayValue(result, 'success');
	const labels = {
    success: '成功',
    failed: '失败',
    corrected: '已修正',
    alert: '告警',
    updated: '已更新',
    skipped: '已跳过',
  };
  const tone = {
    success: 'ok',
    corrected: 'ok',
    updated: 'ok',
    alert: 'warn',
    skipped: 'warn',
    failed: 'bad',
  };
  return { label: labels[result] || result, tone: tone[result] || 'neutral' };
}

function instanceStatusTone(status) {
	return statusMeta(status).tone;
}

function groupStatus(instances) {
	if (instances.length === 1) return getField(instances[0], 'status', 'Status', 'unknown');
	if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'bad')) return 'failed';
	if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'warn')) return 'creating';
	if (instances.length > 0 && instances.every((item) => getField(item, 'status', 'Status') === 'running')) return 'running';
	return instances[0] ? getField(instances[0], 'status', 'Status') : 'unknown';
}

function topologyStatus(groupState, instances) {
	if (getField(groupState, 'failover_conflict', 'FailoverConflict', false)) return 'failover_conflict';
	const topology = getField(groupState, 'topology_status', 'TopologyStatus', '');
	const instanceStatus = groupStatus(instances);
	if (instanceStatusTone(instanceStatus) === 'bad') return instanceStatus;
	if (instanceStatus && instanceStatus !== 'running') return instanceStatus;
	if (topology) return topology;
	return instanceStatus;
}

function buildInstanceGroups(instanceState) {
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

function engineLabel(group) {
	const engine = displayValue(group.engine, '-');
	const version = displayValue(group.engineVersion, '');
	const label = engine.charAt(0).toUpperCase() + engine.slice(1).toLowerCase();
	return version ? `${label} ${version}` : label;
}

function categoryLabel(category) {
	const labels = { cache: '缓存', persistent: '持久化' };
	return labels[category] || category || '-';
}

function extractMetricsPayload(metrics) {
	return metrics?.data || metrics?.Data || metrics || {};
}

function parseRedisInfo(info) {
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

function metricValue(sections, key, fallback = '-') {
	for (const section of Object.values(sections)) {
		if (section && section[key] !== undefined) return section[key];
	}
	return fallback;
}

function topologyLabel(group, master) {
	const type = group.type || (getField(master, 'role', 'Role') === 'standalone' ? 'standalone' : '');
	return type === 'standalone' ? '单点' : `1主 ${group.replicas.length}从`;
}

function StatusBadge({ status, children, metaFn = statusMeta }) {
  const meta = metaFn(status);
  return <span className={`badge ${meta.tone}`}><span className="badge-dot"></span>{children || meta.label}</span>;
}

function IconButton({ icon: Icon, label, onClick, disabled = false }) {
  return (
    <button
      className="icon-button"
      disabled={disabled}
      title={typeof disabled === 'string' ? disabled : disabled ? `${label}暂未开放` : label}
      aria-label={label}
      onClick={onClick}
      type="button"
    >
      <Icon size={16} />
    </button>
  );
}

function MoreMenu({ items }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const btnRef = useRef(null);
  const [pos, setPos] = useState({ top: 0, right: 0 });

  useEffect(() => {
    if (!open) return;
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  function toggle() {
    if (!open && btnRef.current) {
      const rect = btnRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 4, right: window.innerWidth - rect.right });
    }
    setOpen(!open);
  }

  const visibleItems = items.filter(Boolean);
  if (visibleItems.length === 0) return null;

  return (
    <div className="more-menu-wrap" ref={ref}>
      <button ref={btnRef} className="icon-button" title="更多操作" aria-label="更多操作" type="button" onClick={toggle}>
        <MoreVertical size={16} />
      </button>
      {open && (
        <div className="more-menu-dropdown" style={{ position: 'fixed', top: pos.top, right: pos.right }}>
          {visibleItems.map((mi, i) => (
            <button key={i} className={`more-menu-item${mi.danger ? ' danger' : ''}`} disabled={mi.disabled} onClick={() => { setOpen(false); mi.onClick(); }}>
              {mi.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function powerActionFor(item, group) {
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

function ProgressBar({ label, current, max, unit = '' }) {
  const percent = max > 0 ? Math.min(100, Math.round((current / max) * 100)) : 0;
  const tone = percent >= 90 ? 'danger' : percent >= 80 ? 'warn' : '';
  return (
    <div className="progress">
      <div className="progress-label">
        <span>{label}</span>
        <span className="mono">{current || 0}{unit} / {max || 0}{unit}</span>
      </div>
      <div className="progress-track">
        <span className={tone} style={{ width: `${percent}%` }} />
      </div>
    </div>
  );
}

function EmptyState({ icon: Icon = Database, title, detail }) {
  return (
    <div className="empty-state">
      <Icon size={36} />
      <strong>{title}</strong>
      {detail && <span>{detail}</span>}
    </div>
  );
}

export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [mobileOpen, setMobileOpen] = useState(false);
  const [auth, setAuth] = useState(() => getStoredAuth());
  const [query, setQuery] = useState('');
  const [expandedGroups, setExpandedGroups] = useState({});
  const [auditFilters, setAuditFilters] = useState({ from: '', to: '', group: '', instance: '', level: '', action: '' });
  const [detail, setDetail] = useState(null);
  const [backupDetail, setBackupDetail] = useState(null);
  const [confirmAction, setConfirmAction] = useState(null);
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [slowlogTarget, setSlowlogTarget] = useState(null);
  const [bigkeysTarget, setBigkeysTarget] = useState(null);
  const [configTarget, setConfigTarget] = useState(null);
  const [accessModeTarget, setAccessModeTarget] = useState(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [pendingAction, setPendingAction] = useState('');
  const [data, setData] = useState({ instances: {}, servers: {}, audits: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const instances = useMemo(() => normalizeMap(data.instances, 'Instances', 'instances'), [data.instances]);
  const instanceGroups = useMemo(() => buildInstanceGroups(data.instances), [data.instances]);
  const servers = useMemo(() => normalizeMap(data.servers, 'Servers', 'servers'), [data.servers]);
  const activeTitle = TABS.find((tab) => tab.id === activeTab)?.label || '大盘看板';

  async function request(path, options = {}) {
    const headers = authHeaders();
    const response = await fetch(path, {
      ...options,
      headers: {
        ...headers,
        'X-Operator': auth?.username || 'dashboard',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {})
      }
    });
    if (response.status === 401) {
      clearStoredAuth();
      setAuth(null);
      throw new Error('登录已过期，请重新登录');
    }
    if (!response.ok) throw new Error(`请求失败：${response.status} ${path}`);
    const body = await response.json();
    if (body.error) throw new Error(body.error);
    return body.data;
  }

  async function refresh() {
    setLoading(true);
    setError('');
    try {
      const [instanceData, nodeData, auditData] = await Promise.all([
        request('/instance/list'),
        request('/node/list'),
        request(auditQuery(auditFilters))
      ]);
      setData({
        instances: instanceData || {},
        servers: nodeData || {},
        audits: auditData?.records || auditData?.Records || []
      });
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    let ignore = false;
    async function load() {
      setLoading(true);
      setError('');
      try {
        const [instanceData, nodeData, auditData] = await Promise.all([
          request('/instance/list'),
          request('/node/list'),
          request(auditQuery(auditFilters))
        ]);
        if (!ignore) {
          setData({
            instances: instanceData || {},
            servers: nodeData || {},
            audits: auditData?.records || auditData?.Records || []
          });
        }
      } catch (err) {
        if (!ignore) setError(err.message || String(err));
      } finally {
        if (!ignore) setLoading(false);
      }
    }
    load();
    return () => {
      ignore = true;
    };
  }, []);

  function handleLogout() {
    clearStoredAuth();
    setAuth(null);
  }

  async function openInstanceDetail(item, group) {
    setDetail({ item, group, loading: true, error: '', metrics: null });
    try {
      const metrics = await request(`/instance/metrics?name=${encodeURIComponent(item.name)}`);
      setDetail({ item, group, loading: false, error: '', metrics });
    } catch (err) {
      setDetail({ item, group, loading: false, error: err.message || String(err), metrics: null });
    }
  }

  async function openBackupList(item, group) {
    setBackupDetail({ item, group, loading: true, error: '', backups: [] });
    try {
      const payload = await request(`/backup/list?name=${encodeURIComponent(item.name)}`);
      setBackupDetail({ item, group, loading: false, error: '', backups: backupRows(payload) });
    } catch (err) {
      setBackupDetail({ item, group, loading: false, error: err.message || String(err), backups: [] });
    }
  }

  function changeInstancePower(item, group, action) {
    setConfirmAction({ item, group, action });
  }

  async function executeInstancePower() {
    if (!confirmAction) return;
    const { item, action } = confirmAction;
    const actionKey = `${action}:${item.name}`;
    setPendingAction(actionKey);
    setError('');
    try {
      await request(`/instance/${action}`, {
        method: 'POST',
        body: JSON.stringify({ name: item.name })
      });
      setConfirmAction(null);
      await refresh();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setPendingAction('');
    }
  }

  async function toggleAccessMode(group, mode) {
    if (!mode) {
      setAccessModeTarget(group);
      return;
    }
    try {
      await request('/instance/access-mode', {
        method: 'POST',
        body: JSON.stringify({ name: group.groupName, access_mode: mode })
      });
      setAccessModeTarget(null);
      await refresh();
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  async function executeDelete() {
    if (!deleteConfirm) return;
    const { item, cleanData } = deleteConfirm;
    const actionKey = `delete:${item.name}`;
    setPendingAction(actionKey);
    setError('');
    try {
      await request('/instance/delete', {
        method: 'POST',
        body: JSON.stringify({ name: item.name, clean_data: cleanData })
      });
      setDeleteConfirm(null);
      await refresh();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setPendingAction('');
    }
  }

  async function createInstance(payload) {
    try {
      await request('/instance/create', {
        method: 'POST',
        body: JSON.stringify(payload)
      });
      return null;
    } catch (err) {
      return err.message || String(err);
    }
  }

  const nav = (
    <nav className="nav">
      {TABS.map((tab) => {
        const Icon = tab.icon;
        return (
          <button
            key={tab.id}
            className={activeTab === tab.id ? 'active' : ''}
            onClick={() => {
              setActiveTab(tab.id);
              setMobileOpen(false);
            }}
          >
            <Icon size={19} />
            <span>{tab.label}</span>
          </button>
        );
      })}
    </nav>
  );

  if (!auth) {
    return <LoginPage onLogin={(data) => setAuth({ token: data.token, username: data.username })} />;
  }

  return (
    <div className="app-shell">
      <header className="mobile-header">
        <div className="mobile-brand"><Database size={22} /> Redis Pilot</div>
        <button className="mobile-menu-button" onClick={() => setMobileOpen(!mobileOpen)} aria-label="导航">
          {mobileOpen ? <X size={22} /> : <Menu size={22} />}
        </button>
      </header>
      {mobileOpen && <div className="mobile-nav">{nav}</div>}

      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><Database size={24} /></div>
          <div>
            <h1>Redis Pilot</h1>
            <p>多实例管理平台</p>
          </div>
        </div>
        {nav}
      </aside>

      <main className="main">
        <header className="topbar">
          <h2>{activeTitle}</h2>
          <div className="toolbar">
            <span className="toolbar-user">{auth.username}</span>
            <button className="ghost-button" onClick={handleLogout}>
              <LogOut size={16} /> 退出
            </button>
          </div>
        </header>

        <section className="content">
          {error && <div className="notice"><AlertCircle size={16} /> {error}</div>}
          {loading ? (
            <div className="panel loading">正在读取服务状态...</div>
          ) : (
            <>
              {activeTab === 'dashboard' && <DashboardView groups={instanceGroups} servers={servers} audits={data.audits} />}
	              {activeTab === 'instances' && (
	                <InstancesView
	                  groups={instanceGroups}
	                  query={query}
	                  setQuery={setQuery}
	                  expandedGroups={expandedGroups}
	                  setExpandedGroups={setExpandedGroups}
	                  onOpenDetail={openInstanceDetail}
	                  onOpenBackups={openBackupList}
	                  onPowerAction={changeInstancePower}
	                  onDelete={(item, group) => setDeleteConfirm({ item, group, cleanData: true })}
	                  onSlowlog={(item) => setSlowlogTarget(item.name)}
	                  onBigkeys={(item) => setBigkeysTarget(item.name)}
	                  onConfig={(item) => setConfigTarget(item.name)}
	                  onToggleAccessMode={toggleAccessMode}
	                  onCreate={() => setCreateOpen(true)}
	                  servers={servers}
	                  pendingAction={pendingAction}
	                />
	              )}
              {activeTab === 'alerts' && <AlertsView groups={instanceGroups} servers={servers} />}
              {activeTab === 'servers' && <ServersView servers={servers} />}
              {activeTab === 'audit' && (
                <AuditView
                  audits={data.audits}
                  filters={auditFilters}
                  setFilters={setAuditFilters}
                  onApply={refresh}
                />
              )}
              {activeTab === 'users' && (
                <UserManagement request={request} />
              )}
              {activeTab === 'tokens' && (
                <TokenManagement request={request} />
              )}
            </>
          )}
        </section>
	      </main>
	      {detail && <InstanceDetailModal detail={detail} requestFn={request} onClose={() => setDetail(null)} />}
	      {slowlogTarget && <SlowlogModal instanceName={slowlogTarget} requestFn={request} onClose={() => setSlowlogTarget(null)} />}
	      {bigkeysTarget && <BigkeysModal instanceName={bigkeysTarget} requestFn={request} onClose={() => setBigkeysTarget(null)} />}
	      {configTarget && <ConfigModal instanceName={configTarget} requestFn={request} onClose={() => setConfigTarget(null)} />}
	      {accessModeTarget && <AccessModeModal group={accessModeTarget} onConfirm={(mode) => toggleAccessMode(accessModeTarget, mode)} onClose={() => setAccessModeTarget(null)} />}
	      {backupDetail && <BackupListModal detail={backupDetail} requestFn={request} onRefresh={refresh} onClose={() => setBackupDetail(null)} />}
	      {confirmAction && (
	        <ConfirmPowerModal
	          detail={confirmAction}
	          loading={pendingAction === `${confirmAction.action}:${confirmAction.item.name}`}
	          onCancel={() => setConfirmAction(null)}
	          onConfirm={executeInstancePower}
	        />
	      )}
	      {deleteConfirm && (
	        <DeleteConfirmModal
	          detail={deleteConfirm}
	          loading={pendingAction === `delete:${deleteConfirm.item.name}`}
	          onCancel={() => setDeleteConfirm(null)}
	          onConfirm={executeDelete}
	          onToggleClean={(v) => setDeleteConfirm((prev) => ({ ...prev, cleanData: v }))}
	        />
	      )}
	      {createOpen && (
	        <CreateInstanceModal
	          servers={servers}
	          groups={instanceGroups}
	          requestFn={request}
          onRefresh={refresh}
          onClose={() => setCreateOpen(false)}
	        />
	      )}
	    </div>
	  );
	}

function DashboardView({ groups, servers, audits }) {
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
    <div className="stack">
      <section className="panel summary-strip">
        <SummaryCell label="实例状态" value={totalInstances ? `${runningInstances}/${totalInstances}` : '0'} icon={Database} tone={instanceTone} />
        <SummaryCell label="拓扑健康" value={totalGroups ? `${healthyGroups}/${totalGroups}` : '0'} icon={GitMerge} tone={groupTone} />
        <SummaryCell label="服务器" value={totalServers ? `${healthyServers}/${totalServers}` : '0'} icon={Server} tone={serverTone} />
      </section>

      <div className="dashboard-grid">
        <section className="panel">
          <div className="panel-header">
            <h3>实例组概览</h3>
            <Database size={18} />
          </div>
          {groups.length === 0 ? (
            <div className="panel-body"><EmptyState title="暂无实例组" detail="创建实例后会在这里展示运行状态。" /></div>
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>实例组</th>
                    <th>引擎</th>
                    <th>拓扑</th>
                    <th>主库</th>
                    <th>代理端口</th>
                    <th>实例</th>
                    <th>状态</th>
                  </tr>
                </thead>
                <tbody>
                  {groups.map((group) => {
                    const master = group.master;
                    const running = group.instances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
                    const total = group.instances.length;
                    return (
                      <tr key={group.groupName}>
                        <td><strong>{group.groupName}</strong></td>
                        <td><span className="muted">{engineLabel(group)}</span></td>
                        <td>{topologyLabel(group, master)}</td>
                        <td>
                          {master ? (
                            <><strong>{master.name}</strong><span className="muted">{getField(master, 'server', 'Server', '-')}</span></>
                          ) : <span className="muted">-</span>}
                        </td>
                        <td><ProxyPorts group={group} /></td>
                        <td><span className={`mono ${running === total ? '' : 'text-warn'}`}>{running}/{total}</span></td>
                        <td><StatusBadge status={group.status} /></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <div className="stack">
          <section className="panel">
            <div className="panel-header">
              <h3>服务器状态</h3>
              <Server size={18} />
            </div>
            <div className="panel-body list">
              {servers.length === 0 ? <EmptyState icon={Server} title="暂无服务器" /> :
                servers.map((s) => (
                  <div key={s.name} className="server-row">
                    <StatusBadge status={getField(s, 'status', 'Status')} />
                    <div>
                      <strong>{s.name}</strong>
                      <code className="muted">{getField(s, 'endpoint', 'Endpoint', '-')}</code>
                    </div>
                    <span className="muted">{instancesPerServer[s.name] || 0} 个实例</span>
                  </div>
                ))
              }
            </div>
          </section>

          <section className="panel">
            <div className="panel-header">
              <h3>最近操作</h3>
              <Clock size={18} />
            </div>
            <div className="panel-body audit-list">
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

function SummaryCell({ label, value, icon: Icon, tone = 'neutral' }) {
  return (
    <div className={`summary-cell tone-${tone}`}>
      <span className="summary-label"><Icon size={15} /> {label}</span>
      <span className="summary-value">{value}</span>
    </div>
  );
}

function InstancesView({ groups, query, setQuery, expandedGroups, setExpandedGroups, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, onCreate, pendingAction }) {
  const filtered = groups.filter((group) => {
    const text = [
      group.groupName,
      group.engine,
      group.engineVersion,
      group.category,
      group.type,
      group.status,
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
    <div className="stack">
      <div className="panel table-toolbar">
        <div className="search-box">
          <Search size={17} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索实例组、节点或端口..." />
        </div>
        <button className="primary-button" onClick={onCreate}>
          <Plus size={16} /> 创建实例
        </button>
      </div>
      <section className="panel table-panel">
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>实例名称 / 组</th>
                <th>类别</th>
                <th>接入方式</th>
                <th>引擎版本</th>
                <th>拓扑角色</th>
                <th>节点</th>
                <th>端口</th>
                <th>内存</th>
                <th>状态</th>
                <th className="right">操作</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr><td colSpan="9"><EmptyState title="暂无实例" /></td></tr>
              ) : filtered.map((group) => (
                <InstanceGroupRows
                  key={group.groupName}
                  group={group}
                  expanded={expandedGroups[group.groupName] === true}
	                onToggle={() => toggleGroup(group.groupName)}
	                onOpenDetail={onOpenDetail}
	                onOpenBackups={onOpenBackups}
	                onPowerAction={onPowerAction}
	                onDelete={onDelete}
	                onSlowlog={onSlowlog}
	                onBigkeys={onBigkeys}
	                onConfig={onConfig}
	                onToggleAccessMode={onToggleAccessMode}
	                pendingAction={pendingAction}
	              />
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function InstanceGroupRows({ group, expanded, onToggle, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, pendingAction }) {
  const master = group.master;
  const hasReplicas = group.replicas.length > 0;
  const topology = topologyLabel(group, master);
  const isStandalone = !hasReplicas;
  const category = group.category ? categoryLabel(group.category) : '';

  const powerAction = master ? powerActionFor(master, group) : null;
  const isMasterWithReplicas = hasReplicas;

  return (
    <>
      <tr className={hasReplicas ? 'group-row clickable' : 'group-row'} onClick={hasReplicas ? onToggle : undefined}>
        <td>
          <div className="group-cell">
            {hasReplicas ? (
              <button className="expand-button" type="button" aria-label={expanded ? '折叠实例组' : '展开实例组'}>
                {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
              </button>
            ) : (
              <span className="expand-placeholder" />
            )}
            <div>
              <div className="group-title">
                <strong>{group.groupName}</strong>
              </div>
              {(hasReplicas || !master || master.name !== group.groupName) && (
                <span className="muted">
                  {master ? `主库: ${master.name}` : '主库状态异常'}
                </span>
              )}
            </div>
          </div>
        </td>
        <td>{category && <span className="role-chip subtle">{category}</span>}</td>
        <td>{group.access_mode === 'rw-split' ? '读写分离' : '标准'}</td>
        <td><span className="engine">{engineLabel(group)}</span></td>
        <td>{topology}</td>
        <td>{master ? getField(master, 'server', 'Server', '-') : '-'}</td>
        <td><ProxyPorts group={group} /></td>
        <td>{master ? <ResourceValue memory={group.resource.memory} memoryUsed={group.resource.memoryUsed} /> : '-'}</td>
        <td><StatusBadge status={group.status} /></td>
        <td>
          {isStandalone && master && (
            <div className="row-actions">
              <IconButton icon={Info} label="详情与指标" onClick={() => onOpenDetail(master, group)} />
              <IconButton icon={HardDrive} label="备份列表" onClick={() => onOpenBackups(master, group)} />
              <MoreMenu items={[
                { label: '修改配置', onClick: () => onConfig(master) },
                { label: '修改接入方式', onClick: () => onToggleAccessMode(group, null) },
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
                  disabled: pendingAction === `delete:${master.name}`,
                  onClick: () => onDelete(master, group)
                }
              ]} />
            </div>
          )}
        </td>
      </tr>
      {expanded && master && (
        <InstanceRow
          item={master}
          group={group}
          role="主库"
          onOpenDetail={onOpenDetail}
          onOpenBackups={onOpenBackups}
          onPowerAction={onPowerAction}
          onDelete={onDelete}
          onSlowlog={onSlowlog}
          onBigkeys={onBigkeys}
          onConfig={onConfig}
          onToggleAccessMode={onToggleAccessMode}
          pendingAction={pendingAction}
        />
      )}
      {expanded && group.replicas.map((replica) => (
        <InstanceRow
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
          onToggleAccessMode={onToggleAccessMode}
          pendingAction={pendingAction}
        />
      ))}
    </>
  );
}

function ProxyPorts({ group }) {
  const proxy = getField(group, 'proxy', 'Proxy', {}) || {};
  const masterPort = proxy.master_port || proxy.MasterPort;
  const autoPort = proxy.auto_port || proxy.AutoPort;
  return (
    <>
      {autoPort && <span className="muted">读写 <span className="mono">{autoPort}</span></span>}
      {masterPort && <span className="muted">主库 <span className="mono">{masterPort}</span></span>}
      {!masterPort && !autoPort && <span className="muted">-</span>}
    </>
  );
}

function ResourceValue({ memory, memoryUsed }) {
  const totalBytes = memory * 1024 * 1024 * 1024;
  const pct = totalBytes > 0 && memoryUsed > 0 ? (memoryUsed / totalBytes) * 100 : 0;
  const usedLabel = memoryUsed > 0 ? formatBytes(memoryUsed) : '0MB';
  const totalLabel = formatBytes(totalBytes);
  const tone = pct > 80 ? 'bar-danger' : pct > 60 ? 'bar-warn' : 'bar-ok';
  return (
    <div className="mem-bar-wrap">
      <div className="mem-bar-track">
        <div className={`mem-bar-fill ${tone}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span className="mem-bar-label mono">{usedLabel} / {totalLabel}</span>
    </div>
  );
}

function InstanceRow({ item, group, role, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, pendingAction }) {
  const powerAction = powerActionFor(item, group);
  const isReplica = role === '从库';
  const isMasterWithReplicas = role === '主库' && group.replicas.length > 0;

  return (
    <tr className={isReplica ? 'replica-row' : 'master-row'}>
      <td>
        <div className={isReplica ? 'replica-cell' : 'master-cell'}>
          {isReplica && <span className="replica-branch" />}
          <div>
            <strong>{item.name}</strong>
            <span className="muted">{getField(item, 'server', 'Server', '-')}</span>
          </div>
        </div>
      </td>
      <td />
      <td />
      <td><span className="muted">{engineLabel(group)}</span></td>
      <td>{role}</td>
      <td>{getField(item, 'server', 'Server', '-')}</td>
      <td />
      <td>
        {(() => {
          const used = Number(getField(item, 'memory_used', 'MemoryUsed', 0) || 0);
          const totalGi = parseGi(getField(item, 'memory', 'Memory'));
          const totalBytes = totalGi * 1024 * 1024 * 1024;
          const pct = totalBytes > 0 && used > 0 ? (used / totalBytes) * 100 : 0;
          const usedLabel = used > 0 ? formatBytes(used) : '0MB';
          const totalLabel = formatBytes(totalBytes);
          const tone = pct > 80 ? 'bar-danger' : pct > 60 ? 'bar-warn' : 'bar-ok';
          return (
            <div className="mem-bar-wrap">
              <div className="mem-bar-track">
                <div className={`mem-bar-fill ${tone}`} style={{ width: `${Math.min(pct, 100)}%` }} />
              </div>
              <span className="mem-bar-label mono">{usedLabel} / {totalLabel}</span>
            </div>
          );
        })()}
      </td>
      <td><StatusBadge status={getField(item, 'status', 'Status')} /></td>
      <td>
        <div className="row-actions">
          <IconButton icon={Info} label="详情与指标" onClick={() => onOpenDetail(item, group)} />
          <IconButton icon={HardDrive} label="备份列表" onClick={() => onOpenBackups(item, group)} />
          <MoreMenu items={[
            { label: '修改配置', onClick: () => onConfig(item) },
            !isReplica && { label: '修改接入方式', onClick: () => onToggleAccessMode(group, null) },
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
          ]} />
        </div>
      </td>
    </tr>
  );
}

function AlertsView({ groups, servers }) {
  const badTone = (status) => instanceStatusTone(status) === 'bad';
  const warnTone = (status) => instanceStatusTone(status) === 'warn';

  // Instance-level anomalies
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

  // Group-level topology anomalies
  const groupAlerts = groups.filter(
    (g) => g.status === 'degraded' || g.status === 'failover_conflict'
  );

  // Server-level anomalies
  const serverAlerts = servers.filter(
    (s) => getField(s, 'status', 'Status') === 'unhealthy'
  );

  const total = instanceAlerts.length + groupAlerts.length + serverAlerts.length;

  return (
    <div className="stack">
      <section className="panel summary-strip">
        <SummaryCell label="实例异常" value={instanceAlerts.length} icon={AlertCircle} tone={instanceAlerts.length > 0 ? 'alert' : 'neutral'} />
        <SummaryCell label="拓扑降级" value={groupAlerts.length} icon={GitMerge} tone={groupAlerts.length > 0 ? 'alert' : 'neutral'} />
        <SummaryCell label="节点异常" value={serverAlerts.length} icon={Server} tone={serverAlerts.length > 0 ? 'alert' : 'neutral'} />
      </section>

      {total === 0 ? (
        <section className="panel">
          <EmptyState icon={ShieldCheck} title="一切正常" detail="当前没有检测到异常。" />
        </section>
      ) : (
        <>
          {instanceAlerts.length > 0 && (
            <section className="panel">
              <div className="panel-header">
                <h3>实例异常</h3>
                <AlertCircle size={18} />
              </div>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>实例</th>
                      <th>实例组</th>
                      <th>节点</th>
                      <th>状态</th>
                      <th>期望</th>
                      <th>观测</th>
                    </tr>
                  </thead>
                  <tbody>
                    {instanceAlerts.map((a, i) => (
                      <tr key={i}>
                        <td className="mono">{a.instance}</td>
                        <td>{a.group}</td>
                        <td>{a.server}</td>
                        <td><StatusBadge status={a.status} /></td>
                        <td className="muted">{a.desired}</td>
                        <td className="muted">{a.observed}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {groupAlerts.length > 0 && (
            <section className="panel">
              <div className="panel-header">
                <h3>拓扑异常</h3>
                <GitMerge size={18} />
              </div>
              <div className="panel-body list">
                {groupAlerts.map((g) => (
                  <div key={g.groupName} className="alert-item">
                    <StatusBadge status={g.status} />
                    <div>
                      <strong>{g.groupName}</strong>
                      <span className="muted">{topologyLabel(g)}</span>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {serverAlerts.length > 0 && (
            <section className="panel">
              <div className="panel-header">
                <h3>节点异常</h3>
                <Server size={18} />
              </div>
              <div className="panel-body list">
                {serverAlerts.map((s) => (
                  <div key={s.name} className="alert-item">
                    <StatusBadge status={getField(s, 'status', 'Status')} />
                    <div>
                      <strong>{s.name}</strong>
                      <code className="muted">{getField(s, 'endpoint', 'Endpoint', '-')}</code>
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

function ServersView({ servers }) {
  if (servers.length === 0) {
    return <section className="panel"><EmptyState icon={Server} title="暂无服务器" detail="节点会从 pool-state.yaml 展示。" /></section>;
  }

  return (
    <div className="server-grid">
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
    <section className="panel server-card">
      <div className="server-head">
        <div>
          <h3>{server.name}</h3>
          <code>{getField(server, 'endpoint', 'Endpoint', '-')}</code>
        </div>
        <StatusBadge status={getField(server, 'status', 'Status')} />
      </div>
      <div className="chip-line">
        {Object.keys(labels).length === 0 ? <span className="chip">无标签</span> :
          Object.entries(labels).map(([key, value]) => <span className="chip" key={key}>{key}: {value}</span>)}
      </div>
      <div className="resource-block">
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
      <div className="muted">实例: {instances.length ? instances.join(', ') : '暂无'}</div>
    </section>
  );
}

function AuditView({ audits, filters, setFilters, onApply }) {
  const records = audits || [];
  function updateFilter(key, value) {
    setFilters((current) => ({ ...current, [key]: value }));
  }
  function resetFilters() {
    setFilters({ from: '', to: '', group: '', instance: '', level: '', action: '' });
  }
  return (
    <div className="stack">
      <section className="panel filter-panel">
        <div className="filter-grid">
          <label>
            <span>开始日期</span>
            <input type="date" value={filters.from} onChange={(event) => updateFilter('from', event.target.value)} />
          </label>
          <label>
            <span>结束日期</span>
            <input type="date" value={filters.to} onChange={(event) => updateFilter('to', event.target.value)} />
          </label>
          <label>
            <span>实例组</span>
            <input value={filters.group} onChange={(event) => updateFilter('group', event.target.value)} placeholder="group" />
          </label>
          <label>
            <span>实例</span>
            <input value={filters.instance} onChange={(event) => updateFilter('instance', event.target.value)} placeholder="instance" />
          </label>
          <label>
            <span>级别</span>
            <select value={filters.level} onChange={(event) => updateFilter('level', event.target.value)}>
              <option value="">全部</option>
              <option value="normal">常规</option>
              <option value="important">重要</option>
              <option value="critical">关键</option>
            </select>
          </label>
          <label>
            <span>动作</span>
            <input value={filters.action} onChange={(event) => updateFilter('action', event.target.value)} placeholder="instance.start" />
          </label>
        </div>
        <div className="filter-actions">
          <button className="ghost-button" onClick={() => {
            resetFilters();
            window.setTimeout(onApply, 0);
          }}>重置</button>
          <button className="primary-button" onClick={onApply}>
            <SlidersHorizontal size={16} /> 应用筛选
          </button>
        </div>
      </section>
      <section className="panel table-panel">
        <div className="panel-header">
          <h3>审计记录</h3>
          <span className="muted">当前展示 {records.length} 条</span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>操作者</th>
                <th>动作</th>
                <th>目标</th>
                <th>级别</th>
                <th>结果</th>
              </tr>
            </thead>
            <tbody>
              {records.length === 0 ? (
                <tr><td colSpan="6"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
              ) : records.slice(0, 50).map((log, index) => (
                <tr key={log.id || log.ID || index}>
                  <td className="mono">{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</td>
                  <td>{auditOperator(log)}</td>
                  <td>{auditActionLabel(log.action || log.Action)}</td>
                  <td><span className="mono">{auditTargetDisplay(log.target || log.Target || log.instance || log.Instance)}</span></td>
                  <td>{auditLevelLabel(log.level || log.Level)}</td>
                  <td><StatusBadge status={log.result || log.Result || 'success'} metaFn={auditResultMeta} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function AuditCard({ log }) {
  const result = log.result || log.Result || 'success';
  const isBad = auditResultMeta(result).tone === 'bad';
  const Icon = isBad ? AlertCircle : CheckCircle2;
  return (
    <div className={isBad ? 'audit-card is-bad' : 'audit-card'}>
      <Icon size={18} />
      <div>
        <strong>{auditActionLabel(log.action || log.Action)}</strong>
        <span>{auditTargetDisplay(log.target || log.Target)} · {auditOperator(log)}</span>
      </div>
      <time>{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</time>
    </div>
  );
}

function MetricsTrendView({ instanceName, requestFn }) {
  const [range_, setRange] = useState('1h');
  const [points, setPoints] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const chartRefs = useRef({});
  const chartInstances = useRef([]);

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError('');
    requestFn(`/instance/metrics/history?name=${encodeURIComponent(instanceName)}&range=${range_}`)
      .then((data) => { if (!ignore) setPoints(data?.points || []); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName, range_]);

  useEffect(() => {
    if (!points || points.length === 0) return;
    import('echarts').then((echarts) => {
      // 清理旧实例
      chartInstances.current.forEach((c) => c.dispose());
      chartInstances.current = [];

      const ts = points.map((p) => new Date(p.ts * 1000));
      const charts = [
        {
          id: 'mem',
          title: '内存使用率 (%)',
          data: points.map((p) => p.mem_max > 0 ? +(p.mem_used / p.mem_max * 100).toFixed(1) : 0),
          color: '#3b82f6',
          max: 100
        },
        {
          id: 'qps',
          title: 'QPS (ops/s)',
          data: points.map((p) => p.ops_sec),
          color: '#3b82f6'
        },
        {
          id: 'hit',
          title: '命中率 (%)',
          data: points.map((p) => {
            const total = p.hits + p.misses;
            return total > 0 ? +((p.hits / total) * 100).toFixed(1) : 100;
          }),
          color: '#3b82f6',
          max: 100
        },
        {
          id: 'conns',
          title: '连接数',
          data: points.map((p) => p.conns),
          color: '#3b82f6'
        },
        {
          id: 'frag',
          title: '内存碎片率',
          data: points.map((p) => p.frag_ratio ? +p.frag_ratio.toFixed(2) : 0),
          color: '#3b82f6'
        },
        {
          id: 'repl',
          title: '主从同步状态',
          data: points.map((p) => p.repl_link !== undefined ? p.repl_link : 1),
          color: '#3b82f6',
          max: 1
        },
        {
          id: 'offset',
          title: '复制偏移量',
          data: points.map((p) => p.repl_offset || 0),
          color: '#3b82f6'
        },
        {
          id: 'keys',
          title: 'Key 数量',
          data: points.map((p) => p.total_keys || 0),
          color: '#3b82f6'
        }
      ];

      charts.forEach(({ id, title, data, color, max }) => {
        const el = chartRefs.current[id];
        if (!el) return;
        const chart = echarts.init(el);
        chart.setOption({
          title: { text: title, textStyle: { fontSize: 13, fontWeight: 500 }, left: 'center' },
          tooltip: { trigger: 'axis', formatter: (params) => {
            const p = params[0];
            return `${p.axisValueLabel}<br/>${p.marker} ${p.value}`;
          }},
          grid: { left: 45, right: 12, top: 35, bottom: 28 },
          xAxis: { type: 'category', data: ts.map((d) => d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })), boundaryGap: false, axisLine: { lineStyle: { color: '#ddd' } }, axisLabel: { fontSize: 10, color: '#999' } },
          yAxis: { type: 'value', min: 0, max: max || undefined, splitLine: { lineStyle: { color: '#f0f0f0' } }, axisLabel: { fontSize: 10, color: '#999' } },
          series: [{ type: 'line', data, smooth: true, symbol: 'none', lineStyle: { width: 2, color }, areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: color + '30' }, { offset: 1, color: color + '05' }] } } }]
        });
        chartInstances.current.push(chart);
      });

      // resize 响应
      const handleResize = () => chartInstances.current.forEach((c) => c.resize());
      window.addEventListener('resize', handleResize);
      return () => window.removeEventListener('resize', handleResize);
    });

    return () => {
      chartInstances.current.forEach((c) => c.dispose());
      chartInstances.current = [];
    };
  }, [points]);

  const rangeOptions = [
    { value: '1h', label: '1小时' },
    { value: '6h', label: '6小时' },
    { value: '24h', label: '24小时' },
    { value: '7d', label: '7天' },
    { value: '30d', label: '30天' }
  ];

  return (
    <section>
      <div className="tab-toolbar" style={{ justifyContent: 'flex-start', gap: 6 }}>
        {rangeOptions.map((opt) => (
          <button key={opt.value} className={`ghost-button small ${range_ === opt.value ? 'active' : ''}`} onClick={() => setRange(opt.value)}>
            {opt.label}
          </button>
        ))}
      </div>
      {loading ? <div className="inline-loading">正在加载趋势数据...</div> :
       error ? <div className="notice compact"><AlertCircle size={16} /> {error}</div> :
       (!points || points.length === 0) ? <div className="empty-state">暂无历史数据，Agent 启动后每分钟采集一次</div> : (
        <div className="metrics-charts">
          <div className="chart-cell" ref={(el) => { chartRefs.current.mem = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.qps = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.hit = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.conns = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.frag = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.repl = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.offset = el; }}></div>
          <div className="chart-cell" ref={(el) => { chartRefs.current.keys = el; }}></div>
        </div>
      )}
    </section>
  );
}

function InstanceDetailModal({ detail, requestFn, onClose }) {
  const { item, group, loading, error, metrics } = detail;
  const payload = extractMetricsPayload(metrics);
  const info = payload.info || payload.Info || '';
  const sections = parseRedisInfo(info);
  const [activeTab, setActiveTab] = useState('trend');
  const metricsSummary = [
    ['Redis 版本', metricValue(sections, 'redis_version')],
    ['角色', metricValue(sections, 'role', getField(item, 'role', 'Role', '-'))],
    ['已用内存', metricValue(sections, 'used_memory_human')],
    ['连接数', metricValue(sections, 'connected_clients')],
    ['Keyspace', displayValue(sections.keyspace || sections.Keyspace)],
    ['运行秒数', metricValue(sections, 'uptime_in_seconds')]
  ];
  const fields = [
    ['实例', item.name],
    ['实例组', group.groupName],
    ['引擎版本', engineLabel(group)],
    ['角色', getField(item, 'role', 'Role', '-')],
    ['节点', getField(item, 'server', 'Server', '-')],
    ['Redis 端口', getField(item, 'port', 'Port', '-')],
    ['资源', `${getField(item, 'memory', 'Memory', '-')} / ${getField(item, 'cpus', 'CPUs', '-')}C / 磁盘 ${displayValue(getField(item, 'disk', 'Disk'), '0Gi')}`],
    ['数据目录', getField(item, 'data_path', 'DataPath', '-')],
    ['配置目录', getField(item, 'config_path', 'ConfigPath', '-')],
    ['备份目录', getField(item, 'backup_path', 'BackupPath', '-')],
    ['创建时间', formatDateTime(getField(item, 'created_at', 'CreatedAt', '-'))]
  ];

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal modal-wide" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3>{item.name}</h3>
            <span>{group.groupName} · {engineLabel(group)}</span>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          <div className="tab-bar">
            <button className={`tab-btn ${activeTab === 'trend' ? 'active' : ''}`} onClick={() => setActiveTab('trend')}>趋势</button>
            <button className={`tab-btn ${activeTab === 'detail' ? 'active' : ''}`} onClick={() => setActiveTab('detail')}>详情</button>
          </div>
          {activeTab === 'detail' && (
            <>
              <section>
                <h4><Info size={16} /> 实例详情</h4>
                <div className="detail-grid">
                  {fields.map(([label, value]) => (
                    <div key={label}>
                      <span>{label}</span>
                      <strong>{displayValue(value)}</strong>
                    </div>
                  ))}
                </div>
              </section>
              <section>
                <h4><BarChart3 size={16} /> 运行指标</h4>
                {loading ? (
                  <div className="inline-loading">正在读取 metrics...</div>
                ) : error ? (
                  <div className="notice compact"><AlertCircle size={16} /> {error}</div>
                ) : (
                  <>
                    <div className="detail-grid metrics-grid">
                      {metricsSummary.map(([label, value]) => (
                        <div key={label}>
                          <span>{label}</span>
                          <strong>{displayValue(value)}</strong>
                        </div>
                      ))}
                    </div>
                    <details className="raw-metrics">
                      <summary>查看 INFO 原文</summary>
                      <pre>{info || '无 metrics 数据'}</pre>
                    </details>
                  </>
                )}
              </section>
            </>
          )}
          {activeTab === 'trend' && (
            <MetricsTrendView instanceName={item.name} requestFn={requestFn} />
          )}
        </div>
      </section>
    </div>
  );
}

function SlowlogModal({ instanceName, requestFn, onClose }) {
  const [entries, setEntries] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError('');
    requestFn(`/instance/slowlog?name=${encodeURIComponent(instanceName)}&count=50`)
      .then((data) => { if (!ignore) setEntries(data?.entries || []); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName]);

  function reload() {
    setLoading(true);
    setError('');
    requestFn(`/instance/slowlog?name=${encodeURIComponent(instanceName)}&count=50`)
      .then((data) => setEntries(data?.entries || []))
      .catch((err) => setError(err.message || String(err)))
      .finally(() => setLoading(false));
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div><h3>慢查询</h3><span>{instanceName}</span></div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          {loading ? <div className="inline-loading">正在加载慢查询...</div> :
           error ? <div className="notice compact"><AlertCircle size={16} /> {error}</div> :
           (!entries || entries.length === 0) ? <div className="empty-state">无慢查询记录</div> : (
            <section>
              <div className="tab-toolbar">
                <span className="muted">{entries.length} 条记录</span>
                <button className="ghost-button small" onClick={reload}><RefreshCw size={14} /> 刷新</button>
              </div>
              <table className="detail-table">
                <thead>
                  <tr><th>时间</th><th>耗时</th><th>命令</th><th>客户端</th></tr>
                </thead>
                <tbody>
                  {entries.map((e, i) => (
                    <tr key={i}>
                      <td className="mono">{e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '-'}</td>
                      <td className="mono">{e.duration_us >= 1000 ? `${(e.duration_us / 1000).toFixed(1)}ms` : `${e.duration_us}µs`}</td>
                      <td className="mono cmd-cell">{e.command || '-'}</td>
                      <td className="mono">{e.client || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </div>
      </section>
    </div>
  );
}

function BigkeysModal({ instanceName, requestFn, onClose }) {
  const [keys, setKeys] = useState(null);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  function run() {
    setLoading(true);
    setError('');
    requestFn(`/instance/bigkeys?name=${encodeURIComponent(instanceName)}`)
      .then((data) => { setKeys(data?.keys || []); setMeta({ scanned: data?.scanned, timeout: data?.timeout, elapsed_ms: data?.elapsed_ms }); })
      .catch((err) => setError(err.message || String(err)))
      .finally(() => setLoading(false));
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div><h3>大 Key 分析</h3><span>{instanceName}</span></div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          <div className="tab-toolbar">
            <span className="muted">阈值: string &gt; 100KB，集合类 &gt; 10000 元素</span>
            <button className="ghost-button small" onClick={run} disabled={loading}>
              {loading ? <><RefreshCw size={14} className="spin" /> 扫描中...</> : <><Play size={14} /> 开始扫描</>}
            </button>
          </div>
          {error && <div className="notice compact"><AlertCircle size={16} /> {error}</div>}
          {meta && (
            <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
              已扫描 {meta.scanned} 个 key，耗时 {meta.elapsed_ms}ms
              {meta.timeout && <span style={{ color: '#f59e0b' }}> （达到 60s 上限，结果可能不完整）</span>}
            </div>
          )}
          {keys && keys.length > 0 && (
            <table className="detail-table">
              <thead>
                <tr><th>Key</th><th>类型</th><th>大小</th></tr>
              </thead>
              <tbody>
                {keys.map((k, i) => (
                  <tr key={i}>
                    <td className="mono cmd-cell">{k.key}</td>
                    <td>{k.type}</td>
                    <td className="mono">{k.size_desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {keys && keys.length === 0 && !loading && <div className="empty-state">未发现大 Key</div>}
        </div>
      </section>
    </div>
  );
}

function AccessModeModal({ group, onConfirm, onClose }) {
  const [mode, setMode] = useState(group.access_mode || 'standard');

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="confirm-modal" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div><h3>修改接入方式</h3><span>{group.groupName}</span></div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body" style={{ padding: 18 }}>
          <p style={{ fontSize: 13, color: 'var(--ink-secondary)', margin: '0 0 12px' }}>当前：{group.access_mode === 'rw-split' ? '读写分离' : '标准'}</p>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, cursor: 'pointer' }}>
            <input type="radio" name="access_mode" value="standard" checked={mode === 'standard'} onChange={() => setMode('standard')} />
            <span><strong>标准</strong> — 所有流量走主库</span>
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
            <input type="radio" name="access_mode" value="rw-split" checked={mode === 'rw-split'} onChange={() => setMode('rw-split')} />
            <span><strong>读写分离</strong> — 读请求分发到从库</span>
          </label>
          <div style={{ marginTop: 16, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <button className="ghost-button small" onClick={onClose}>取消</button>
            <button className="ghost-button small active" disabled={mode === (group.access_mode || 'standard')} onClick={() => onConfirm(mode)}>确认修改</button>
          </div>
        </div>
      </section>
    </div>
  );
}

function ConfigModal({ instanceName, requestFn, onClose }) {
  const VISIBLE_KEYS = [
    'maxmemory', 'maxmemory-policy', 'hz', 'timeout', 'tcp-keepalive', 'tcp-backlog',
    'databases', 'maxclients', 'slowlog-log-slower-than', 'slowlog-max-len',
    'loglevel', 'latency-tracking', 'activedefrag', 'active-defrag-enabled',
    'active-defrag-ignore-bytes', 'active-defrag-threshold-lower', 'active-defrag-threshold-upper',
    'active-defrag-cycle-min', 'active-defrag-cycle-max',
    'lazyfree-lazy-eviction', 'lazyfree-lazy-expire', 'lazyfree-lazy-server-del',
    'no-appendfsync-on-rewrite', 'auto-aof-rewrite-percentage', 'auto-aof-rewrite-min-size',
    'aof-use-rdb-preamble', 'save', 'rdbcompression', 'rdbchecksum',
    'list-max-ziplist-size', 'list-max-listpack-size', 'zset-max-ziplist-entries', 'zset-max-ziplist-value',
    'zset-max-listpack-entries', 'zset-max-listpack-value',
    'hash-max-ziplist-entries', 'hash-max-ziplist-value', 'hash-max-listpack-entries', 'hash-max-listpack-value',
    'set-max-intset-entries', 'set-max-listpack-entries',
    'notify-keyspace-events', 'client-output-buffer-limit',
    'proto-max-bulk-len', 'lua-time-limit', 'min-replicas-to-write', 'min-replicas-max-lag',
    'io-threads', 'io-threads-do-reads',
    // kvrocks
    'rocksdb.compression', 'rocksdb.write_buffer_size', 'rocksdb.max_write_buffer_number',
    'rocksdb.target_file_size_base', 'rocksdb.max_background_compactions', 'rocksdb.max_background_flushes',
    'rocksdb.block_size', 'rocksdb.metadata_block_size', 'rocksdb.enable_blob_files',
  ];
  const visibleSet = new Set(VISIBLE_KEYS);

  const [config, setConfig] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [filter, setFilter] = useState('');
  const [edits, setEdits] = useState({});
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState('');

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError('');
    requestFn(`/instance/config?name=${encodeURIComponent(instanceName)}`)
      .then((data) => { if (!ignore) setConfig(data?.config || {}); })
      .catch((err) => { if (!ignore) setError(err.message || String(err)); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [instanceName]);

  const filteredKeys = config ? Object.keys(config).filter((k) => visibleSet.has(k) && (!filter || k.includes(filter))).sort() : [];
  const hasEdits = Object.keys(edits).length > 0;

  function handleEdit(key, value) {
    if (value === config[key]) {
      const next = { ...edits };
      delete next[key];
      setEdits(next);
    } else {
      setEdits({ ...edits, [key]: value });
    }
  }

  async function handleSave() {
    setSaving(true);
    setSaveResult('');
    try {
      await requestFn('/instance/config', {
        method: 'POST',
        body: JSON.stringify({ name: instanceName, config_overrides: edits })
      });
      setSaveResult(`已保存 ${Object.keys(edits).length} 项配置`);
      // 更新本地 config 状态
      setConfig((prev) => ({ ...prev, ...edits }));
      setEdits({});
    } catch (err) {
      setSaveResult(`保存失败: ${err.message || String(err)}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal modal-wide" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div><h3>修改配置</h3><span>{instanceName}</span></div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          {loading ? <div className="inline-loading">正在加载配置...</div> :
           error ? <div className="notice compact"><AlertCircle size={16} /> {error}</div> : (
            <>
              <div className="tab-toolbar">
                <input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="搜索配置项..." style={{ flex: 1, maxWidth: 300 }} />
                <span className="muted">{filteredKeys.length} 项</span>
                {hasEdits && (
                  <button className="ghost-button small" onClick={handleSave} disabled={saving}>
                    {saving ? '保存中...' : `保存 ${Object.keys(edits).length} 项修改`}
                  </button>
                )}
              </div>
              {saveResult && <div className="notice compact" style={{ marginBottom: 8 }}>{saveResult}</div>}
              <div className="table-scroll" style={{ maxHeight: 480 }}>
                <table className="detail-table">
                  <thead>
                    <tr><th style={{ width: '40%' }}>配置项</th><th>值</th></tr>
                  </thead>
                  <tbody>
                    {filteredKeys.map((key) => {
                      const current = edits[key] !== undefined ? edits[key] : config[key];
                      const modified = edits[key] !== undefined;
                      return (
                        <tr key={key} className={modified ? 'row-modified' : ''}>
                          <td className="mono">{key}</td>
                          <td>
                            <input
                              className="config-value-input"
                              value={current}
                              onChange={(e) => handleEdit(key, e.target.value)}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </section>
    </div>
  );
}

function BackupListModal({ detail, requestFn, onRefresh, onClose }) {
  const { item, group, loading, error, backups } = detail;
  const [execLoading, setExecLoading] = useState(false);
  const [execMsg, setExecMsg] = useState('');
  const [scheduleLoading, setScheduleLoading] = useState(true);
  const [schedule, setSchedule] = useState('');
  const [retention, setRetention] = useState(7);
  const [scheduleError, setScheduleError] = useState('');
  const [scheduleSaving, setScheduleSaving] = useState(false);
  const [scheduleSaved, setScheduleSaved] = useState(false);

  useEffect(() => {
    let ignore = false;
    async function loadSchedule() {
      try {
        const data = await requestFn(`/backup/schedule?name=${encodeURIComponent(item.name)}`);
        if (!ignore) {
          setSchedule(data?.schedule || '');
          setRetention(data?.retention || 7);
        }
      } catch (err) {
        if (!ignore) setScheduleError(err.message || String(err));
      } finally {
        if (!ignore) setScheduleLoading(false);
      }
    }
    loadSchedule();
    return () => { ignore = true; };
  }, [item.name]);

  const isMaster = getField(item, 'role', 'Role') === 'master';
  const hasReplicas = group.replicas && group.replicas.length > 0;

  async function handleExecBackup() {
    setExecLoading(true);
    setExecMsg('');
    try {
      const res = await requestFn('/backup/exec', {
        method: 'POST',
        body: JSON.stringify({ name: item.name })
      });
      const taskId = res?.task_id;
      if (!taskId) {
        setExecMsg('备份已触发');
        setExecLoading(false);
        return;
      }
      setExecMsg('备份执行中...');
      // Poll for status
      const poll = setInterval(async () => {
        try {
          const statusRes = await requestFn(`/backup/status?name=${encodeURIComponent(item.name)}&task_id=${encodeURIComponent(taskId)}`);
          const status = statusRes?.status;
          if (status === 'success') {
            clearInterval(poll);
            setExecMsg('备份完成');
            setExecLoading(false);
          } else if (status === 'failed') {
            clearInterval(poll);
            setExecMsg('备份失败: ' + (statusRes?.error || '未知错误'));
            setExecLoading(false);
          }
          // else still running, keep polling
        } catch {
          clearInterval(poll);
          setExecMsg('查询状态失败');
          setExecLoading(false);
        }
      }, 2000);
    } catch (err) {
      setExecMsg('备份触发失败: ' + (err.message || String(err)));
      setExecLoading(false);
    }
  }

  async function handleSaveSchedule() {
    setScheduleSaving(true);
    setScheduleError('');
    setScheduleSaved(false);
    try {
      await requestFn('/backup/schedule', {
        method: 'POST',
        body: JSON.stringify({ name: item.name, schedule, retention: Number(retention) || 7 })
      });
      setScheduleSaved(true);
    } catch (err) {
      setScheduleError(err.message || String(err));
    } finally {
      setScheduleSaving(false);
    }
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3>{item.name} 备份管理</h3>
            <span>{group.groupName}</span>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          <section>
            <div className="section-header-row">
              <h4><HardDrive size={16} /> 可用备份</h4>
              <button
                className="primary-button compact"
                onClick={handleExecBackup}
                disabled={execLoading}
              >
                {execLoading ? <><RefreshCw size={14} className="spin" /> 备份中...</> : <><Play size={14} /> 立即备份</>}
              </button>
            </div>
            {execMsg && <div className={`notice compact ${execMsg.includes('失败') ? '' : 'notice-ok'}`}><AlertCircle size={16} /> {execMsg}</div>}
            {isMaster && hasReplicas && <div className="form-hint">⚠️ 当前为主库，建议在从库上执行备份以避免影响主库性能。</div>}
            {loading ? (
              <div className="inline-loading">正在读取备份列表...</div>
            ) : error ? (
              <div className="notice compact"><AlertCircle size={16} /> {error}</div>
            ) : backups.length === 0 ? (
              <EmptyState icon={HardDrive} title="暂无备份" detail="该实例当前没有可用备份文件。" />
            ) : (
              <div className="backup-list">
                {backups.map((backup) => (
                  <div className="backup-item" key={backup.name}>
                    <HardDrive size={18} />
                    <div>
                      <strong>{backup.time}</strong>
                      <span>{backup.type} · {backup.size}</span>
                    </div>
                    <StatusBadge status={backup.status === '成功' ? 'running' : 'failed'}>
                      {backup.status}
                    </StatusBadge>
                  </div>
                ))}
              </div>
            )}
          </section>
          <section>
            <h4><Calendar size={16} /> 定时备份策略</h4>
            {scheduleLoading ? (
              <div className="inline-loading">正在读取策略...</div>
            ) : (
              <div className="schedule-form">
                {scheduleError && <div className="notice compact"><AlertCircle size={16} /> {scheduleError}</div>}
                {scheduleSaved && <div className="notice compact notice-ok"><CheckCircle2 size={16} /> 策略已保存</div>}
                <div className="form-grid form-grid-2col">
                  <label>Cron 表达式
                    <input
                      value={schedule}
                      onChange={(e) => { setSchedule(e.target.value); setScheduleSaved(false); }}
                      placeholder="0 2 * * *（每天凌晨2点）"
                    />
                  </label>
                  <label>保留天数
                    <input
                      type="number"
                      min={1}
                      value={retention}
                      onChange={(e) => { setRetention(e.target.value); setScheduleSaved(false); }}
                    />
                  </label>
                </div>
                <div className="schedule-hint">
                  <span className="muted">留空 Cron 表达式则关闭定时备份。常用: <code>0 2 * * *</code>(每天2:00)、<code>0 */6 * * *</code>(每6小时)</span>
                </div>
                <div className="schedule-actions">
                  <button
                    className="primary-button compact"
                    onClick={handleSaveSchedule}
                    disabled={scheduleSaving}
                  >
                    {scheduleSaving ? <><RefreshCw size={14} className="spin" /> 保存中...</> : <><Save size={14} /> 保存策略</>}
                  </button>
                </div>
              </div>
            )}
          </section>
        </div>
      </section>
    </div>
  );
}

function DeleteConfirmModal({ detail, loading, onCancel, onConfirm, onToggleClean }) {
  const { item, group, cleanData } = detail;
  const role = getField(item, 'role', 'Role', '-');
  const status = getField(item, 'status', 'Status', '-');

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onCancel}>
      <section className="confirm-modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="confirm-head">
          <div className="confirm-icon warn">
            <Trash2 size={20} />
          </div>
          <div>
            <h3>确认删除实例</h3>
            <span>{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="confirm-body">
          <p>
            将要删除实例 <strong>{item.name}</strong>。此操作不可恢复，实例容器将被停止并移除。
          </p>
          <div className="confirm-facts">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
          <label className="form-check delete-check">
            <input type="checkbox" checked={cleanData} onChange={(e) => onToggleClean(e.target.checked)} />
            同时删除数据文件（备份 + 数据目录）
          </label>
        </div>
        <div className="confirm-actions">
          <button className="ghost-button" onClick={onCancel} disabled={loading}>取消</button>
          <button className="danger-button" onClick={onConfirm} disabled={loading}>
            {loading ? '删除中...' : '确认删除'}
          </button>
        </div>
      </section>
    </div>
  );
}

function ConfirmPowerModal({ detail, loading, onCancel, onConfirm }) {
  const { item, group, action } = detail;
  const isStop = action === 'stop';
  const title = isStop ? '确认停止实例' : '确认启动实例';
  const actionText = isStop ? '停止' : '启动';
  const role = getField(item, 'role', 'Role', '-');
  const status = getField(item, 'status', 'Status', '-');

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onCancel}>
      <section className="confirm-modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="confirm-head">
          <div className={isStop ? 'confirm-icon warn' : 'confirm-icon'}>
            {isStop ? <Power size={20} /> : <Play size={20} />}
          </div>
          <div>
            <h3>{title}</h3>
            <span>{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="confirm-body">
          <p>
            将要{actionText}实例 <strong>{item.name}</strong>。
            {isStop ? '停止后该实例上的 Redis 服务会不可用，副本冗余或读流量可能受影响。' : '启动会尝试恢复该实例容器。'}
          </p>
          <div className="confirm-facts">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
        </div>
        <div className="confirm-actions">
          <button className="ghost-button" onClick={onCancel} disabled={loading}>取消</button>
          <button className={isStop ? 'danger-button' : 'primary-button'} onClick={onConfirm} disabled={loading}>
            {loading ? '处理中...' : `确认${actionText}`}
          </button>
        </div>
      </section>
    </div>
  );
}

function CreateInstanceModal({ servers, groups, requestFn, onRefresh, onClose }) {
  const [form, setForm] = useState({
    group: '', engine: 'redis', engine_version: '', category: 'cache',
    joinExisting: false, replicaCount: 1, access_mode: 'standard',
    memory: '', memory_unit: 'Gi', cpus: '', disk: '', disk_unit: 'Gi',
    server: '', password: '', config_overrides: ''
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [progress, setProgress] = useState(null);
  const [engines, setEngines] = useState(null);

  useEffect(() => {
    requestFn('/server/info').then((data) => setEngines(data?.engines || null)).catch(() => {});
  }, []);

  const currentVersions = engines && engines[form.engine] ? engines[form.engine].versions || [] : [];
  const defaultVersion = engines && engines[form.engine] ? engines[form.engine].default || '' : '';

  const replicaGroups = groups.filter((g) => g.master);
  const isReplica = form.joinExisting;

  function update(key, value) {
    setForm((prev) => {
      const next = { ...prev, [key]: value };
      if (key === 'joinExisting') {
        next.group = '';
        next.replicaCount = 1;
      }
      if (key === 'group' && prev.joinExisting) {
        const g = replicaGroups.find((g) => g.groupName === value);
        if (g) {
          next.category = g.category || prev.category;
          next.engine = g.engine || prev.engine;
        }
      }
      return next;
    });
  }

  function parseOverrides(text) {
    const map = {};
    for (const line of String(text || '').split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const eq = trimmed.indexOf('=');
      if (eq < 0) continue;
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (key) map[key] = val;
    }
    return Object.keys(map).length ? map : undefined;
  }

  function buildInstances() {
    const overrides = parseOverrides(form.config_overrides);
    const base = {
      category: form.category,
      engine: form.engine,
      memory: form.memory.trim() + form.memory_unit,
      cpus: form.cpus ? Number(form.cpus) : 0,
      ...(form.server.trim() ? { server: form.server.trim() } : {}),
      ...(form.disk.trim() ? { disk: form.disk.trim() + form.disk_unit } : {}),
      ...(form.password ? { password: form.password } : {}),
      ...(overrides ? { config_overrides: overrides } : {})
    };

    if (isReplica) {
      const group = replicaGroups.find((g) => g.groupName === form.group);
      if (!group || !group.master) return [];
      const masterName = group.master.name;
      let maxN = -1;
      for (const inst of group.instances) {
        const m = String(inst.name).match(/-(\d+)$/);
        if (m) maxN = Math.max(maxN, parseInt(m[1]));
      }
      const list = [];
      for (let i = 0; i < form.replicaCount; i++) {
        const name = `${form.group}-${maxN + 1 + i}`;
        list.push({
          name, role: '副本',
          payload: { ...base, name, type: 'replication', replica_of: masterName }
        });
      }
      return list;
    }

    const isStandalone = form.replicaCount === 0;
    const masterName = isStandalone ? form.group : `${form.group}-0`;
    const list = [{
      name: masterName,
      role: isStandalone ? '单点' : '主库',
      payload: {
        ...base,
        name: masterName,
        type: isStandalone ? 'standalone' : 'replication',
        group: form.group,
        ...(form.engine_version.trim() ? { engine_version: form.engine_version.trim() } : {}),
        ...(!isStandalone && form.access_mode ? { access_mode: form.access_mode } : {})
      }
    }];
    for (let i = 1; i <= form.replicaCount; i++) {
      const name = `${form.group}-${i}`;
      list.push({
        name, role: '副本',
        payload: { ...base, name, type: 'replication', replica_of: masterName }
      });
    }
    return list;
  }

  const instances = buildInstances();

  async function handleSubmit() {
    setError('');
    if (!form.group.trim()) { setError(isReplica ? '请选择实例组' : '请填写名称'); return; }
    if (!form.memory.trim()) { setError('请填写内存'); return; }

    setSubmitting(true);
    const created = [];
    for (let i = 0; i < instances.length; i++) {
      const inst = instances[i];
      setProgress({ current: i + 1, total: instances.length, name: inst.name });
      try {
        await requestFn('/instance/create', { method: 'POST', body: JSON.stringify(inst.payload) });
        created.push(inst.name);
      } catch (err) {
        const msg = err.message || String(err);
        if (created.length > 0) {
          setError(`已创建 ${created.length}/${instances.length} 个（${created.join('、')}）。${inst.name} 失败：${msg}`);
          await onRefresh();
        } else {
          setError(`${inst.name} 创建失败：${msg}`);
        }
        setProgress(null);
        setSubmitting(false);
        return;
      }
    }
    setProgress(null);
    setSubmitting(false);
    await onRefresh();
    onClose();
  }

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={submitting ? undefined : onClose}>
      <section className="modal create-modal" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3>创建实例</h3>
            <span>新建 Redis / Kvrocks 实例</span>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          {error && <div className="notice compact"><AlertCircle size={16} /> {error}</div>}
          <section>
            <h4><Info size={16} /> 基本信息</h4>
            <div className="form-grid">
              <label>{isReplica ? '实例组' : '名称'} <span className="req">*</span>
                {isReplica ? (
                  <select value={form.group} onChange={(e) => update('group', e.target.value)}>
                    <option value="">选择组</option>
                    {replicaGroups.map((g) => <option key={g.groupName} value={g.groupName}>{g.groupName}</option>)}
                  </select>
                ) : (
                  <input value={form.group} onChange={(e) => update('group', e.target.value)} placeholder="如 order" />
                )}
              </label>
              <label>类别 <span className="req">*</span>
                <select value={form.category} onChange={(e) => update('category', e.target.value)} disabled={isReplica}>
                  <option value="cache">缓存</option>
                  <option value="persistent">持久化</option>
                </select>
              </label>
              <label>引擎 <span className="req">*</span>
                <select value={form.engine} onChange={(e) => update('engine', e.target.value)} disabled={isReplica}>
                  <option value="redis">Redis</option>
                  <option value="kvrocks">Kvrocks</option>
                </select>
              </label>
              <label>引擎版本
                <select value={form.engine_version} onChange={(e) => update('engine_version', e.target.value)} disabled={isReplica}>
                  <option value="">默认{defaultVersion ? ` (${defaultVersion})` : ''}</option>
                  {currentVersions.map((v) => <option key={v} value={v}>{v}</option>)}
                </select>
              </label>
              <label>副本数量 <span className="req">*</span>
                <input type="number" min={0} value={form.replicaCount} onChange={(e) => update('replicaCount', Math.max(0, Number(e.target.value) || 0))} />
              </label>
              <label>接入方式
                <select value={form.access_mode} onChange={(e) => update('access_mode', e.target.value)} disabled={isReplica || form.replicaCount === 0}>
                  <option value="standard">标准（全部走主库）</option>
                  <option value="rw-split">读写分离</option>
                </select>
              </label>
            </div>
            {replicaGroups.length > 0 && (
              <label className="form-check">
                <input type="checkbox" checked={form.joinExisting} onChange={(e) => update('joinExisting', e.target.checked)} disabled={submitting} />
                添加副本到已有组
              </label>
            )}
            {isReplica && <p className="form-hint">引擎 / 版本 / 类别将从主库自动继承。副本数量为本次新增的副本数。</p>}
            {!isReplica && <p className="form-hint">副本数量 0 = 单点，1+ = 主从。实例名自动生成为 {form.group || '组名'}-0、{form.group || '组名'}-1 …</p>}
          </section>
          <section>
            <h4><Database size={16} /> 资源配置</h4>
            <div className="form-grid">
              <label>内存 <span className="req">*</span>
                <div className="input-unit">
                  <input type="number" value={form.memory} onChange={(e) => update('memory', e.target.value)} placeholder="如 2" />
                  <select value={form.memory_unit} onChange={(e) => update('memory_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                    <option value="Mi">Mi</option>
                  </select>
                </div>
              </label>
              <label>CPU 核数
                <input type="number" step="0.5" value={form.cpus} onChange={(e) => update('cpus', e.target.value)} placeholder="如 1" />
              </label>
              <label>磁盘
                <div className="input-unit">
                  <input type="number" value={form.disk} onChange={(e) => update('disk', e.target.value)} placeholder="如 10" />
                  <select value={form.disk_unit} onChange={(e) => update('disk_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                  </select>
                </div>
              </label>
            </div>
          </section>
          <section>
            <h4><Server size={16} /> 部署选项</h4>
            <div className="form-grid">
              <label>目标节点
                <select value={form.server} onChange={(e) => update('server', e.target.value)}>
                  <option value="">自动调度</option>
                  {servers.map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
                </select>
              </label>
              <label>密码
                <input type="password" value={form.password} onChange={(e) => update('password', e.target.value)} placeholder="留空则无密码" />
              </label>
            </div>
          </section>
          <section>
            <h4><SlidersHorizontal size={16} /> 高级</h4>
            <div className="form-grid form-grid-full">
              <label>配置覆盖
                <textarea rows="4" value={form.config_overrides} onChange={(e) => update('config_overrides', e.target.value)} placeholder={'每行一个，格式 key=value\n如 maxmemory-policy=allkeys-lru'} />
              </label>
            </div>
          </section>
          {instances.length > 0 && !submitting && (
            <section>
              <h4><CheckCircle2 size={16} /> 将创建以下实例</h4>
              <div className="instance-preview">
                {instances.map((inst) => (
                  <div key={inst.name} className="preview-item">
                    <code>{inst.name}</code>
                    <span className="preview-role">{inst.role}</span>
                  </div>
                ))}
              </div>
            </section>
          )}
          {progress && (
            <div className="notice compact"><RefreshCw size={16} className="spin" /> 正在创建 {progress.name}（{progress.current}/{progress.total}）…</div>
          )}
        </div>
        <div className="confirm-actions">
          <button className="ghost-button" onClick={onClose} disabled={submitting}>取消</button>
          <button className="primary-button" onClick={handleSubmit} disabled={submitting || instances.length === 0}>
            {submitting ? '创建中…' : `创建 ${instances.length} 个实例`}
          </button>
        </div>
      </section>
    </div>
  );
}
