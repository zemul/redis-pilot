import { useEffect, useMemo, useRef, useState } from 'react';
import { Routes, Route, Navigate, useNavigate, useParams } from 'react-router-dom';
import {
  AlertCircle,
  AlertTriangle,
  BarChart3,
  Calendar,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  Check,
  Clock,
  Copy,
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

function auditQuery(filters, page = 1, pageSize = 30) {
	const params = new URLSearchParams();
	if (filters.from) params.set('from', formatAuditDate(filters.from));
	if (filters.to) params.set('to', formatAuditDate(filters.to));
	if (filters.group) params.set('group', filters.group.trim());
	if (filters.instance) params.set('instance', filters.instance.trim());
	if (filters.level) params.set('level', filters.level);
	if (filters.action) params.set('action', filters.action.trim());
	params.set('page', String(page));
	params.set('page_size', String(pageSize));
	return `/audit/query?${params.toString()}`;
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

const dotColor = { ok: 'bg-emerald-400', warn: 'bg-amber-500', bad: 'bg-red-500', neutral: 'bg-gray-300' };

function StatusBadge({ status, children, metaFn = statusMeta }) {
  const meta = metaFn(status);
  const textTone = { ok: 'text-gray-500', warn: 'text-amber-600', bad: 'text-red-600', neutral: 'text-gray-500' };
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium whitespace-nowrap ${textTone[meta.tone] || textTone.neutral}`}>
      <span className={`w-2 h-2 rounded-full shrink-0 ${dotColor[meta.tone] || dotColor.neutral}`} />
      {children || meta.label}
    </span>
  );
}

function StatusDot({ status }) {
  const meta = statusMeta(status);
  return <span className={`w-[7px] h-[7px] rounded-full shrink-0 ${dotColor[meta.tone] || dotColor.neutral}`} />;
}

function IconButton({ icon: Icon, label, onClick, disabled = false }) {
  return (
    <button
      className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 hover:border-gray-300 hover:text-gray-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
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

function UserMenu({ username, onLogout, onNavigate }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (!open) return;
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  return (
    <div className="relative inline-flex" ref={ref}>
      <button
        type="button"
        className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-700 font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer"
        onClick={() => setOpen(!open)}
      >
        {username}
        <ChevronDown size={14} className={`text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-[calc(100%+4px)] z-50 min-w-[120px] bg-white border border-gray-200 rounded-md shadow-lg py-1">
          <button
            className="block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer text-gray-700 hover:bg-gray-100"
            onClick={() => { setOpen(false); onNavigate('tokens'); }}
          >
            <Key size={13} className="inline -mt-0.5 mr-1.5" />密钥管理
          </button>
          <button
            className="block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer text-red-600 hover:bg-red-50"
            onClick={() => { setOpen(false); onLogout(); }}
          >
            <LogOut size={13} className="inline -mt-0.5 mr-1.5" />退出登录
          </button>
        </div>
      )}
    </div>
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
    <div className="relative inline-flex" ref={ref}>
      <button ref={btnRef} className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 hover:border-gray-300 hover:text-gray-900 transition-colors" title="更多操作" aria-label="更多操作" type="button" onClick={toggle}>
        <MoreVertical size={16} />
      </button>
      {open && (
        <div className="fixed z-50 min-w-[100px] bg-white border border-gray-200 rounded-md shadow-lg py-1" style={{ top: pos.top, right: pos.right }}>
          {visibleItems.map((mi, i) => (
            <button
              key={i}
              className={`block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${mi.danger ? 'text-red-600 hover:bg-red-50' : 'text-gray-700 hover:bg-gray-100'}`}
              disabled={mi.disabled}
              onClick={() => { setOpen(false); mi.onClick(); }}
            >
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
  const barColor = percent >= 90 ? 'bg-red-500' : percent >= 80 ? 'bg-amber-500' : 'bg-gray-900';
  return (
    <div className="grid gap-1.5">
      <div className="flex items-center justify-between gap-3 text-xs text-gray-500">
        <span className="inline-flex items-center gap-1.5">{label}</span>
        <span className="font-mono tabular-nums">{current || 0}{unit} / {max || 0}{unit}</span>
      </div>
      <div className="h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <span className={`block h-full rounded-full transition-all duration-300 ${barColor}`} style={{ width: `${percent}%` }} />
      </div>
    </div>
  );
}

function EmptyState({ icon: Icon = Database, title, detail }) {
  return (
    <div className="min-h-[200px] p-7 grid place-items-center content-center gap-2 text-center text-gray-400">
      <Icon size={36} className="text-gray-300" />
      <strong className="text-gray-500 font-medium">{title}</strong>
      {detail && <span className="text-[13px]">{detail}</span>}
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/:tab?" element={<AppShell />} />
    </Routes>
  );
}

const VALID_TABS = TABS.map((tab) => tab.id);

function AppShell() {
  const { tab } = useParams();
  const navigate = useNavigate();
  const activeTab = tab || 'dashboard';
  const setActiveTab = (id) => navigate(id === 'dashboard' ? '/' : `/${id}`);
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
  const [data, setData] = useState({ instances: {}, servers: {}, audits: [], sentinel: null });
  const [serverInfo, setServerInfo] = useState(null);
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
    const body = await response.json().catch(() => null);
    if (!response.ok) throw new Error(body?.error || `请求失败：${response.status} ${path}`);
    if (body?.error) throw new Error(body.error);
    return body?.data;
  }

  async function refresh() {
    setLoading(true);
    setError('');
    try {
      const [instanceData, nodeData, auditData, sentinelData] = await Promise.all([
        request('/instance/list'),
        request('/node/list'),
        request(auditQuery(auditFilters, 1, 6)),
        request('/sentinel/status').catch(() => null)
      ]);
      setData({
        instances: instanceData || {},
        servers: nodeData || {},
        audits: auditData?.records || auditData?.Records || [],
        sentinel: sentinelData
      });
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!auth) return;
    request('/server/info').then((data) => setServerInfo(data)).catch(() => {});
  }, [auth]);

  useEffect(() => {
    let ignore = false;
    async function load() {
      setLoading(true);
      setError('');
      try {
        const [instanceData, nodeData, auditData, sentinelData] = await Promise.all([
          request('/instance/list'),
          request('/node/list'),
          request(auditQuery(auditFilters, 1, 6)),
          request('/sentinel/status').catch(() => null)
        ]);
        if (!ignore) {
          setData({
            instances: instanceData || {},
            servers: nodeData || {},
            audits: auditData?.records || auditData?.Records || [],
            sentinel: sentinelData
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
    <nav className="grid gap-0.5">
      {TABS.map((tab) => {
        const Icon = tab.icon;
        return (
          <button
            key={tab.id}
            className={`min-h-[40px] rounded-md px-3 flex items-center gap-3 text-left text-sm font-medium cursor-pointer border-0 transition-colors ${
              activeTab === tab.id
                ? 'bg-gray-900 text-white'
                : 'bg-transparent text-gray-500 hover:bg-gray-100 hover:text-gray-900'
            }`}
            onClick={() => {
              setActiveTab(tab.id);
              setMobileOpen(false);
            }}
          >
            <Icon size={19} className={activeTab === tab.id ? 'text-white' : 'text-gray-400'} />
            <span>{tab.label}</span>
          </button>
        );
      })}
    </nav>
  );

  if (!auth) {
    return <LoginPage onLogin={(data) => setAuth({ token: data.token, username: data.username })} />;
  }

  if (tab && !VALID_TABS.includes(tab)) {
    return <Navigate to="/" replace />;
  }

  return (
    <div className="min-h-screen grid grid-cols-[248px_minmax(0,1fr)] max-md:block">
      {/* Mobile header */}
      <header className="hidden max-md:flex sticky top-0 z-20 h-[52px] px-3.5 items-center justify-between bg-white border-b border-gray-200">
        <div className="inline-flex items-center gap-2 font-semibold text-[15px]"><Database size={22} className="text-gray-900" /> Redis Pilot</div>
        <button className="w-[38px] h-[34px] rounded-md border border-gray-200 bg-white text-gray-900 inline-flex items-center justify-center" onClick={() => setMobileOpen(!mobileOpen)} aria-label="导航">
          {mobileOpen ? <X size={22} /> : <Menu size={22} />}
        </button>
      </header>
      {mobileOpen && <div className="fixed inset-x-0 top-[52px] z-10 p-3 bg-white border-b border-gray-200 shadow-sm md:hidden">{nav}</div>}

      {/* Sidebar */}
      <aside className="sticky top-0 h-screen p-5 px-3.5 bg-white border-r border-gray-200 flex flex-col max-md:hidden">
        <div className="flex items-center gap-3 px-2 pb-6">
          <div className="w-[34px] h-[34px] rounded-md grid place-items-center bg-gray-900 text-white shrink-0">
            <Database size={24} />
          </div>
          <div>
            <h1 className="m-0 text-[15px] font-semibold leading-tight tracking-tight text-gray-900">Redis Pilot</h1>
            <p className="mt-0.5 text-xs text-gray-400 font-medium">多实例管理平台</p>
          </div>
        </div>
        {nav}
      </aside>

      {/* Main */}
      <main className="min-w-0">
        <header className="sticky top-0 z-20 min-h-[64px] px-7 py-3.5 flex items-center justify-between gap-4 border-b border-gray-200 bg-white/90 backdrop-blur-sm max-md:static max-md:flex-col max-md:items-start max-md:p-4">
          <h2 className="m-0 text-xl font-semibold tracking-tight">{activeTitle}</h2>
          <div className="flex items-center justify-end gap-2 flex-wrap">
            <UserMenu username={auth.username} onLogout={handleLogout} onNavigate={setActiveTab} />
          </div>
        </header>

        <section className="p-7 max-md:p-4">
          {error && <div className="mb-4 border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} className="shrink-0" /> {error}</div>}
          {loading ? (
            <div className="border border-gray-200 rounded-lg bg-white min-h-[260px] grid place-items-center text-gray-400">正在读取服务状态...</div>
          ) : (
            <>
              {activeTab === 'dashboard' && <DashboardView groups={instanceGroups} servers={servers} audits={data.audits} sentinel={data.sentinel} publicHost={serverInfo?.proxy_public_endpoint} />}
              {activeTab === 'instances' && (
                <InstancesView
                  groups={instanceGroups}
                  publicHost={serverInfo?.proxy_public_endpoint}
                  sentinelPort={serverInfo?.proxy_sentinel_port}
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
              {activeTab === 'alerts' && <AlertsView groups={instanceGroups} servers={servers} publicHost={serverInfo?.proxy_public_endpoint} />}
              {activeTab === 'servers' && <ServersView servers={servers} />}
              {activeTab === 'audit' && (
                <AuditView
                  filters={auditFilters}
                  setFilters={setAuditFilters}
                  request={request}
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
      {accessModeTarget && <AccessModeModal group={accessModeTarget} publicHost={serverInfo?.proxy_public_endpoint} sentinelPort={serverInfo?.proxy_sentinel_port} onConfirm={(mode) => toggleAccessMode(accessModeTarget, mode)} onClose={() => setAccessModeTarget(null)} />}
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

function DashboardView({ groups, servers, audits, sentinel, publicHost }) {
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

      <div className="grid grid-cols-[minmax(0,1.15fr)_minmax(360px,0.85fr)] gap-4 max-lg:grid-cols-1">
        {/* Instance groups table */}
        <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
          <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
            <h3 className="m-0 text-[15px] font-semibold">实例组概览</h3>
            <Database size={18} className="text-gray-400" />
          </div>
          {groups.length === 0 ? (
            <div className="p-4"><EmptyState title="暂无实例组" detail="创建实例后会在这里展示运行状态。" /></div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[920px] border-collapse">
                <thead>
                  <tr>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例组</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">引擎</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">拓扑</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">主库</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">代理端口</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例</th>
                    <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">状态</th>
                  </tr>
                </thead>
                <tbody>
                  {groups.map((group) => {
                    const master = group.master;
                    const running = group.instances.filter((inst) => getField(inst, 'status', 'Status') === 'running').length;
                    const total = group.instances.length;
                    return (
                      <tr key={group.groupName} className="border-b border-gray-200 last:border-b-0">
                        <td className="px-4 py-3 text-[13px]"><strong className="font-medium">{group.groupName}</strong></td>
                        <td className="px-4 py-3 text-[13px] text-gray-400">{engineLabel(group)}</td>
                        <td className="px-4 py-3 text-[13px]">{topologyLabel(group, master)}</td>
                        <td className="px-4 py-3 text-[13px]">
                          {master ? (
                            <><strong className="font-medium">{master.name}</strong> <span className="text-gray-400">{getField(master, 'server', 'Server', '-')}</span></>
                          ) : <span className="text-gray-400">-</span>}
                        </td>
                        <td className="px-4 py-3 text-[13px]"><ProxyPorts group={group} publicHost={publicHost} /></td>
                        <td className="px-4 py-3 text-[13px]"><span className={`font-mono tabular-nums ${running === total ? '' : 'text-amber-600'}`}>{running}/{total}</span></td>
                        <td className="px-4 py-3 text-[13px]"><StatusBadge status={group.status} /></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
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

function SummaryCell({ label, value, icon: Icon, tone = 'neutral' }) {
  const toneStyles = {
    neutral: '',
    warn: '',
    alert: 'relative bg-red-50',
  };
  const valueTone = {
    neutral: 'text-gray-900',
    warn: 'text-amber-600',
    alert: 'text-red-700',
  };
  const labelTone = {
    neutral: 'text-gray-400',
    warn: 'text-gray-400',
    alert: 'text-red-600',
  };
  return (
    <div className={`px-5 py-4 border-r border-gray-200 last:border-r-0 flex flex-col gap-2.5 max-md:border-r-0 max-md:border-b max-md:last:border-b-0 ${toneStyles[tone] || ''}`}>
      {tone === 'alert' && <span className="absolute inset-x-0 top-0 h-0.5 bg-red-500" />}
      <span className={`inline-flex items-center gap-1.5 text-[13px] font-medium ${labelTone[tone] || 'text-gray-400'}`}><Icon size={15} /> {label}</span>
      <span className={`font-mono text-[34px] font-semibold leading-none tracking-tight tabular-nums ${valueTone[tone] || 'text-gray-900'}`}>{value}</span>
    </div>
  );
}

function InstancesView({ groups, publicHost, sentinelPort, query, setQuery, expandedGroups, setExpandedGroups, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, onCreate, pendingAction }) {
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
    <div className="grid gap-4">
      <div className="border border-gray-200 rounded-lg bg-white min-h-[52px] px-4 py-2.5 flex items-center justify-between gap-3 max-md:flex-col max-md:items-stretch">
        <div className="min-w-[240px] w-[min(380px,100%)] h-[34px] border border-gray-200 rounded-md flex items-center gap-2 px-2.5 bg-white text-gray-400 focus-within:border-gray-900 focus-within:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all max-md:w-full max-md:min-w-0">
          <Search size={15} />
          <input className="w-full border-0 outline-0 bg-transparent text-[13px] text-gray-900 placeholder:text-gray-400" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索实例组、节点..." />
        </div>
        <button className="h-[32px] rounded-md px-3 bg-gray-900 text-white font-medium text-[12px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer" onClick={onCreate}>
          <Plus size={14} /> 创建实例
        </button>
      </div>
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] border-collapse">
            <thead>
              <tr>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">实例名称 / 组</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">类别</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">接入方式</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">引擎 / 拓扑</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">节点</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">接入点</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">内存</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">状态</th>
                <th className="px-4 py-2.5 border-b border-gray-200 text-right text-xs font-medium text-gray-400 bg-gray-50">操作</th>
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
                  publicHost={publicHost}
                  sentinelPort={sentinelPort}
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

function InstanceGroupRows({ group, expanded, onToggle, publicHost, sentinelPort, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, pendingAction }) {
  const master = group.master;
  const hasReplicas = group.replicas.length > 0;
  const topology = topologyLabel(group, master);
  const isStandalone = !hasReplicas;
  const category = group.category ? categoryLabel(group.category) : '';

  const powerAction = master ? powerActionFor(master, group) : null;

  return (
    <>
      <tr className={`border-b border-gray-200 last:border-b-0 ${hasReplicas ? 'cursor-pointer hover:bg-gray-50' : 'hover:bg-gray-50'}`} onClick={hasReplicas ? onToggle : undefined}>
        <td className="px-4 py-2.5 text-[13px]">
          <div className="flex items-center gap-2.5">
            {hasReplicas ? (
              <button className="w-5 h-5 rounded inline-flex items-center justify-center bg-transparent text-gray-400 border-0 cursor-pointer hover:text-gray-900 transition-colors" type="button">
                {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </button>
            ) : (
              <span className="w-5" />
            )}
            <div>
              <CopyableText text={group.groupName} className="font-semibold text-gray-900" />
              {(hasReplicas || !master || master.name !== group.groupName) && (
                <span className="text-[11px] text-gray-400 block mt-0.5">
                  {master ? `主库: ${master.name}` : '主库状态异常'}
                </span>
              )}
            </div>
          </div>
        </td>
        <td className="px-4 py-2.5 text-[13px] text-gray-500">{category}</td>
        <td className="px-4 py-2.5 text-[13px] text-gray-500">{group.access_mode === 'rw-split' ? '读写分离' : '标准'}</td>
        <td className="px-4 py-2.5 text-[13px] text-gray-700">
          {engineLabel(group)}
          <span className="text-[11px] text-gray-400 block mt-0.5">{topology}</span>
        </td>
        <td className="px-4 py-2.5 text-[13px] text-gray-500 font-mono">{master ? getField(master, 'server', 'Server', '-') : '-'}</td>
        <td className="px-4 py-2.5 text-[13px]"><ProxyPorts group={group} publicHost={publicHost} /></td>
        <td className="px-4 py-2.5 text-[13px]">{master ? <ResourceValue memory={group.resource.memory} memoryUsed={group.resource.memoryUsed} /> : '-'}</td>
        <td className="px-4 py-2.5 text-[13px]"><StatusBadge status={group.status} /></td>
        <td className="px-4 py-2.5 text-[13px]">
          {isStandalone && master && (
            <div className="flex justify-end gap-1">
              <IconButton icon={Info} label="详情与指标" onClick={() => onOpenDetail(master, group)} />
              <IconButton icon={HardDrive} label="备份列表" onClick={() => onOpenBackups(master, group)} />
              <MoreMenu items={[
                { label: '接入信息', onClick: () => onToggleAccessMode(group, null) },
                { label: '修改配置', onClick: () => onConfig(master) },
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

function CopyableText({ text, className = '', children }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(event) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {}
  }

  return (
    <span className={`inline-flex items-center gap-1 group ${className}`}>
      {children || <span>{text}</span>}
      <button
        type="button"
        className="w-4 h-4 inline-flex items-center justify-center text-gray-300 hover:text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer border-0 bg-transparent p-0"
        onClick={handleCopy}
        title="复制"
      >
        {copied ? <Check size={12} className="text-emerald-500" /> : <Copy size={12} />}
      </button>
    </span>
  );
}

function CopyableEndpoint({ host, port, className = '' }) {
  const [copied, setCopied] = useState(false);
  const text = host ? `${host}:${port}` : String(port);

  async function handleCopy(event) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {
      // 剪贴板权限不可用时静默失败，不影响页面其他功能
    }
  }

  return (
    <span className={`inline-flex items-center gap-1 group ${className}`}>
      <span className="font-mono tabular-nums">{text}</span>
      <button
        type="button"
        className="w-4 h-4 inline-flex items-center justify-center text-gray-300 hover:text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer border-0 bg-transparent p-0"
        onClick={handleCopy}
        title="复制连接地址"
      >
        {copied ? <Check size={12} className="text-emerald-500" /> : <Copy size={12} />}
      </button>
    </span>
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

function ResourceValue({ memory, memoryUsed }) {
  const totalBytes = memory * 1024 * 1024 * 1024;
  const pct = totalBytes > 0 && memoryUsed > 0 ? (memoryUsed / totalBytes) * 100 : 0;
  const usedLabel = memoryUsed > 0 ? formatBytes(memoryUsed) : '0MB';
  const totalLabel = formatBytes(totalBytes);
  const tone = pct > 80 ? 'bg-red-500' : pct > 60 ? 'bg-amber-500' : 'bg-emerald-500';
  return (
    <div className="flex flex-col gap-0.5 min-w-[100px]">
      <div className="h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-300 ${tone}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span className="font-mono tabular-nums text-[11px] text-gray-500">{usedLabel} / {totalLabel}</span>
    </div>
  );
}

function InstanceRow({ item, group, role, onOpenDetail, onOpenBackups, onPowerAction, onDelete, onSlowlog, onBigkeys, onConfig, onToggleAccessMode, pendingAction }) {
  const powerAction = powerActionFor(item, group);
  const isReplica = role === '从库';
  const isMasterWithReplicas = role === '主库' && group.replicas.length > 0;

  return (
    <tr className="border-b border-gray-200 last:border-b-0 hover:bg-gray-50 bg-gray-50/50">
      <td className="px-4 py-2.5 text-[13px]">
        <div className="flex items-center gap-2.5 pl-8">
          {isReplica && <span className="w-3.5 h-3.5 border-l border-b border-gray-300 rounded-bl shrink-0 -translate-y-0.5" />}
          <CopyableText text={item.name} className="font-medium" />
        </div>
      </td>
      <td className="px-4 py-2.5 text-[13px]" />
      <td className="px-4 py-2.5 text-[13px]" />
      <td className="px-4 py-2.5 text-[13px]">
        <span className={`text-[11px] font-medium px-1.5 py-0.5 rounded ${isReplica ? 'bg-gray-100 text-gray-500' : 'bg-gray-900 text-white'}`}>{role}</span>
      </td>
      <td className="px-4 py-2.5 text-[13px] text-gray-500 font-mono">{getField(item, 'server', 'Server', '-')}</td>
      <td className="px-4 py-2.5 text-[13px]" />
      <td className="px-4 py-2.5 text-[13px]">        {(() => {
          const used = Number(getField(item, 'memory_used', 'MemoryUsed', 0) || 0);
          const totalGi = parseGi(getField(item, 'memory', 'Memory'));
          const totalBytes = totalGi * 1024 * 1024 * 1024;
          return <ResourceValue memory={totalGi} memoryUsed={used} />;
        })()}
      </td>
      <td className="px-4 py-2.5 text-[13px]"><StatusBadge status={getField(item, 'status', 'Status')} /></td>
      <td className="px-4 py-2.5 text-[13px]">
        <div className="flex justify-end gap-1">
          <IconButton icon={Info} label="详情与指标" onClick={() => onOpenDetail(item, group)} />
          <IconButton icon={HardDrive} label="备份列表" onClick={() => onOpenBackups(item, group)} />
          <MoreMenu items={[
            !isReplica && { label: '接入信息', onClick: () => onToggleAccessMode(group, null) },
            { label: '修改配置', onClick: () => onConfig(item) },
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

function AlertsView({ groups, servers, publicHost }) {
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

function ServersView({ servers }) {
  if (servers.length === 0) {
    return <section className="border border-gray-200 rounded-lg bg-white"><EmptyState icon={Server} title="暂无服务器" detail="节点会从 pool-state.yaml 展示。" /></section>;
  }

  return (
    <div className="grid grid-cols-3 gap-4 max-lg:grid-cols-2 max-md:grid-cols-1">
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
    <section className="border border-gray-200 rounded-lg bg-white p-4">
      <div className="flex justify-between items-start gap-3">
        <div>
          <h3 className="m-0 mb-1 text-base font-semibold">{server.name}</h3>
          <code className="text-gray-400 font-mono text-xs">{getField(server, 'endpoint', 'Endpoint', '-')}</code>
        </div>
        <StatusBadge status={getField(server, 'status', 'Status')} />
      </div>
      <div className="mt-3 flex flex-wrap gap-1.5">
        {Object.keys(labels).length === 0 ? <span className="rounded-md px-2 py-0.5 bg-gray-100 text-gray-500 text-xs font-medium border border-gray-200">无标签</span> :
          Object.entries(labels).map(([key, value]) => <span className="rounded-md px-2 py-0.5 bg-gray-100 text-gray-500 text-xs font-medium border border-gray-200" key={key}>{key}: {value}</span>)}
      </div>
      <div className="mt-4 border-t border-gray-200 pt-4 grid gap-3.5">
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
      <div className="mt-3 text-xs text-gray-400">实例: {instances.length ? instances.join(', ') : '暂无'}</div>
    </section>
  );
}

function AuditView({ filters, setFilters, request }) {
  const [records, setRecords] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const pageSize = 30;

  async function fetchAudit(p = page) {
    setLoading(true);
    try {
      const data = await request(auditQuery(filters, p, pageSize));
      setRecords(data?.records || data?.Records || []);
      setTotal(data?.total || 0);
    } catch { /* ignore */ }
    setLoading(false);
  }

  useEffect(() => { fetchAudit(1); setPage(1); }, []);

  function updateFilter(key, value) {
    setFilters((current) => ({ ...current, [key]: value }));
  }
  function resetFilters() {
    setFilters({ from: '', to: '', group: '', instance: '', level: '', action: '' });
  }
  function handleApply() {
    setPage(1);
    fetchAudit(1);
  }
  function goPage(p) {
    setPage(p);
    fetchAudit(p);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="grid gap-4">
      <section className="border border-gray-200 rounded-lg bg-white p-4">
        <div className="grid grid-cols-6 gap-3 max-lg:grid-cols-2 max-md:grid-cols-1">
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>开始日期</span>
            <input type="date" className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.from} onChange={(event) => updateFilter('from', event.target.value)} />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>结束日期</span>
            <input type="date" className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.to} onChange={(event) => updateFilter('to', event.target.value)} />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>实例组</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.group} onChange={(event) => updateFilter('group', event.target.value)} placeholder="group" />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>实例</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.instance} onChange={(event) => updateFilter('instance', event.target.value)} placeholder="instance" />
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>级别</span>
            <select className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.level} onChange={(event) => updateFilter('level', event.target.value)}>
              <option value="">全部</option>
              <option value="normal">常规</option>
              <option value="important">重要</option>
              <option value="critical">关键</option>
            </select>
          </label>
          <label className="grid gap-1.5 text-xs font-medium text-gray-400">
            <span>动作</span>
            <input className="w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] transition-all" value={filters.action} onChange={(event) => updateFilter('action', event.target.value)} placeholder="instance.start" />
          </label>
        </div>
        <div className="mt-3.5 flex justify-end gap-2 max-md:flex-col max-md:items-stretch">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer" onClick={() => { resetFilters(); setTimeout(handleApply, 0); }}>重置</button>
          <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-700 transition-colors border-0 cursor-pointer" onClick={handleApply}>
            <SlidersHorizontal size={16} /> 应用筛选
          </button>
        </div>
      </section>
      <section className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="min-h-[54px] px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
          <h3 className="m-0 text-[15px] font-semibold">审计记录</h3>
          <span className="text-xs text-gray-400">共 {total} 条</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] border-collapse">
            <thead>
              <tr>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">时间</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">操作者</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">动作</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">目标</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">级别</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">结果</th>
                <th className="px-4 py-3 border-b border-gray-200 text-left text-xs font-medium text-gray-400 bg-gray-50">详情</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan="7" className="px-4 py-8 text-center text-gray-400 text-[13px]">加载中...</td></tr>
              ) : records.length === 0 ? (
                <tr><td colSpan="7"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
              ) : records.map((log, index) => {
                const id = log.id || log.ID || index;
                const params = log.params || log.Params;
                const target = log.target || log.Target;
                const detail = log.detail || log.Detail;
                const paramsText = params && Object.keys(params).length > 0
                  ? Object.entries(params).map(([k, v]) => `${k}=${v}`).join(', ')
                  : detail || '-';
                return (
                  <tr key={id} className="border-b border-gray-200 last:border-b-0">
                    <td className="px-4 py-3 text-[13px] font-mono tabular-nums">{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditOperator(log)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditActionLabel(log.action || log.Action)}</td>
                    <td className="px-4 py-3 text-[13px] font-mono">{auditTargetDisplay(target || log.instance || log.Instance)}</td>
                    <td className="px-4 py-3 text-[13px]">{auditLevelLabel(log.level || log.Level)}</td>
                    <td className="px-4 py-3 text-[13px]"><StatusBadge status={log.result || log.Result || 'success'} metaFn={auditResultMeta} /></td>
                    <td className="px-4 py-3 text-[13px] text-gray-500 font-mono max-w-[280px] truncate" title={paramsText}>{paramsText}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
            <span className="text-xs text-gray-400">第 {page}/{totalPages} 页</span>
            <div className="flex items-center gap-1">
              <button
                className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
                disabled={page <= 1}
                onClick={() => goPage(page - 1)}
              >上一页</button>
              <button
                className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
                disabled={page >= totalPages}
                onClick={() => goPage(page + 1)}
              >下一页</button>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}

function AuditCard({ log }) {
  const result = log.result || log.Result || 'success';
  const isBad = auditResultMeta(result).tone === 'bad';
  const Icon = isBad ? AlertCircle : CheckCircle2;
  return (
    <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-3 px-4 py-3 border-b border-gray-200 last:border-b-0 hover:bg-gray-50 transition-colors">
      <Icon size={18} className={isBad ? 'text-red-600 mt-px' : 'text-emerald-600 mt-px'} />
      <div className="min-w-0">
        <strong className="block text-[13px] font-medium truncate">{auditActionLabel(log.action || log.Action)}</strong>
        <span className="block mt-0.5 text-xs text-gray-400">{auditTargetDisplay(log.target || log.Target)} · {auditOperator(log)}</span>
      </div>
      <time className="text-xs text-gray-400 font-mono whitespace-nowrap">{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</time>
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
      chartInstances.current.forEach((c) => c.dispose());
      chartInstances.current = [];

      const ts = points.map((p) => new Date(p.ts * 1000));
      const charts = [
        { id: 'mem', title: '内存使用率 (%)', data: points.map((p) => p.mem_max > 0 ? +(p.mem_used / p.mem_max * 100).toFixed(1) : 0), color: '#3b82f6', max: 100 },
        { id: 'qps', title: 'QPS (ops/s)', data: points.map((p) => p.ops_sec), color: '#3b82f6' },
        { id: 'hit', title: '命中率 (%)', data: points.map((p) => { const total = p.hits + p.misses; return total > 0 ? +((p.hits / total) * 100).toFixed(1) : 100; }), color: '#3b82f6', max: 100 },
        { id: 'conns', title: '连接数', data: points.map((p) => p.conns), color: '#3b82f6' },
        { id: 'frag', title: '内存碎片率', data: points.map((p) => p.frag_ratio ? +p.frag_ratio.toFixed(2) : 0), color: '#3b82f6' },
        { id: 'repl', title: '主从同步状态', data: points.map((p) => p.repl_link !== undefined ? p.repl_link : 1), color: '#3b82f6', max: 1 },
        { id: 'offset', title: '复制偏移量', data: points.map((p) => p.repl_offset || 0), color: '#3b82f6' },
        { id: 'keys', title: 'Key 数量', data: points.map((p) => p.total_keys || 0), color: '#3b82f6' }
      ];

      charts.forEach(({ id, title, data, color, max }) => {
        const el = chartRefs.current[id];
        if (!el) return;
        const chart = echarts.init(el);
        chart.setOption({
          title: { text: title, textStyle: { fontSize: 13, fontWeight: 500 }, left: 'center' },
          tooltip: { trigger: 'axis', formatter: (params) => { const p = params[0]; return `${p.axisValueLabel}<br/>${p.marker} ${p.value}`; } },
          grid: { left: 45, right: 12, top: 35, bottom: 28 },
          xAxis: { type: 'category', data: ts.map((d) => d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })), boundaryGap: false, axisLine: { lineStyle: { color: '#ddd' } }, axisLabel: { fontSize: 10, color: '#999' } },
          yAxis: { type: 'value', min: 0, max: max || undefined, splitLine: { lineStyle: { color: '#f0f0f0' } }, axisLabel: { fontSize: 10, color: '#999' } },
          series: [{ type: 'line', data, smooth: true, symbol: 'none', lineStyle: { width: 2, color }, areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: color + '30' }, { offset: 1, color: color + '05' }] } } }]
        });
        chartInstances.current.push(chart);
      });

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
      <div className="flex items-center gap-1.5 mb-3">
        {rangeOptions.map((opt) => (
          <button key={opt.value} className={`h-7 px-2.5 rounded-md border text-xs font-medium cursor-pointer transition-colors ${range_ === opt.value ? 'bg-gray-900 text-white border-gray-900' : 'bg-white text-gray-700 border-gray-200 hover:bg-gray-50'}`} onClick={() => setRange(opt.value)}>
            {opt.label}
          </button>
        ))}
      </div>
      {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载趋势数据...</div> :
       error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> :
       (!points || points.length === 0) ? <div className="text-center p-8 text-gray-400 text-sm">暂无历史数据，Agent 启动后每分钟采集一次</div> : (
        <div className="grid grid-cols-2 gap-4 mt-3">
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.mem = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.qps = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.hit = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.conns = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.frag = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.repl = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.offset = el; }}></div>
          <div className="h-[200px] overflow-hidden" ref={(el) => { chartRefs.current.keys = el; }}></div>
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">{item.name}</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{group.groupName} · {engineLabel(group)}</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          <div className="flex gap-0 border-b border-gray-200 -mx-4 px-4 -mt-4">
            <button className={`px-4 py-2.5 text-[13px] border-0 bg-transparent cursor-pointer border-b-2 mb-[-1px] ${activeTab === 'trend' ? 'text-gray-900 border-gray-900 font-medium' : 'text-gray-400 border-transparent hover:text-gray-700'}`} onClick={() => setActiveTab('trend')}>趋势</button>
            <button className={`px-4 py-2.5 text-[13px] border-0 bg-transparent cursor-pointer border-b-2 mb-[-1px] ${activeTab === 'detail' ? 'text-gray-900 border-gray-900 font-medium' : 'text-gray-400 border-transparent hover:text-gray-700'}`} onClick={() => setActiveTab('detail')}>详情</button>
          </div>
          {activeTab === 'detail' && (
            <>
              <section>
                <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Info size={16} className="text-gray-400" /> 实例详情</h4>
                <div className="grid grid-cols-3 gap-2.5 max-lg:grid-cols-2 max-md:grid-cols-1">
                  {fields.map(([label, value]) => (
                    <div key={label} className="min-w-0 border border-gray-200 rounded-md p-3 bg-gray-50">
                      <span className="block text-xs font-medium text-gray-400">{label}</span>
                      <strong className="block mt-1 text-[13px] font-medium break-all">{displayValue(value)}</strong>
                    </div>
                  ))}
                </div>
              </section>
              <section>
                <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><BarChart3 size={16} className="text-gray-400" /> 运行指标</h4>
                {loading ? (
                  <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取 metrics...</div>
                ) : error ? (
                  <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>
                ) : (
                  <>
                    <div className="grid grid-cols-3 gap-2.5 max-lg:grid-cols-2 max-md:grid-cols-1">
                      {metricsSummary.map(([label, value]) => (
                        <div key={label} className="min-w-0 border border-gray-200 rounded-md p-3 bg-gray-50">
                          <span className="block text-xs font-medium text-gray-400">{label}</span>
                          <strong className="block mt-1 text-[13px] font-mono font-normal break-all">{displayValue(value)}</strong>
                        </div>
                      ))}
                    </div>
                    <details className="mt-3 border border-gray-200 rounded-md bg-gray-50 overflow-hidden">
                      <summary className="px-3 py-2.5 cursor-pointer text-[13px] font-medium text-gray-500 hover:text-gray-900">查看 INFO 原文</summary>
                      <pre className="max-h-[320px] m-0 px-3 pb-3 overflow-auto font-mono text-xs leading-relaxed text-gray-500 whitespace-pre-wrap">{info || '无 metrics 数据'}</pre>
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">慢查询</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载慢查询...</div> :
           error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> :
           (!entries || entries.length === 0) ? <div className="text-center p-8 text-gray-400 text-sm">无慢查询记录</div> : (
            <section>
              <div className="flex items-center justify-between mb-3">
                <span className="text-xs text-gray-400">{entries.length} 条记录</span>
                <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors" onClick={reload}><RefreshCw size={14} /> 刷新</button>
              </div>
              <table className="w-full border-collapse text-[13px]">
                <thead>
                  <tr>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">时间</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">耗时</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">命令</th>
                    <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">客户端</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((e, i) => (
                    <tr key={i} className="hover:bg-gray-50">
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '-'}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.duration_us >= 1000 ? `${(e.duration_us / 1000).toFixed(1)}ms` : `${e.duration_us}µs`}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top max-w-[320px] overflow-hidden text-ellipsis whitespace-nowrap">{e.command || '-'}</td>
                      <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 align-top">{e.client || '-'}</td>
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">大 Key 分析</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          <div className="flex items-center justify-between">
            <span className="text-xs text-gray-400">阈值: string &gt; 100KB，集合类 &gt; 10000 元素</span>
            <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors disabled:opacity-50" onClick={run} disabled={loading}>
              {loading ? <><RefreshCw size={14} className="animate-spin" /> 扫描中...</> : <><Play size={14} /> 开始扫描</>}
            </button>
          </div>
          {error && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>}
          {meta && (
            <div className="text-xs text-gray-400">
              已扫描 {meta.scanned} 个 key，耗时 {meta.elapsed_ms}ms
              {meta.timeout && <span className="text-amber-500"> （达到 60s 上限，结果可能不完整）</span>}
            </div>
          )}
          {keys && keys.length > 0 && (
            <table className="w-full border-collapse text-[13px]">
              <thead>
                <tr>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">Key</th>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">类型</th>
                  <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">大小</th>
                </tr>
              </thead>
              <tbody>
                {keys.map((k, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="px-2.5 py-1.5 font-mono border-b border-gray-100 max-w-[320px] overflow-hidden text-ellipsis whitespace-nowrap">{k.key}</td>
                    <td className="px-2.5 py-1.5 border-b border-gray-100">{k.type}</td>
                    <td className="px-2.5 py-1.5 font-mono border-b border-gray-100">{k.size_desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {keys && keys.length === 0 && !loading && <div className="text-center p-8 text-gray-400 text-sm">未发现大 Key</div>}
        </div>
      </section>
    </div>
  );
}

function AccessModeModal({ group, publicHost, sentinelPort, onConfirm, onClose }) {
  const [mode, setMode] = useState(group.access_mode || 'standard');
  const proxy = getField(group, 'proxy', 'Proxy', {}) || {};
  const masterPort = proxy.master_port || proxy.MasterPort;
  const autoPort = proxy.auto_port || proxy.AutoPort;
  const isRwSplit = group.access_mode === 'rw-split';
  const directEndpoint = publicHost ? `${publicHost}:${isRwSplit ? (autoPort || masterPort) : (masterPort || autoPort)}` : (isRwSplit ? autoPort : masterPort) || '-';
  const sentinelEndpoint = publicHost && sentinelPort ? `${publicHost}:${sentinelPort}` : null;

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(500px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">接入信息</h3><span className="block mt-0.5 text-xs text-gray-400">{group.groupName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="p-4 space-y-4">
          {/* 接入点信息 */}
          <div className="space-y-2.5">
            <h4 className="m-0 text-[13px] font-medium text-gray-700">Redis 直连</h4>
            <div className="bg-gray-50 rounded-md px-3 py-2">
              <CopyableText text={directEndpoint} className="text-[13px] font-mono text-gray-900" />
            </div>
          </div>

          {sentinelEndpoint && (
            <div className="space-y-2.5">
              <h4 className="m-0 text-[13px] font-medium text-gray-700">Sentinel 模式</h4>
              <div className="bg-gray-50 rounded-md px-3 py-2 space-y-2">
                <div>
                  <span className="text-[11px] text-gray-400 block mb-0.5">Sentinel 地址</span>
                  <CopyableText text={sentinelEndpoint} className="text-[13px] font-mono text-gray-900" />
                </div>
                <div>
                  <span className="text-[11px] text-gray-400 block mb-0.5">Master Name</span>
                  <CopyableText text={group.groupName} className="text-[13px] font-mono text-gray-900" />
                </div>
              </div>
            </div>
          )}

          {/* 接入方式切换 */}
          <div className="pt-3 border-t border-gray-100">
            <h4 className="m-0 mb-2.5 text-[13px] font-medium text-gray-700">接入方式</h4>
            <label className="flex items-center gap-2 mb-2 cursor-pointer">
              <input type="radio" name="access_mode" value="standard" checked={mode === 'standard'} onChange={() => setMode('standard')} />
              <span className="text-[13px]"><strong className="font-medium">标准</strong> — 所有流量走主库</span>
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="radio" name="access_mode" value="rw-split" checked={mode === 'rw-split'} onChange={() => setMode('rw-split')} />
              <span className="text-[13px]"><strong className="font-medium">读写分离</strong> — 读请求分发到从库</span>
            </label>
            {mode !== (group.access_mode || 'standard') && (() => {
              const newPort = mode === 'rw-split' ? (autoPort || masterPort) : (masterPort || autoPort);
              const newEndpoint = publicHost ? `${publicHost}:${newPort}` : newPort;
              return newPort ? (
                <p className="text-[12px] text-amber-600 bg-amber-50 border border-amber-200 rounded px-2.5 py-1.5 mt-2 mb-0">
                  切换后接入端口将变更为 <span className="font-mono font-medium">{newEndpoint}</span>
                </p>
              ) : null;
            })()}
            <div className="mt-3 flex justify-end gap-2">
              <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onClose}>关闭</button>
              <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50" disabled={mode === (group.access_mode || 'standard')} onClick={() => onConfirm(mode)}>切换模式</button>
            </div>
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
      setConfig((prev) => ({ ...prev, ...edits }));
      setEdits({});
    } catch (err) {
      setSaveResult(`保存失败: ${err.message || String(err)}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(720px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div><h3 className="m-0 text-base font-semibold">修改配置</h3><span className="block mt-0.5 text-xs text-gray-400">{instanceName}</span></div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {loading ? <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在加载配置...</div> :
           error ? <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div> : (
            <>
              <div className="flex items-center justify-between gap-3">
                <input className="flex-1 max-w-[300px] h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="搜索配置项..." />
                <span className="text-xs text-gray-400">{filteredKeys.length} 项</span>
                {hasEdits && (
                  <button className="h-7 px-2.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-50 cursor-pointer transition-colors" onClick={handleSave} disabled={saving}>
                    {saving ? '保存中...' : `保存 ${Object.keys(edits).length} 项修改`}
                  </button>
                )}
              </div>
              {saveResult && <div className="border border-gray-200 rounded-md px-3 py-2 text-[13px] text-gray-600">{saveResult}</div>}
              <div className="overflow-auto max-h-[480px]">
                <table className="w-full border-collapse text-[13px]">
                  <thead>
                    <tr>
                      <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs w-[40%]">配置项</th>
                      <th className="text-left px-2.5 py-1.5 font-medium text-gray-400 border-b border-gray-200 text-xs">值</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredKeys.map((key) => {
                      const current = edits[key] !== undefined ? edits[key] : config[key];
                      const modified = edits[key] !== undefined;
                      return (
                        <tr key={key} className={modified ? 'bg-gray-900/5' : ''}>
                          <td className="px-2.5 py-1.5 font-mono border-b border-gray-100">{key}</td>
                          <td className="px-2.5 py-1.5 border-b border-gray-100">
                            <input
                              className={`w-full border bg-transparent px-1 py-0.5 font-mono text-xs rounded transition-colors focus:outline-none focus:border-gray-900 ${modified ? 'border-gray-900' : 'border-transparent'}`}
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">{item.name} 备份管理</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{group.groupName}</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          <section>
            <div className="flex items-center justify-between gap-3 mb-3">
              <h4 className="m-0 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><HardDrive size={16} className="text-gray-400" /> 可用备份</h4>
              <button
                className="h-7 px-2.5 rounded-md bg-gray-900 text-white text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50"
                onClick={handleExecBackup}
                disabled={execLoading}
              >
                {execLoading ? <><RefreshCw size={14} className="animate-spin" /> 备份中...</> : <><Play size={14} /> 立即备份</>}
              </button>
            </div>
            {execMsg && <div className={`border rounded-md px-3 py-2 flex items-center gap-2 text-[13px] mb-2 ${execMsg.includes('失败') ? 'border-red-200 bg-red-50 text-red-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'}`}><AlertCircle size={16} /> {execMsg}</div>}
            {isMaster && hasReplicas && <div className="text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2 mb-2">⚠️ 当前为主库，建议在从库上执行备份以避免影响主库性能。</div>}
            {loading ? (
              <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取备份列表...</div>
            ) : error ? (
              <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>
            ) : backups.length === 0 ? (
              <EmptyState icon={HardDrive} title="暂无备份" detail="该实例当前没有可用备份文件。" />
            ) : (
              <div className="grid gap-2">
                {backups.map((backup) => (
                  <div className="border border-gray-200 rounded-md p-3 grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 bg-gray-50" key={backup.name}>
                    <HardDrive size={18} className="text-gray-400" />
                    <div>
                      <strong className="block font-mono text-[13px] font-medium">{backup.time}</strong>
                      <span className="block mt-0.5 text-xs text-gray-400 font-mono">{backup.type} · {backup.size}</span>
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
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Calendar size={16} className="text-gray-400" /> 定时备份策略</h4>
            {scheduleLoading ? (
              <div className="min-h-[110px] border border-dashed border-gray-200 rounded-md grid place-items-center text-gray-400">正在读取策略...</div>
            ) : (
              <div className="grid gap-3">
                {scheduleError && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {scheduleError}</div>}
                {scheduleSaved && <div className="border border-emerald-200 rounded-md px-3 py-2 flex items-center gap-2 bg-emerald-50 text-emerald-700 text-[13px]"><CheckCircle2 size={16} /> 策略已保存</div>}
                <div className="grid grid-cols-2 gap-3">
                  <label className="block text-xs font-medium text-gray-400">Cron 表达式
                    <input
                      className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]"
                      value={schedule}
                      onChange={(e) => { setSchedule(e.target.value); setScheduleSaved(false); }}
                      placeholder="0 2 * * *（每天凌晨2点）"
                    />
                  </label>
                  <label className="block text-xs font-medium text-gray-400">保留天数
                    <input
                      className="mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]"
                      type="number"
                      min={1}
                      value={retention}
                      onChange={(e) => { setRetention(e.target.value); setScheduleSaved(false); }}
                    />
                  </label>
                </div>
                <div className="text-xs text-gray-400">
                  留空 Cron 表达式则关闭定时备份。常用: <code className="bg-gray-100 px-1 py-0.5 rounded text-[11px] font-mono">0 2 * * *</code>(每天2:00)、<code className="bg-gray-100 px-1 py-0.5 rounded text-[11px] font-mono">0 */6 * * *</code>(每6小时)
                </div>
                <div className="flex justify-end">
                  <button
                    className="h-7 px-2.5 rounded-md bg-gray-900 text-white text-xs font-medium inline-flex items-center gap-1 hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50"
                    onClick={handleSaveSchedule}
                    disabled={scheduleSaving}
                  >
                    {scheduleSaving ? <><RefreshCw size={14} className="animate-spin" /> 保存中...</> : <><Save size={14} /> 保存策略</>}
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onCancel}>
      <section className="w-[min(460px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="px-4 py-4 border-b border-gray-200 flex items-center gap-3">
          <div className="w-10 h-10 rounded-md grid place-items-center bg-red-50 text-red-600 shrink-0">
            <Trash2 size={20} />
          </div>
          <div>
            <h3 className="m-0 text-base font-semibold">确认删除实例</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="p-4">
          <p className="m-0 text-[13px] text-gray-900 leading-relaxed">
            将要删除实例 <strong className="font-semibold">{item.name}</strong>。此操作不可恢复，实例容器将被停止并移除。
          </p>
          <div className="mt-3 grid gap-1.5 text-[13px] text-gray-500">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
          <label className="flex items-center gap-2 mt-3.5 p-2.5 border border-gray-200 rounded-md bg-gray-50 text-[13px] text-gray-600 cursor-pointer">
            <input type="checkbox" checked={cleanData} onChange={(e) => onToggleClean(e.target.checked)} />
            同时删除数据文件（备份 + 数据目录）
          </label>
        </div>
        <div className="px-4 pb-4 pt-2 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onCancel} disabled={loading}>取消</button>
          <button className="h-[34px] rounded-md px-3 bg-red-600 text-white font-medium text-[13px] hover:bg-red-700 border-0 cursor-pointer transition-colors disabled:opacity-50" onClick={onConfirm} disabled={loading}>
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
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={onCancel}>
      <section className="w-[min(460px,100%)] border border-gray-200 rounded-xl bg-white shadow-lg overflow-hidden animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="px-4 py-4 border-b border-gray-200 flex items-center gap-3">
          <div className={`w-10 h-10 rounded-md grid place-items-center shrink-0 ${isStop ? 'bg-red-50 text-red-600' : 'bg-gray-100 text-gray-600'}`}>
            {isStop ? <Power size={20} /> : <Play size={20} />}
          </div>
          <div>
            <h3 className="m-0 text-base font-semibold">{title}</h3>
            <span className="block mt-0.5 text-xs text-gray-400">{item.name} · {group.groupName}</span>
          </div>
        </div>
        <div className="p-4">
          <p className="m-0 text-[13px] text-gray-900 leading-relaxed">
            将要{actionText}实例 <strong className="font-semibold">{item.name}</strong>。
            {isStop ? '停止后该实例上的 Redis 服务会不可用，副本冗余或读流量可能受影响。' : '启动会尝试恢复该实例容器。'}
          </p>
          <div className="mt-3 grid gap-1.5 text-[13px] text-gray-500">
            <span>角色: {displayValue(role)}</span>
            <span>当前状态: {statusMeta(status).label}</span>
            <span>节点: {displayValue(getField(item, 'server', 'Server'))}</span>
          </div>
        </div>
        <div className="px-4 pb-4 pt-2 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onCancel} disabled={loading}>取消</button>
          <button className={`h-[34px] rounded-md px-3 font-medium text-[13px] border-0 cursor-pointer transition-colors disabled:opacity-50 ${isStop ? 'bg-red-600 text-white hover:bg-red-700' : 'bg-gray-900 text-white hover:bg-gray-700'}`} onClick={onConfirm} disabled={loading}>
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

  const inputCls = "mt-1.5 w-full h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 focus:shadow-[0_0_0_3px_rgba(0,0,0,0.08)] text-[13px] transition-all disabled:opacity-50 disabled:cursor-not-allowed";
  const selectCls = inputCls;

  return (
    <div className="fixed inset-0 z-40 p-7 grid place-items-center bg-black/40 animate-[fadeIn_0.15s_ease-out]" role="presentation" onMouseDown={submitting ? undefined : onClose}>
      <section className="w-[min(980px,100%)] max-h-[min(860px,calc(100vh-56px))] rounded-xl border border-gray-200 overflow-hidden grid grid-rows-[auto_minmax(0,1fr)_auto] bg-white shadow-lg animate-[slideUp_0.18s_cubic-bezier(0.16,1,0.3,1)]" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()}>
        <div className="min-h-[62px] px-4 py-4 border-b border-gray-200 flex items-center justify-between gap-3">
          <div>
            <h3 className="m-0 text-base font-semibold">创建实例</h3>
            <span className="block mt-0.5 text-xs text-gray-400">新建 Redis / Kvrocks 实例</span>
          </div>
          <button className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 transition-colors" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="min-h-0 overflow-auto p-4 grid gap-4">
          {error && <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]"><AlertCircle size={16} /> {error}</div>}
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Info size={16} className="text-gray-400" /> 基本信息</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">{isReplica ? '实例组' : '名称'} <span className="text-red-500">*</span>
                {isReplica ? (
                  <select className={selectCls} value={form.group} onChange={(e) => update('group', e.target.value)}>
                    <option value="">选择组</option>
                    {replicaGroups.map((g) => <option key={g.groupName} value={g.groupName}>{g.groupName}</option>)}
                  </select>
                ) : (
                  <input className={inputCls} value={form.group} onChange={(e) => update('group', e.target.value)} placeholder="如 order" />
                )}
              </label>
              <label className="block text-xs font-medium text-gray-400">类别 <span className="text-red-500">*</span>
                <select className={selectCls} value={form.category} onChange={(e) => update('category', e.target.value)} disabled={isReplica}>
                  <option value="cache">缓存</option>
                  <option value="persistent">持久化</option>
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">引擎 <span className="text-red-500">*</span>
                <select className={selectCls} value={form.engine} onChange={(e) => update('engine', e.target.value)} disabled={isReplica}>
                  <option value="redis">Redis</option>
                  <option value="kvrocks">Kvrocks</option>
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">引擎版本
                <select className={selectCls} value={form.engine_version} onChange={(e) => update('engine_version', e.target.value)} disabled={isReplica}>
                  <option value="">默认{defaultVersion ? ` (${defaultVersion})` : ''}</option>
                  {currentVersions.map((v) => <option key={v} value={v}>{v}</option>)}
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">副本数量 <span className="text-red-500">*</span>
                <input className={inputCls} type="number" min={0} value={form.replicaCount} onChange={(e) => update('replicaCount', Math.max(0, Number(e.target.value) || 0))} />
              </label>
              <label className="block text-xs font-medium text-gray-400">接入方式
                <select className={selectCls} value={form.access_mode} onChange={(e) => update('access_mode', e.target.value)} disabled={isReplica || form.replicaCount === 0}>
                  <option value="standard">标准（全部走主库）</option>
                  <option value="rw-split">读写分离</option>
                </select>
              </label>
            </div>
            {replicaGroups.length > 0 && (
              <label className="flex items-center gap-2 mt-2.5 text-[13px] text-gray-500 cursor-pointer">
                <input type="checkbox" checked={form.joinExisting} onChange={(e) => update('joinExisting', e.target.checked)} disabled={submitting} />
                添加副本到已有组
              </label>
            )}
            {isReplica && <p className="mt-2 text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2">引擎 / 版本 / 类别将从主库自动继承。副本数量为本次新增的副本数。</p>}
            {!isReplica && <p className="mt-2 text-xs text-gray-500 bg-gray-100 rounded-md px-2.5 py-2">副本数量 0 = 单点，1+ = 主从。实例名自动生成为 {form.group || '组名'}-0、{form.group || '组名'}-1 …</p>}
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Database size={16} className="text-gray-400" /> 资源配置</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">内存 <span className="text-red-500">*</span>
                <div className="flex gap-1.5 mt-1.5">
                  <input className="flex-1 min-w-0 h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" type="number" value={form.memory} onChange={(e) => update('memory', e.target.value)} placeholder="如 2" />
                  <select className="shrink-0 w-16 h-9 border border-gray-200 rounded-md px-2 bg-white text-gray-900 outline-0 text-[13px]" value={form.memory_unit} onChange={(e) => update('memory_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                    <option value="Mi">Mi</option>
                  </select>
                </div>
              </label>
              <label className="block text-xs font-medium text-gray-400">CPU 核数
                <input className={inputCls} type="number" step="0.5" value={form.cpus} onChange={(e) => update('cpus', e.target.value)} placeholder="如 1" />
              </label>
              <label className="block text-xs font-medium text-gray-400">磁盘
                <div className="flex gap-1.5 mt-1.5">
                  <input className="flex-1 min-w-0 h-9 border border-gray-200 rounded-md px-2.5 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px]" type="number" value={form.disk} onChange={(e) => update('disk', e.target.value)} placeholder="如 10" />
                  <select className="shrink-0 w-16 h-9 border border-gray-200 rounded-md px-2 bg-white text-gray-900 outline-0 text-[13px]" value={form.disk_unit} onChange={(e) => update('disk_unit', e.target.value)}>
                    <option value="Gi">Gi</option>
                  </select>
                </div>
              </label>
            </div>
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><Server size={16} className="text-gray-400" /> 部署选项</h4>
            <div className="grid grid-cols-3 gap-3 max-md:grid-cols-1">
              <label className="block text-xs font-medium text-gray-400">目标节点
                <select className={selectCls} value={form.server} onChange={(e) => update('server', e.target.value)}>
                  <option value="">自动调度</option>
                  {servers.map((s) => <option key={s.name} value={s.name}>{s.name}</option>)}
                </select>
              </label>
              <label className="block text-xs font-medium text-gray-400">密码
                <input className={inputCls} type="password" value={form.password} onChange={(e) => update('password', e.target.value)} placeholder="留空则无密码" />
              </label>
            </div>
          </section>
          <section>
            <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><SlidersHorizontal size={16} className="text-gray-400" /> 高级</h4>
            <div className="grid grid-cols-1 gap-3 mt-3">
              <label className="block text-xs font-medium text-gray-400">配置覆盖
                <textarea className="mt-1.5 w-full border border-gray-200 rounded-md px-2.5 py-2 bg-white text-gray-900 outline-0 focus:border-gray-900 text-[13px] font-inherit resize-y min-h-[80px]" rows="4" value={form.config_overrides} onChange={(e) => update('config_overrides', e.target.value)} placeholder={'每行一个，格式 key=value\n如 maxmemory-policy=allkeys-lru'} />
              </label>
            </div>
          </section>
          {instances.length > 0 && !submitting && (
            <section>
              <h4 className="m-0 mb-3 inline-flex items-center gap-1.5 text-[13px] font-semibold text-gray-500"><CheckCircle2 size={16} className="text-gray-400" /> 将创建以下实例</h4>
              <div className="grid gap-1.5">
                {instances.map((inst) => (
                  <div key={inst.name} className="flex items-center gap-2.5 px-3 py-2 border border-gray-200 rounded-md bg-gray-50">
                    <code className="font-mono text-[13px] text-gray-900">{inst.name}</code>
                    <span className="ml-auto text-xs text-gray-400">{inst.role}</span>
                  </div>
                ))}
              </div>
            </section>
          )}
          {progress && (
            <div className="border border-gray-200 rounded-md px-3 py-2.5 flex items-center gap-2 text-[13px] text-gray-600"><RefreshCw size={16} className="animate-spin" /> 正在创建 {progress.name}（{progress.current}/{progress.total}）…</div>
          )}
        </div>
        <div className="px-4 pb-4 pt-3 border-t border-gray-200 flex justify-end gap-2">
          <button className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-900 font-medium text-[13px] hover:bg-gray-50 cursor-pointer transition-colors" onClick={onClose} disabled={submitting}>取消</button>
          <button className="h-[34px] rounded-md px-3 bg-gray-900 text-white font-medium text-[13px] hover:bg-gray-700 border-0 cursor-pointer transition-colors disabled:opacity-50" onClick={handleSubmit} disabled={submitting || instances.length === 0}>
            {submitting ? '创建中…' : `创建 ${instances.length} 个实例`}
          </button>
        </div>
      </section>
    </div>
  );
}
