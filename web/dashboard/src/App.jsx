import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertCircle,
  BarChart3,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  Clock,
  Cpu,
  Database,
  GitMerge,
  HardDrive,
  Info,
  LayoutDashboard,
  List,
  Menu,
  Play,
  Power,
  RefreshCw,
  Search,
  Server,
  SlidersHorizontal,
  ShieldCheck,
  X
} from 'lucide-react';

const TABS = [
  { id: 'dashboard', label: '大盘看板', icon: LayoutDashboard },
  { id: 'instances', label: '实例管理', icon: Database },
  { id: 'servers', label: '服务器节点', icon: Server },
  { id: 'audit', label: '操作审计', icon: List }
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
	return list.sort().reverse().map((name) => {
		const text = String(name);
		const timestamp = text.replace(/\.(rdb|aof|tar\.gz|checkpoint\.tar\.gz)$/i, '').replace(/\/$/, '');
		const extMatch = text.match(/\.([^.]+(?:\.tar\.gz)?)$/i);
		return {
			name: text,
			time: formatDateTime(timestamp, timestamp),
			type: extMatch ? extMatch[1].toUpperCase() : 'DIR'
		};
	});
}

function auditOperator(log) {
	return displayValue(log?.operator || log?.Operator, 'unknown');
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
    failed: '异常',
    unexpected_stopped: '异常停止',
    healthy: '健康',
    unhealthy: '异常',
    degraded: '拓扑降级',
    failover_conflict: '故障转移冲突',
    drain: '下线中',
    success: '成功',
    error: '失败'
  };
  const tone = {
    running: 'ok',
    healthy: 'ok',
    success: 'ok',
    creating: 'warn',
    drain: 'warn',
    stopped: 'warn',
    failed: 'bad',
    unexpected_stopped: 'bad',
    unhealthy: 'bad',
    degraded: 'warn',
    failover_conflict: 'bad',
    error: 'bad'
  };
  return { label: labels[status] || status || '未知', tone: tone[status] || 'neutral' };
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
				type: getField(groupState, 'type', 'Type', ''),
				currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
				envoy: getField(groupState, 'envoy', 'Envoy', null),
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
				type: getField(groupState, 'type', 'Type', ''),
				currentMaster: getField(groupState, 'current_master', 'CurrentMaster', ''),
				envoy: getField(groupState, 'envoy', 'Envoy', null),
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
			resource: { memory, disk, cpus }
		};
	});
}

function engineLabel(group) {
	const engine = displayValue(group.engine, '-');
	const version = displayValue(group.engineVersion, '');
	return version ? `${engine.toUpperCase()} ${version}` : engine.toUpperCase();
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
	return type === 'standalone' ? 'Standalone' : `1 Master / ${group.replicas.length} Replicas`;
}

function StatusBadge({ status, children }) {
  const meta = statusMeta(status);
  return <span className={`badge ${meta.tone}`}>{children || meta.label}</span>;
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

function ProgressBar({ label, current, max, unit = '', tone = 'blue' }) {
  const percent = max > 0 ? Math.min(100, Math.round((current / max) * 100)) : 0;
  return (
    <div className="progress">
      <div className="progress-label">
        <span>{label}</span>
        <span>{current || 0}{unit} / {max || 0}{unit}</span>
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
  const [token, setToken] = useState(() => localStorage.getItem('redisPilotToken') || '');
  const [query, setQuery] = useState('');
  const [expandedGroups, setExpandedGroups] = useState({});
  const [auditFilters, setAuditFilters] = useState({ from: '', to: '', group: '', instance: '', level: '', action: '' });
  const [detail, setDetail] = useState(null);
  const [backupDetail, setBackupDetail] = useState(null);
  const [confirmAction, setConfirmAction] = useState(null);
  const [pendingAction, setPendingAction] = useState('');
  const [data, setData] = useState({ instances: {}, servers: {}, audits: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const instances = useMemo(() => normalizeMap(data.instances, 'Instances', 'instances'), [data.instances]);
  const instanceGroups = useMemo(() => buildInstanceGroups(data.instances), [data.instances]);
  const servers = useMemo(() => normalizeMap(data.servers, 'Servers', 'servers'), [data.servers]);
  const activeTitle = TABS.find((tab) => tab.id === activeTab)?.label || '大盘看板';

  async function request(path, options = {}) {
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    const response = await fetch(path, {
      ...options,
      headers: {
        ...headers,
        'X-Operator': 'dashboard',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {})
      }
    });
    if (response.status === 401) throw new Error('接口鉴权失败，请输入 server.yaml 中配置的 token。');
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

  function saveToken() {
    localStorage.setItem('redisPilotToken', token.trim());
    refresh();
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
            <div className="health-pill"><span /> Server API</div>
            <input
              value={token}
              onChange={(event) => setToken(event.target.value)}
              className="token-input"
              type="password"
              placeholder="Bearer Token（如已配置）"
            />
            <button className="ghost-button" onClick={saveToken}>保存 Token</button>
            <button className="primary-button" onClick={refresh}>
              <RefreshCw size={16} /> 刷新
            </button>
          </div>
        </header>

        <section className="content">
          {error && <div className="notice"><AlertCircle size={16} /> {error}</div>}
          {loading ? (
            <div className="panel loading">正在读取 Server 状态...</div>
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
	                  pendingAction={pendingAction}
	                />
	              )}
              {activeTab === 'servers' && <ServersView servers={servers} />}
              {activeTab === 'audit' && (
                <AuditView
                  audits={data.audits}
                  filters={auditFilters}
                  setFilters={setAuditFilters}
                  onApply={refresh}
                />
              )}
            </>
          )}
        </section>
	      </main>
	      {detail && <InstanceDetailModal detail={detail} onClose={() => setDetail(null)} />}
	      {backupDetail && <BackupListModal detail={backupDetail} onClose={() => setBackupDetail(null)} />}
	      {confirmAction && (
	        <ConfirmPowerModal
	          detail={confirmAction}
	          loading={pendingAction === `${confirmAction.action}:${confirmAction.item.name}`}
	          onCancel={() => setConfirmAction(null)}
	          onConfirm={executeInstancePower}
	        />
	      )}
	    </div>
	  );
	}

function DashboardView({ groups, servers, audits }) {
  const healthyGroups = groups.filter((group) => group.status === 'healthy' || group.status === 'running').length;
  const alerts = groups.filter((group) => instanceStatusTone(group.status) === 'bad').length +
    servers.filter((item) => getField(item, 'status', 'Status') === 'unhealthy').length;

  return (
    <div className="stack">
      <div className="stat-grid">
        <StatCard label="总实例组数" value={groups.length} icon={Database} tone="blue" />
        <StatCard label="服务器节点" value={servers.length} icon={Server} tone="indigo" />
        <StatCard label="健康集群" value={healthyGroups} icon={ShieldCheck} tone="green" />
        <StatCard label="待处理告警" value={alerts} icon={AlertCircle} tone="red" />
      </div>

      <div className="dashboard-grid">
        <section className="panel">
          <div className="panel-header">
            <h3>实例运行状态概览</h3>
            <Activity size={20} />
          </div>
          <div className="panel-body list">
            {groups.length === 0 ? <EmptyState title="暂无实例组" detail="创建实例后会在这里展示运行状态。" /> :
              groups.slice(0, 8).map((group) => <InstanceGroupCard key={group.groupName} group={group} />)}
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h3>审计记录</h3>
            <Clock size={20} />
          </div>
          <div className="panel-body audit-list">
            {(audits || []).slice(0, 4).map((log, index) => (
              <AuditCard key={log.id || log.ID || index} log={log} />
            ))}
            {(!audits || audits.length === 0) && <EmptyState icon={List} title="暂无审计记录" />}
          </div>
        </section>
      </div>
    </div>
  );
}

function StatCard({ label, value, icon: Icon, tone }) {
  return (
    <section className="panel stat-card">
      <div className={`stat-icon ${tone}`}><Icon size={24} /></div>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </section>
  );
}

function InstanceGroupCard({ group }) {
  const master = group.master;
  const masterServer = master ? getField(master, 'server', 'Server', '-') : '';
  const topology = topologyLabel(group, master).startsWith('Standalone') ? '单点' : '主从';
  return (
    <div className="instance-card">
      <div className={`status-dot ${statusMeta(group.status).tone}`} />
      <div className="card-main">
        <strong>{group.groupName}</strong>
        <span>{master ? `主库: ${masterServer}` : '无主节点'}{group.replicas.length > 0 ? ` · 从库: ${group.replicas.length} 个` : ''}</span>
      </div>
      <div className="card-side">
        <StatusBadge status={group.status} />
        <span><GitMerge size={12} /> {engineLabel(group)} · {topology}</span>
      </div>
    </div>
  );
}

function InstancesView({ groups, query, setQuery, expandedGroups, setExpandedGroups, onOpenDetail, onOpenBackups, onPowerAction, pendingAction }) {
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
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索实例组、Server 或端口..." />
        </div>
        <button className="primary-button" disabled>创建实例</button>
      </div>
      <section className="panel table-panel">
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>实例名称 / 组</th>
                <th>引擎版本</th>
                <th>拓扑角色</th>
                <th>Server</th>
                <th>端口</th>
                <th>资源</th>
                <th>状态</th>
                <th className="right">操作</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr><td colSpan="8"><EmptyState title="暂无实例" /></td></tr>
              ) : filtered.map((group) => (
                <InstanceGroupRows
                  key={group.groupName}
                  group={group}
                  expanded={!!expandedGroups[group.groupName]}
	                onToggle={() => toggleGroup(group.groupName)}
	                onOpenDetail={onOpenDetail}
	                onOpenBackups={onOpenBackups}
	                onPowerAction={onPowerAction}
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

function InstanceGroupRows({ group, expanded, onToggle, onOpenDetail, onOpenBackups, onPowerAction, pendingAction }) {
  const master = group.master;
  const hasReplicas = group.replicas.length > 0;
  const topology = topologyLabel(group, master);
  const isStandalone = topology === 'Standalone';
  const category = group.category ? ` · ${group.category}` : '';
  const powerAction = master ? powerActionFor(master, group) : null;

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
                <span className="role-chip">{isStandalone ? '单点' : 'Master'}</span>
                {category && <span className="role-chip subtle">{group.category}</span>}
              </div>
              <span className="muted">
                {master ? `当前主库: ${master.name} · ${getField(master, 'server', 'Server', '-')}${category}` : '主库状态异常'}
              </span>
            </div>
          </div>
        </td>
        <td><span className="engine">{engineLabel(group)}</span></td>
        <td>{topology}</td>
        <td>{master ? getField(master, 'server', 'Server', '-') : '-'}</td>
        <td>{master ? <EnvoyPorts group={group} item={master} /> : '-'}</td>
        <td>{master ? <ResourceValue memory={group.resource.memory} cpus={group.resource.cpus} disk={group.resource.disk} /> : '-'}</td>
        <td><StatusBadge status={group.status} /></td>
        <td>
          <div className="row-actions">
            {master && <IconButton icon={Info} label="详情与指标" onClick={(event) => {
              event.stopPropagation();
              onOpenDetail(master, group);
            }} />}
            {master && <IconButton icon={HardDrive} label="备份列表" onClick={(event) => {
              event.stopPropagation();
              onOpenBackups(master, group);
            }} />}
            {master && powerAction && <IconButton
              icon={powerAction.icon}
              label={powerAction.label}
              disabled={powerAction.disabledReason || pendingAction === `${powerAction.action}:${master.name}`}
              onClick={(event) => {
                event.stopPropagation();
                if (powerAction.disabledReason) return;
                onPowerAction(master, group, powerAction.action);
              }}
            />}
          </div>
        </td>
      </tr>
      {expanded && group.replicas.map((replica) => (
        <ReplicaRow
          key={replica.name}
          item={replica}
          group={group}
          onOpenDetail={onOpenDetail}
          onOpenBackups={onOpenBackups}
          onPowerAction={onPowerAction}
          pendingAction={pendingAction}
        />
      ))}
    </>
  );
}

function EnvoyPorts({ group, item }) {
  const envoy = getField(group, 'envoy', 'Envoy', {}) || {};
  const ports = [
    envoy.master_port || envoy.MasterPort ? `MASTER ${envoy.master_port || envoy.MasterPort}` : '',
    envoy.auto_port || envoy.AutoPort ? `AUTO ${envoy.auto_port || envoy.AutoPort}` : ''
  ].filter(Boolean).join(' / ');
  return (
    <>
      <span className="muted">Redis: {getField(item, 'port', 'Port', '-')}</span>
      <span className="muted">Envoy: {ports || '-'}</span>
    </>
  );
}

function ResourceValue({ memory, cpus, disk }) {
  return (
    <>
      <span>{formatGi(memory)} / {cpus || 0}C</span>
      <span className="muted">Disk: {formatGi(disk)}</span>
    </>
  );
}

function ReplicaRow({ item, group, onOpenDetail, onOpenBackups, onPowerAction, pendingAction }) {
  const powerAction = powerActionFor(item, group);

  return (
    <tr className="replica-row">
      <td>
        <div className="replica-cell">
          <span className="replica-branch" />
          <div>
            <strong>{item.name}</strong>
            <span className="muted">节点: {getField(item, 'server', 'Server', '-')}</span>
          </div>
        </div>
      </td>
      <td><span className="muted">{engineLabel(group)}</span></td>
      <td>Replica</td>
      <td>{getField(item, 'server', 'Server', '-')}</td>
      <td><span className="muted">通过主库 Envoy 路由读</span></td>
      <td>
        <span>{getField(item, 'memory', 'Memory', '-')} / {getField(item, 'cpus', 'CPUs', '-')}C</span>
        <span className="muted">Disk: {displayValue(getField(item, 'disk', 'Disk'), '0Gi')}</span>
      </td>
      <td><StatusBadge status={getField(item, 'status', 'Status')} /></td>
      <td>
        <div className="row-actions">
          <IconButton icon={Info} label="详情与指标" onClick={() => onOpenDetail(item, group)} />
          <IconButton icon={HardDrive} label="备份列表" onClick={() => onOpenBackups(item, group)} />
          {powerAction && <IconButton
            icon={powerAction.icon}
            label={powerAction.label}
            disabled={powerAction.disabledReason || pendingAction === `${powerAction.action}:${item.name}`}
            onClick={() => {
              if (powerAction.disabledReason) return;
              onPowerAction(item, group, powerAction.action);
            }}
          />}
        </div>
      </td>
    </tr>
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
      <div className="watermark"><Server size={104} /></div>
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
          tone="indigo"
        />
        <ProgressBar
          label={<><Database size={13} /> Memory</>}
          current={parseGi(allocated.memory || allocated.Memory)}
          max={parseGi(capacity.memory || capacity.Memory)}
          unit="Gi"
          tone="blue"
        />
        <ProgressBar
          label={<><HardDrive size={13} /> Disk</>}
          current={parseGi(allocated.disk || allocated.Disk)}
          max={parseGi(capacity.disk || capacity.Disk)}
          unit="Gi"
          tone="green"
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
              <option value="normal">normal</option>
              <option value="important">important</option>
              <option value="critical">critical</option>
            </select>
          </label>
          <label>
            <span>动作</span>
            <input value={filters.action} onChange={(event) => updateFilter('action', event.target.value)} placeholder="backup.exec" />
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
                <th>结果</th>
              </tr>
            </thead>
            <tbody>
              {records.length === 0 ? (
                <tr><td colSpan="5"><EmptyState icon={List} title="暂无审计记录" /></td></tr>
              ) : records.slice(0, 50).map((log, index) => (
                <tr key={log.id || log.ID || index}>
	                  <td>{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</td>
                  <td>{auditOperator(log)}</td>
                  <td>{displayValue(log.action || log.Action)}</td>
                  <td>{displayValue(log.target || log.Target || log.instance || log.Instance)}</td>
                  <td><StatusBadge status={log.result || log.Result || log.level || log.Level} /></td>
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
  const result = log.result || log.Result || log.level || log.Level || 'success';
  const Icon = statusMeta(result).tone === 'bad' ? AlertCircle : CheckCircle2;
  return (
    <div className="audit-card">
      <Icon size={18} />
      <div>
        <strong>{displayValue(log.action || log.Action)}</strong>
        <span>目标: {displayValue(log.target || log.Target || log.instance || log.Instance)} · 操作人: {auditOperator(log)}</span>
      </div>
      <time>{formatDateTime(log.time || log.Time || log.timestamp || log.Timestamp)}</time>
    </div>
  );
}

function InstanceDetailModal({ detail, onClose }) {
  const { item, group, loading, error, metrics } = detail;
  const payload = extractMetricsPayload(metrics);
  const info = payload.info || payload.Info || '';
  const sections = parseRedisInfo(info);
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
    ['Server', getField(item, 'server', 'Server', '-')],
    ['Redis 端口', getField(item, 'port', 'Port', '-')],
    ['资源', `${getField(item, 'memory', 'Memory', '-')} / ${getField(item, 'cpus', 'CPUs', '-')}C / Disk ${displayValue(getField(item, 'disk', 'Disk'), '0Gi')}`],
    ['数据目录', getField(item, 'data_path', 'DataPath', '-')],
    ['配置目录', getField(item, 'config_path', 'ConfigPath', '-')],
    ['备份目录', getField(item, 'backup_path', 'BackupPath', '-')],
    ['创建时间', formatDateTime(getField(item, 'created_at', 'CreatedAt', '-'))]
  ];

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3>{item.name}</h3>
            <span>{group.groupName} · {engineLabel(group)}</span>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
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
        </div>
      </section>
    </div>
  );
}

function BackupListModal({ detail, onClose }) {
  const { item, group, loading, error, backups } = detail;

  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3>{item.name} 备份列表</h3>
            <span>{group.groupName} · 只读查看</span>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭" type="button"><X size={18} /></button>
        </div>
        <div className="modal-body">
          <section>
            <h4><HardDrive size={16} /> 可用备份</h4>
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
                      <span>{backup.name}</span>
                    </div>
                    <span className="backup-type">{backup.type}</span>
                  </div>
                ))}
              </div>
            )}
          </section>
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
            <span>Server: {displayValue(getField(item, 'server', 'Server'))}</span>
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
