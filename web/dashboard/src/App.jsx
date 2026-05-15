import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertCircle,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  Clock,
  Cpu,
  Database,
  GitMerge,
  HardDrive,
  LayoutDashboard,
  List,
  Menu,
  RefreshCw,
  Search,
  Server,
  Settings,
  ShieldCheck,
  Trash2,
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
    error: 'bad'
  };
  return { label: labels[status] || status || '未知', tone: tone[status] || 'neutral' };
}

function instanceStatusTone(status) {
	return statusMeta(status).tone;
}

function groupStatus(instances) {
	if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'bad')) return 'failed';
	if (instances.some((item) => instanceStatusTone(getField(item, 'status', 'Status')) === 'warn')) return 'creating';
	if (instances.length > 0 && instances.every((item) => getField(item, 'status', 'Status') === 'running')) return 'running';
	return instances[0] ? getField(instances[0], 'status', 'Status') : 'unknown';
}

function buildInstanceGroups(instances) {
	const groups = new Map();
	for (const item of instances) {
		const groupName = getField(item, 'group', 'Group', item.name) || item.name;
		if (!groups.has(groupName)) {
			groups.set(groupName, {
				groupName,
				engine: getField(item, 'engine', 'Engine', '-'),
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
	return Array.from(groups.values()).map((group) => {
		const primary = group.master || group.instances[0] || null;
		return {
			...group,
			master: primary,
			replicas: group.instances.filter((item) => item.name !== primary?.name && getField(item, 'role', 'Role') !== 'master' && getField(item, 'role', 'Role') !== 'standalone'),
			status: groupStatus(group.instances),
			engine: primary ? getField(primary, 'engine', 'Engine', group.engine) : group.engine
		};
	});
}

function StatusBadge({ status, children }) {
  const meta = statusMeta(status);
  return <span className={`badge ${meta.tone}`}>{children || meta.label}</span>;
}

function IconButton({ icon: Icon, label }) {
  return (
    <button className="icon-button" disabled title={`${label}暂未开放`} aria-label={label}>
      <Icon size={16} />
    </button>
  );
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
  const [data, setData] = useState({ instances: {}, servers: {}, audits: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const instances = useMemo(() => normalizeMap(data.instances, 'Instances', 'instances'), [data.instances]);
  const instanceGroups = useMemo(() => buildInstanceGroups(instances), [instances]);
  const servers = useMemo(() => normalizeMap(data.servers, 'Servers', 'servers'), [data.servers]);
  const activeTitle = TABS.find((tab) => tab.id === activeTab)?.label || '大盘看板';

  async function request(path) {
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    const response = await fetch(path, { headers });
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
        request('/audit/query')
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
          request('/audit/query')
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
        <div className="sidebar-card">
          <strong>GAL Agent</strong>
          <span>Server 同端口只读看板</span>
        </div>
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
              {activeTab === 'dashboard' && <DashboardView instances={instances} servers={servers} audits={data.audits} />}
              {activeTab === 'instances' && (
                <InstancesView
                  groups={instanceGroups}
                  query={query}
                  setQuery={setQuery}
                  expandedGroups={expandedGroups}
                  setExpandedGroups={setExpandedGroups}
                />
              )}
              {activeTab === 'servers' && <ServersView servers={servers} />}
              {activeTab === 'audit' && <AuditView audits={data.audits} />}
            </>
          )}
        </section>
      </main>
    </div>
  );
}

function DashboardView({ instances, servers, audits }) {
  const groups = useMemo(() => buildInstanceGroups(instances), [instances]);
  const healthy = servers.filter((item) => getField(item, 'status', 'Status') === 'healthy').length;
  const healthyGroups = groups.filter((group) => group.status === 'running').length;
  const alerts = groups.filter((group) => instanceStatusTone(group.status) === 'bad').length +
    servers.filter((item) => getField(item, 'status', 'Status') === 'unhealthy').length;
  const memory = instances.reduce((sum, item) => sum + parseGi(getField(item, 'memory', 'Memory')), 0);

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
            <h3>节点与审计</h3>
            <Clock size={20} />
          </div>
          <div className="panel-body split-list">
            <div>
              <div className="subhead">已分配内存</div>
              <div className="large-metric">{memory ? `${memory.toFixed(memory >= 10 ? 0 : 1)}Gi` : '0Gi'}</div>
            </div>
            <div className="audit-list">
              {(audits || []).slice(0, 4).map((log, index) => (
                <AuditCard key={log.id || log.ID || index} log={log} />
              ))}
              {(!audits || audits.length === 0) && <EmptyState icon={List} title="暂无审计记录" />}
            </div>
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
  const topology = getField(master, 'role', 'Role') === 'standalone' ? '单点' : '集群';
  return (
    <div className="instance-card">
      <div className={`status-dot ${statusMeta(group.status).tone}`} />
      <div className="card-main">
        <strong>{group.groupName}</strong>
        <span>{master ? `主库: ${masterServer}` : '无主节点'}{group.replicas.length > 0 ? ` · 从库: ${group.replicas.length} 个` : ''}</span>
      </div>
      <div className="card-side">
        <StatusBadge status={group.status} />
        <span><GitMerge size={12} /> {group.engine} · {topology}</span>
      </div>
    </div>
  );
}

function InstancesView({ groups, query, setQuery, expandedGroups, setExpandedGroups }) {
  const filtered = groups.filter((group) => {
    const text = [
      group.groupName,
      group.engine,
      group.status,
      ...group.instances.flatMap((item) => [
        item.name,
        getField(item, 'server', 'Server'),
        getField(item, 'engine', 'Engine'),
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
                <th>引擎</th>
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
                />
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function InstanceGroupRows({ group, expanded, onToggle }) {
  const master = group.master;
  const hasReplicas = group.replicas.length > 0;
  const role = getField(master, 'role', 'Role');
  const topology = role === 'standalone' ? 'Standalone' : `1 Master / ${group.replicas.length} Replicas`;

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
                <span className="role-chip">{role === 'standalone' ? '单点' : 'Master'}</span>
              </div>
              <span className="muted">
                {master ? `实例: ${master.name} · ${getField(master, 'server', 'Server', '-')}` : '主库状态异常'}
              </span>
            </div>
          </div>
        </td>
        <td><span className="engine">{String(group.engine || '-').toUpperCase()}</span></td>
        <td>{topology}</td>
        <td>{master ? getField(master, 'server', 'Server', '-') : '-'}</td>
        <td>{master ? <EnvoyPorts item={master} /> : '-'}</td>
        <td>{master ? `${getField(master, 'memory', 'Memory', '-')} / ${getField(master, 'cpus', 'CPUs', '-')}C` : '-'}</td>
        <td><StatusBadge status={group.status} /></td>
        <td>
          <div className="row-actions">
            <IconButton icon={Settings} label="配置" />
            <IconButton icon={HardDrive} label="备份" />
            <IconButton icon={Trash2} label="删除" />
          </div>
        </td>
      </tr>
      {expanded && group.replicas.map((replica) => <ReplicaRow key={replica.name} item={replica} />)}
    </>
  );
}

function EnvoyPorts({ item }) {
  const envoy = getField(item, 'envoy', 'Envoy', {}) || {};
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

function ReplicaRow({ item }) {
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
      <td><span className="muted">--</span></td>
      <td>Replica</td>
      <td>{getField(item, 'server', 'Server', '-')}</td>
      <td><span className="muted">通过主库 Envoy 路由读</span></td>
      <td>{getField(item, 'memory', 'Memory', '-')} / {getField(item, 'cpus', 'CPUs', '-')}C</td>
      <td><StatusBadge status={getField(item, 'status', 'Status')} /></td>
      <td>
        <div className="row-actions">
          <IconButton icon={Settings} label="配置" />
          <IconButton icon={HardDrive} label="备份" />
          <IconButton icon={Trash2} label="删除" />
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
          label={<><HardDrive size={13} /> Memory</>}
          current={parseGi(allocated.memory || allocated.Memory)}
          max={parseGi(capacity.memory || capacity.Memory)}
          unit="Gi"
          tone="blue"
        />
      </div>
      <div className="muted">实例: {instances.length ? instances.join(', ') : '暂无'}</div>
    </section>
  );
}

function AuditView({ audits }) {
  const records = audits || [];
  return (
    <section className="panel table-panel">
      <div className="panel-header">
        <h3>最近审计记录</h3>
        <button className="primary-button" disabled>筛选</button>
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
                <td>{displayValue(log.time || log.Time || log.timestamp || log.Timestamp)}</td>
                <td>{displayValue(log.operator || log.Operator)}</td>
                <td>{displayValue(log.action || log.Action)}</td>
                <td>{displayValue(log.target || log.Target || log.instance || log.Instance)}</td>
                <td><StatusBadge status={log.result || log.Result || log.level || log.Level} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
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
        <span>目标: {displayValue(log.target || log.Target || log.instance || log.Instance)} · {displayValue(log.operator || log.Operator)}</span>
      </div>
      <time>{displayValue(log.time || log.Time || log.timestamp || log.Timestamp)}</time>
    </div>
  );
}
