import { Routes, Route, Navigate, useNavigate, useParams } from 'react-router-dom';
import { useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  Database,
  Key,
  LayoutDashboard,
  List,
  ClipboardList,
  Menu,
  Server,
  ShieldAlert,
  Sparkles,
  Users,
  X
} from 'lucide-react';
import { LoginPage, UserManagement, TokenManagement, getStoredAuth, clearStoredAuth, getStoredRole } from './Auth.jsx';
import { createRequest } from './lib/request.js';
import { UserMenu } from './components/common.jsx';
import { DashboardView } from './views/DashboardView.jsx';
import { InstancesView } from './views/InstancesView.jsx';
import { ServersView } from './views/ServersView.jsx';
import { AuditView } from './views/AuditView.jsx';
import { TasksView } from './views/TasksView.jsx';
import { IncidentsView } from './views/IncidentsView.jsx';
import {
  InstanceDetailModal,
  ConfigModal,
  ResizeModal,
  BackupListModal,
  DeleteConfirmModal,
  ConfirmPowerModal,
  FailoverConfirmModal,
  CreateInstanceModal,
  GroupLabelsModal,
} from './modals/index.jsx';
import AiChatPanel from './components/AiChatPanel.jsx';
import { normalizeMap, backupRows, auditQuery, getField } from './lib/format.js';
import { buildInstanceGroups } from './lib/instance.js';

const TABS = [
  { id: 'dashboard', label: '大盘看板', icon: LayoutDashboard },
  { id: 'instances', label: '实例管理', icon: Database },
  { id: 'servers', label: '服务器节点', icon: Server },
  { id: 'incidents', label: '异常记录', icon: ShieldAlert },
  { id: 'tasks', label: '运维任务', icon: ClipboardList },
  { id: 'audit', label: '操作审计', icon: List },
  { id: 'ai', label: 'AI 助手', icon: Sparkles },
  { id: 'users', label: '用户管理', icon: Users },
  { id: 'tokens', label: '密钥管理', icon: Key }
];

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
  const [auditFilters, setAuditFilters] = useState({
    from: '', to: '', q: '', actor_type: '', source: '', level: '', action: '', result: '', operator: '', group: '', instance: ''
  });
  const [detail, setDetail] = useState(null);
  const [backupDetail, setBackupDetail] = useState(null);
  const [confirmAction, setConfirmAction] = useState(null);
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [failoverConfirm, setFailoverConfirm] = useState(null);
  const [configTarget, setConfigTarget] = useState(null);
  const [resizeTarget, setResizeTarget] = useState(null);
  const [labelsTarget, setLabelsTarget] = useState(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createInitialGroup, setCreateInitialGroup] = useState(null);
  const [taskInstanceFilter, setTaskInstanceFilter] = useState('');
  const [incidentInstanceFilter, setIncidentInstanceFilter] = useState('');
  const [incidentSummary, setIncidentSummary] = useState(null);
  const [openIncidentsByGroup, setOpenIncidentsByGroup] = useState({});
  const [pendingAction, setPendingAction] = useState('');
  const [activeTaskCount, setActiveTaskCount] = useState(0);
  const [data, setData] = useState({ instances: {}, servers: {}, audits: [], sentinel: null });
  const [serverInfo, setServerInfo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const instanceGroups = useMemo(() => buildInstanceGroups(data.instances, openIncidentsByGroup), [data.instances, openIncidentsByGroup]);
  const labelSuggestions = useMemo(
    () => [...new Set(instanceGroups.flatMap((group) => group.labels || []))].sort((a, b) => a.localeCompare(b)),
    [instanceGroups]
  );
  const servers = useMemo(() => normalizeMap(data.servers, 'Servers', 'servers'), [data.servers]);
  const activeTitle = TABS.find((tab) => tab.id === activeTab)?.label || '大盘看板';
  const isAdmin = getStoredRole() === 'admin';

  const request = useMemo(
    () => createRequest({
      operator: auth?.username,
      onUnauthorized: () => setAuth(null),
    }),
    [auth]
  );

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
    if (!auth) return undefined;
    let ignore = false;
    async function loadActiveTasks() {
      try {
        const result = await request('/tasks?status=active&page=1&page_size=1');
        if (!ignore) setActiveTaskCount(result?.total || 0);
      } catch {
        if (!ignore) setActiveTaskCount(0);
      }
    }
    loadActiveTasks();
    const timer = setInterval(loadActiveTasks, 10000);
    return () => { ignore = true; clearInterval(timer); };
  }, [auth, request]);

  useEffect(() => {
    if (!auth) return undefined;
    let ignore = false;
    async function loadSummary() {
      try {
        const result = await request('/incidents/summary');
        if (!ignore) setIncidentSummary(result || null);
      } catch {
        if (!ignore) setIncidentSummary(null);
      }
      // 额外拉取全部 open 异常，按 group 聚合出"每组最高 open 异常等级"，
      // 供实例列表状态与拓扑取最差（口径与异常中心一致）。
      try {
        const list = await request('/incidents?status=open&page_size=200');
        const records = list?.records || list?.Records || [];
        const byGroup = {};
        for (const rec of records) {
          const group = rec.group_name || rec.GroupName;
          if (!group) continue;
          const severity = rec.severity || rec.Severity || 'warning';
          const type = rec.incident_type || rec.IncidentType || '';
          const prev = byGroup[group];
          // critical 优先于 warning；记录条数用于角标计数；types 记录出现过的异常类型（去重，保序）。
          const worse = prev?.severity === 'critical' ? 'critical' : severity;
          const types = prev?.types ? [...prev.types] : [];
          if (type && !types.includes(type)) types.push(type);
          byGroup[group] = {
            severity: prev?.severity === 'critical' ? 'critical' : worse,
            count: (prev?.count || 0) + 1,
            types,
          };
        }
        if (!ignore) setOpenIncidentsByGroup(byGroup);
      } catch {
        if (!ignore) setOpenIncidentsByGroup({});
      }
    }
    loadSummary();
    const timer = setInterval(loadSummary, 15000);
    return () => { ignore = true; clearInterval(timer); };
  }, [auth, request]);

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

  // 从实例详情弹窗跳到独立页并按该实例过滤：审计用 q 关键词（页面可见可清除），
  // 任务用后端 instance 精确过滤（TasksView 展示可清除的过滤标签）。
  function openInstanceAudit(name) {
    setAuditFilters({ from: '', to: '', q: name, actor_type: '', source: '', level: '', action: '', result: '', operator: '', group: '', instance: '' });
    setDetail(null);
    setActiveTab('audit');
  }

  function openInstanceTasks(name) {
    setTaskInstanceFilter(name);
    setDetail(null);
    setActiveTab('tasks');
  }

  function openIncidents(instance = '') {
    setIncidentInstanceFilter(instance);
    setDetail(null);
    setActiveTab('incidents');
  }

  async function openInstanceDetail(item, group, opts = {}) {
    const base = { item, group, mode: opts.mode || 'node', initialTab: opts.initialTab || 'trend' };
    setDetail({ ...base, loading: true, error: '', metrics: null });
    try {
      const metrics = await request(`/instance/metrics?name=${encodeURIComponent(item.name)}`);
      setDetail({ ...base, loading: false, error: '', metrics });
    } catch (err) {
      setDetail({ ...base, loading: false, error: err.message || String(err), metrics: null });
    }
  }

  // 组详情：以组主库为观测节点，mode=group，默认打开「实例组」页签。
  function openGroupDetail(group) {
    const anchor = group.master || group.instances[0];
    if (!anchor) return;
    openInstanceDetail(anchor, group, { mode: 'group', initialTab: 'group' });
  }

  // 节点详情：mode=node，默认「趋势」页签。
  function openNodeDetail(item, group) {
    openInstanceDetail(item, group, { mode: 'node', initialTab: 'trend' });
  }

  // 接入设置：打开组详情的「接入」页签。
  function openGroupAccess(group) {
    const anchor = group.master || group.instances[0];
    if (!anchor) return;
    openInstanceDetail(anchor, group, { mode: 'group', initialTab: 'access' });
  }

  // 添加从库：打开创建实例弹窗，预选「加入已有实例组」+ 当前组。
  function openAddReplica(group) {
    setCreateInitialGroup(group.groupName);
    setCreateOpen(true);
  }

  // 故障转移：二次确认后调用 /instance/failover。
  function openFailover(group) {
    setFailoverConfirm({ group });
  }

  async function executeFailover() {
    if (!failoverConfirm) return;
    const { group } = failoverConfirm;
    const target = group.master || group.instances[0];
    if (!target) return;
    const actionKey = `failover:${group.groupName}`;
    setPendingAction(actionKey);
    setError('');
    try {
      await request('/instance/failover', {
        method: 'POST',
        body: JSON.stringify({ name: target.name })
      });
      setFailoverConfirm(null);
      await refresh();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setPendingAction('');
    }
  }

  async function openBackupList(group) {
    setBackupDetail({ group, loading: true, error: '', backups: [] });
    try {
      const payload = await request(`/backup/list?group=${encodeURIComponent(group.groupName)}`);
      setBackupDetail({ group, loading: false, error: '', backups: backupRows(payload) });
    } catch (err) {
      setBackupDetail({ group, loading: false, error: err.message || String(err), backups: [] });
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

  // 异常记录导航角标：未处理异常总数 + 是否含严重级（严重→红，仅警告→琥珀）。
  const incidentCount = incidentSummary?.total || 0;
  const incidentHasCritical = (incidentSummary?.top || []).some((inc) => inc.severity === 'critical');

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
              if (tab.id === 'tasks') setTaskInstanceFilter('');
              setActiveTab(tab.id);
              setMobileOpen(false);
            }}
          >
            <Icon size={19} className={activeTab === tab.id ? 'text-white' : 'text-gray-400'} />
            <span>{tab.label}</span>
            {tab.id === 'tasks' && activeTaskCount > 0 && <span className={`ml-auto min-w-5 h-5 px-1 rounded-full inline-grid place-items-center text-[11px] font-semibold ${activeTab === 'tasks' ? 'bg-white text-gray-900' : 'bg-amber-100 text-amber-700'}`}>{activeTaskCount > 99 ? '99+' : activeTaskCount}</span>}
            {tab.id === 'incidents' && incidentCount > 0 && <span className={`ml-auto min-w-5 h-5 px-1 rounded-full inline-grid place-items-center text-[11px] font-semibold ${activeTab === 'incidents' ? 'bg-white text-gray-900' : incidentHasCritical ? 'bg-red-100 text-red-700' : 'bg-amber-100 text-amber-700'}`}>{incidentCount > 99 ? '99+' : incidentCount}</span>}
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
              {activeTab === 'dashboard' && <DashboardView groups={instanceGroups} servers={servers} audits={data.audits} sentinel={data.sentinel} incidentSummary={incidentSummary} publicHost={serverInfo?.proxy_public_endpoint} onOpenDetail={openInstanceDetail} onOpenIncidents={openIncidents} />}
              {activeTab === 'instances' && (
                <InstancesView
                  groups={instanceGroups}
                  publicHost={serverInfo?.proxy_public_endpoint}
                  sentinelPort={serverInfo?.proxy_sentinel_port}
                  query={query}
                  setQuery={setQuery}
                  expandedGroups={expandedGroups}
                  setExpandedGroups={setExpandedGroups}
                  callbacks={{
                    onOpenGroupDetail: openGroupDetail,
                    onOpenNodeDetail: openNodeDetail,
                    onOpenBackups: openBackupList,
                    onOpenAccess: openGroupAccess,
                    onAddReplica: openAddReplica,
                    onFailover: openFailover,
                    onEditLabels: setLabelsTarget,
                    onPowerAction: changeInstancePower,
                    onDelete: (item, group, role) => setDeleteConfirm({ item, group, role, cleanData: true }),
                    onConfig: (item) => setConfigTarget(item.name),
                    onResize: (item) => setResizeTarget({ name: item.name, memory: getField(item, 'memory', 'Memory', ''), cpus: String(getField(item, 'cpus', 'CPUs', '') || ''), disk: getField(item, 'disk', 'Disk', '') }),
                  }}
                  onCreate={() => { setCreateInitialGroup(null); setCreateOpen(true); }}
                  pendingAction={pendingAction}
                />
              )}
              {activeTab === 'ai' && <AiChatPanel />}
              {activeTab === 'servers' && <ServersView servers={servers} groups={instanceGroups} />}
              {activeTab === 'tasks' && <TasksView request={request} initialInstance={taskInstanceFilter} />}
              {activeTab === 'incidents' && <IncidentsView request={request} initialInstance={incidentInstanceFilter} />}
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
      {detail && <InstanceDetailModal detail={detail} publicHost={serverInfo?.proxy_public_endpoint} sentinelPort={serverInfo?.proxy_sentinel_port} requestFn={request} onRefresh={refresh} onNavigateAudit={openInstanceAudit} onNavigateTasks={openInstanceTasks} onNavigateIncidents={openIncidents} onConfig={(item) => setConfigTarget(item.name)} onResize={(item) => setResizeTarget({ name: item.name, memory: getField(item, 'memory', 'Memory', ''), cpus: String(getField(item, 'cpus', 'CPUs', '') || ''), disk: getField(item, 'disk', 'Disk', '') })} isAdmin={isAdmin} onClose={() => setDetail(null)} />}
      {configTarget && <ConfigModal instanceName={configTarget} requestFn={request} onClose={() => setConfigTarget(null)} />}
      {resizeTarget && <ResizeModal instanceName={resizeTarget.name} currentSpecs={{ memory: resizeTarget.memory, cpus: resizeTarget.cpus, disk: resizeTarget.disk }} requestFn={request} onClose={() => { setResizeTarget(null); refresh(); }} />}
      {labelsTarget && <GroupLabelsModal group={labelsTarget} suggestions={labelSuggestions} requestFn={request} onSaved={refresh} onClose={() => setLabelsTarget(null)} />}
      {backupDetail && <BackupListModal detail={backupDetail} requestFn={request} onReload={() => openBackupList(backupDetail.group)} onViewTasks={() => { setTaskInstanceFilter(backupDetail.group.groupName); setBackupDetail(null); setActiveTab('tasks'); }} onClose={() => setBackupDetail(null)} />}
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
      {failoverConfirm && (
        <FailoverConfirmModal
          detail={failoverConfirm}
          loading={pendingAction === `failover:${failoverConfirm.group.groupName}`}
          onCancel={() => setFailoverConfirm(null)}
          onConfirm={executeFailover}
        />
      )}
      {createOpen && (
        <CreateInstanceModal
          servers={servers}
          groups={instanceGroups}
          initialGroup={createInitialGroup}
          requestFn={request}
          onRefresh={refresh}
          onClose={() => { setCreateOpen(false); setCreateInitialGroup(null); }}
        />
      )}
    </div>
  );
}
