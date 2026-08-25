import { useState } from 'react';
import { Database, LogIn, UserPlus, Trash2, Key, Users, X, Copy } from 'lucide-react';

// ─── Auth Context ─────────────────────────────────────────────────────────────

const TOKEN_KEY = 'redisPilotJWT';
const USER_KEY = 'redisPilotUser';

export function getStoredAuth() {
  const token = localStorage.getItem(TOKEN_KEY);
  const user = localStorage.getItem(USER_KEY);
  return token ? { token, username: user || '' } : null;
}

export function setStoredAuth(token, username) {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, username);
}

export function clearStoredAuth() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

export function authHeaders() {
  const token = localStorage.getItem(TOKEN_KEY);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

// ─── Login Page ───────────────────────────────────────────────────────────────

export function LoginPage({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await fetch('/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });
      const body = await res.json();
      if (!res.ok || body.error) {
        setError(body.error || '登录失败');
        return;
      }
      setStoredAuth(body.data.token, body.data.username);
      onLogin(body.data);
    } catch (err) {
      setError(err.message || '网络错误');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleSubmit}>
        <div className="login-brand">
          <Database size={36} />
          <h1>Redis Pilot</h1>
          <p>多实例管理平台</p>
        </div>
        {error && <div className="login-error">{error}</div>}
        <label>
          用户名
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="admin"
            autoFocus
            required
          />
        </label>
        <label>
          密码
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="请输入密码"
            required
          />
        </label>
        <button type="submit" className="primary-button login-button" disabled={loading}>
          <LogIn size={16} /> {loading ? '登录中...' : '登录'}
        </button>
      </form>
    </div>
  );
}

// ─── Modal (reusable) ────────────────────────────────────────────────────────

function Modal({ open, onClose, title, children }) {
  if (!open) return null;
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>{title}</h3>
          <button className="icon-button" onClick={onClose}><X size={16} /></button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

// ─── User Management Panel ──────────────────────────────────────────────────

export function UserManagement({ request }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [newUser, setNewUser] = useState({ username: '', password: '' });
  const [creating, setCreating] = useState(false);
  const [showChangePw, setShowChangePw] = useState(false);
  const [pwForm, setPwForm] = useState({ old_password: '', new_password: '' });
  const [changingPw, setChangingPw] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState(null);

  async function loadUsers() {
    setLoading(true);
    setError('');
    try {
      const data = await request('/auth/user/list');
      setUsers(data || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useState(() => { loadUsers(); });

  async function handleCreate(e) {
    e.preventDefault();
    setCreating(true);
    setError('');
    try {
      await request('/auth/user/create', {
        method: 'POST',
        body: JSON.stringify(newUser)
      });
      setNewUser({ username: '', password: '' });
      setShowCreate(false);
      loadUsers();
    } catch (err) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  }

  async function handleDelete(username) {
    setError('');
    try {
      await request('/auth/user/delete', {
        method: 'POST',
        body: JSON.stringify({ username })
      });
      setDeleteTarget(null);
      loadUsers();
    } catch (err) {
      setError(err.message);
    }
  }

  async function handleChangePassword(e) {
    e.preventDefault();
    setChangingPw(true);
    setError('');
    try {
      await request('/auth/change-password', {
        method: 'POST',
        body: JSON.stringify(pwForm)
      });
      setPwForm({ old_password: '', new_password: '' });
      setShowChangePw(false);
    } catch (err) {
      setError(err.message);
    } finally {
      setChangingPw(false);
    }
  }

  return (
    <div className="user-management">
      <div className="panel-header">
        <h3><Users size={18} /> 用户管理</h3>
        <div className="header-actions">
          <button className="ghost-button" onClick={() => setShowChangePw(true)}>
            <Key size={14} /> 修改密码
          </button>
          <button className="primary-button" onClick={() => setShowCreate(true)}>
            <UserPlus size={14} /> 新建用户
          </button>
        </div>
      </div>

      {error && <div className="notice">{error}</div>}

      {loading ? (
        <div className="panel loading">加载中...</div>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>用户名</th>
              <th>创建时间</th>
              <th>创建者</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id}>
                <td><code>{u.username}</code></td>
                <td>{u.created_at ? new Date(u.created_at).toLocaleString() : '-'}</td>
                <td>{u.created_by}</td>
                <td>
                  <button className="icon-button danger" onClick={() => setDeleteTarget(u.username)} title="删除用户">
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* ── 新建用户弹窗 ── */}
      <Modal open={showCreate} onClose={() => setShowCreate(false)} title="新建用户">
        <form className="modal-form" onSubmit={handleCreate}>
          <label>
            用户名
            <input type="text" placeholder="输入用户名" value={newUser.username}
              onChange={(e) => setNewUser({ ...newUser, username: e.target.value })} autoFocus required />
          </label>
          <label>
            密码
            <input type="password" placeholder="至少 4 位" value={newUser.password}
              onChange={(e) => setNewUser({ ...newUser, password: e.target.value })} required />
          </label>
          <div className="modal-actions">
            <button type="button" className="ghost-button" onClick={() => setShowCreate(false)}>取消</button>
            <button type="submit" className="primary-button" disabled={creating}>
              {creating ? '创建中...' : '创建'}
            </button>
          </div>
        </form>
      </Modal>

      {/* ── 修改密码弹窗 ── */}
      <Modal open={showChangePw} onClose={() => setShowChangePw(false)} title="修改密码">
        <form className="modal-form" onSubmit={handleChangePassword}>
          <label>
            当前密码
            <input type="password" placeholder="输入当前密码" value={pwForm.old_password}
              onChange={(e) => setPwForm({ ...pwForm, old_password: e.target.value })} autoFocus required />
          </label>
          <label>
            新密码
            <input type="password" placeholder="至少 4 位" value={pwForm.new_password}
              onChange={(e) => setPwForm({ ...pwForm, new_password: e.target.value })} required />
          </label>
          <div className="modal-actions">
            <button type="button" className="ghost-button" onClick={() => setShowChangePw(false)}>取消</button>
            <button type="submit" className="primary-button" disabled={changingPw}>
              {changingPw ? '修改中...' : '确认修改'}
            </button>
          </div>
        </form>
      </Modal>

      {/* ── 删除用户确认弹窗 ── */}
      <Modal open={!!deleteTarget} onClose={() => setDeleteTarget(null)} title="删除用户">
        <div className="modal-form">
          <p style={{margin: 0, fontSize: '0.9rem'}}>
            确认删除用户 <strong>{deleteTarget}</strong>？该用户的所有 API Token 也会被删除。
          </p>
          <div className="modal-actions">
            <button className="ghost-button" onClick={() => setDeleteTarget(null)}>取消</button>
            <button className="primary-button danger-button" onClick={() => handleDelete(deleteTarget)}>删除</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}

// ─── Token Management Panel ─────────────────────────────────────────────────

export function TokenManagement({ request }) {
  const [tokens, setTokens] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showCreateToken, setShowCreateToken] = useState(false);
  const [tokenName, setTokenName] = useState('');
  const [tokenDesc, setTokenDesc] = useState('');
  const [tokenExpiry, setTokenExpiry] = useState('never');
  const [newToken, setNewToken] = useState('');
  const [creatingToken, setCreatingToken] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState(null);

  async function loadTokens() {
    setLoading(true);
    setError('');
    try {
      const data = await request('/auth/token/list');
      setTokens(data || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useState(() => { loadTokens(); });

  async function handleCreateToken(e) {
    e.preventDefault();
    setCreatingToken(true);
    setError('');
    try {
      const data = await request('/auth/token/create', {
        method: 'POST',
        body: JSON.stringify({ name: tokenName || 'default', description: tokenDesc, expires_in: tokenExpiry })
      });
      setNewToken(data.token);
      setTokenName('');
      setTokenDesc('');
      setTokenExpiry('never');
      setShowCreateToken(false);
      loadTokens();
    } catch (err) {
      setError(err.message);
    } finally {
      setCreatingToken(false);
    }
  }

  return (
    <div className="user-management">
      <div className="panel-header">
        <h3><Key size={18} /> API Token</h3>
        <div className="header-actions">
          <button className="primary-button" onClick={() => setShowCreateToken(true)}>
            <Key size={14} /> 生成 Token
          </button>
        </div>
      </div>

      {error && <div className="notice">{error}</div>}

      {loading ? (
        <div className="panel loading">加载中...</div>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>名称</th>
              <th>Token</th>
              <th>备注</th>
              <th>创建时间</th>
              <th>过期时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {tokens.map((t) => (
              <tr key={t.id}>
                <td>{t.name}</td>
                <td className="token-cell"><code>{t.token}</code><button className="icon-button" onClick={() => navigator.clipboard.writeText(t.token)} title="复制"><Copy size={13} /></button></td>
                <td>{t.description || '-'}</td>
                <td>{t.created_at ? new Date(t.created_at).toLocaleString() : '-'}</td>
                <td>{t.expires_at ? new Date(t.expires_at).toLocaleString() : '永不过期'}</td>
                <td>
                  <button className="icon-button danger" onClick={() => setDeleteTarget(t)} title="删除 Token">
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
            {tokens.length === 0 && <tr><td colSpan="6" style={{textAlign:'center',color:'var(--text-2)'}}>暂无 Token</td></tr>}
          </tbody>
        </table>
      )}

      {/* ── 生成 Token 弹窗 ── */}
      <Modal open={showCreateToken} onClose={() => setShowCreateToken(false)} title="生成 API Token">
        <form className="modal-form" onSubmit={handleCreateToken}>
          <label>
            名称
            <input type="text" placeholder="如：CI 部署、监控脚本" value={tokenName}
              onChange={(e) => setTokenName(e.target.value)} autoFocus required />
          </label>
          <label>
            备注
            <input type="text" placeholder="补充说明（可选）" value={tokenDesc}
              onChange={(e) => setTokenDesc(e.target.value)} />
          </label>
          <label>
            有效期
            <select value={tokenExpiry} onChange={(e) => setTokenExpiry(e.target.value)}
              style={{padding: '0.6rem 0.75rem', border: '1px solid var(--border)', borderRadius: 6, fontSize: '0.9rem', background: 'var(--bg)', color: 'var(--text)'}}>
              <option value="never">永不过期</option>
              <option value="7d">7 天</option>
              <option value="30d">30 天</option>
              <option value="90d">90 天</option>
              <option value="365d">1 年</option>
            </select>
          </label>
          <div className="modal-actions">
            <button type="button" className="ghost-button" onClick={() => setShowCreateToken(false)}>取消</button>
            <button type="submit" className="primary-button" disabled={creatingToken}>
              {creatingToken ? '生成中...' : '生成'}
            </button>
          </div>
        </form>
      </Modal>

      {/* ── Token 展示弹窗 ── */}
      <Modal open={!!newToken} onClose={() => setNewToken('')} title="Token 已生成">
        <div className="token-reveal">
          <p>⚠️ Token 只显示一次，请立即复制保存：</p>
          <code className="token-value">{newToken}</code>
          <div className="modal-actions">
            <button className="primary-button" onClick={() => { navigator.clipboard.writeText(newToken); }}>
              复制
            </button>
            <button className="ghost-button" onClick={() => setNewToken('')}>关闭</button>
          </div>
        </div>
      </Modal>

      {/* ── 删除确认弹窗 ── */}
      <Modal open={!!deleteTarget} onClose={() => setDeleteTarget(null)} title="删除 Token">
        <div className="modal-form">
          <p style={{margin: 0, fontSize: '0.9rem'}}>
            确认删除 Token <strong>{deleteTarget?.name}</strong>？使用此 Token 的 CLI/脚本将无法访问。
          </p>
          <div className="modal-actions">
            <button className="ghost-button" onClick={() => setDeleteTarget(null)}>取消</button>
            <button className="primary-button danger-button" onClick={async () => {
              try {
                await request('/auth/token/delete', { method: 'POST', body: JSON.stringify({ id: deleteTarget.id }) });
                setDeleteTarget(null);
                loadTokens();
              } catch (err) { setError(err.message); setDeleteTarget(null); }
            }}>删除</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
