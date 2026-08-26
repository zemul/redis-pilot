import { useState } from 'react';
import { Database, LogIn, UserPlus, Trash2, Key, Users, X, Copy } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription,
} from '@/components/ui/dialog';
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from '@/components/ui/table';
import {
  Select, SelectTrigger, SelectContent, SelectItem, SelectValue,
} from '@/components/ui/select';

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
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <form className="w-full max-w-sm border rounded-lg p-8 space-y-5" onSubmit={handleSubmit}>
        <div className="flex flex-col items-center gap-1 text-primary">
          <Database size={36} />
          <h1 className="text-xl font-semibold mt-2">Redis Pilot</h1>
          <p className="text-sm text-muted-foreground">多实例管理平台</p>
        </div>
        {error && (
          <div className="text-sm text-destructive bg-destructive/10 border border-destructive/20 rounded-md px-3 py-2">
            {error}
          </div>
        )}
        <div className="space-y-1.5">
          <label className="text-sm text-muted-foreground">用户名</label>
          <Input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="admin"
            autoFocus
            required
          />
        </div>
        <div className="space-y-1.5">
          <label className="text-sm text-muted-foreground">密码</label>
          <Input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="请输入密码"
            required
          />
        </div>
        <Button type="submit" className="w-full" disabled={loading}>
          <LogIn size={16} /> {loading ? '登录中...' : '登录'}
        </Button>
      </form>
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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="flex items-center gap-2 text-base font-semibold">
          <Users size={18} /> 用户管理
        </h3>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" onClick={() => setShowChangePw(true)}>
            <Key size={14} /> 修改密码
          </Button>
          <Button size="sm" onClick={() => setShowCreate(true)}>
            <UserPlus size={14} /> 新建用户
          </Button>
        </div>
      </div>

      {error && (
        <div className="text-sm text-destructive bg-destructive/10 border border-destructive/20 rounded-md px-3 py-2">
          {error}
        </div>
      )}

      {loading ? (
        <div className="text-center text-muted-foreground py-8">加载中...</div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>用户名</TableHead>
              <TableHead>创建时间</TableHead>
              <TableHead>创建者</TableHead>
              <TableHead className="w-16">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {users.map((u) => (
              <TableRow key={u.id}>
                <TableCell><code className="text-sm">{u.username}</code></TableCell>
                <TableCell className="text-muted-foreground">{u.created_at ? new Date(u.created_at).toLocaleString() : '-'}</TableCell>
                <TableCell className="text-muted-foreground">{u.created_by}</TableCell>
                <TableCell>
                  <Button variant="ghost" size="icon" className="h-7 w-7 text-destructive hover:text-destructive" onClick={() => setDeleteTarget(u.username)}>
                    <Trash2 size={14} />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      {/* 新建用户 */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>新建用户</DialogTitle>
          </DialogHeader>
          <form className="space-y-4" onSubmit={handleCreate}>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">用户名</label>
              <Input placeholder="输入用户名" value={newUser.username}
                onChange={(e) => setNewUser({ ...newUser, username: e.target.value })} autoFocus required />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">密码</label>
              <Input type="password" placeholder="至少 4 位" value={newUser.password}
                onChange={(e) => setNewUser({ ...newUser, password: e.target.value })} required />
            </div>
            <DialogFooter>
              <Button variant="ghost" type="button" onClick={() => setShowCreate(false)}>取消</Button>
              <Button type="submit" disabled={creating}>{creating ? '创建中...' : '创建'}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* 修改密码 */}
      <Dialog open={showChangePw} onOpenChange={setShowChangePw}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>修改密码</DialogTitle>
          </DialogHeader>
          <form className="space-y-4" onSubmit={handleChangePassword}>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">当前密码</label>
              <Input type="password" placeholder="输入当前密码" value={pwForm.old_password}
                onChange={(e) => setPwForm({ ...pwForm, old_password: e.target.value })} autoFocus required />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">新密码</label>
              <Input type="password" placeholder="至少 4 位" value={pwForm.new_password}
                onChange={(e) => setPwForm({ ...pwForm, new_password: e.target.value })} required />
            </div>
            <DialogFooter>
              <Button variant="ghost" type="button" onClick={() => setShowChangePw(false)}>取消</Button>
              <Button type="submit" disabled={changingPw}>{changingPw ? '修改中...' : '确认修改'}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* 删除确认 */}
      <Dialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>删除用户</DialogTitle>
            <DialogDescription>
              确认删除用户 <strong>{deleteTarget}</strong>？该用户的所有 API Token 也会被删除。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setDeleteTarget(null)}>取消</Button>
            <Button variant="destructive" onClick={() => handleDelete(deleteTarget)}>删除</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="flex items-center gap-2 text-base font-semibold">
          <Key size={18} /> API Token
        </h3>
        <Button size="sm" onClick={() => setShowCreateToken(true)}>
          <Key size={14} /> 生成 Token
        </Button>
      </div>

      {error && (
        <div className="text-sm text-destructive bg-destructive/10 border border-destructive/20 rounded-md px-3 py-2">
          {error}
        </div>
      )}

      {loading ? (
        <div className="text-center text-muted-foreground py-8">加载中...</div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>名称</TableHead>
              <TableHead>Token</TableHead>
              <TableHead>备注</TableHead>
              <TableHead>创建时间</TableHead>
              <TableHead>过期时间</TableHead>
              <TableHead className="w-16">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {tokens.map((t) => (
              <TableRow key={t.id}>
                <TableCell className="font-medium">{t.name}</TableCell>
                <TableCell>
                  <div className="flex items-center gap-1.5">
                    <code className="text-xs bg-muted px-1.5 py-0.5 rounded break-all">{t.token}</code>
                    <Button variant="ghost" size="icon" className="h-6 w-6 opacity-50 hover:opacity-100"
                      onClick={() => navigator.clipboard.writeText(t.token)}>
                      <Copy size={12} />
                    </Button>
                  </div>
                </TableCell>
                <TableCell className="text-muted-foreground">{t.description || '-'}</TableCell>
                <TableCell className="text-muted-foreground">{t.created_at ? new Date(t.created_at).toLocaleString() : '-'}</TableCell>
                <TableCell className="text-muted-foreground">{t.expires_at ? new Date(t.expires_at).toLocaleString() : '永不过期'}</TableCell>
                <TableCell>
                  <Button variant="ghost" size="icon" className="h-7 w-7 text-destructive hover:text-destructive"
                    onClick={() => setDeleteTarget(t)}>
                    <Trash2 size={14} />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
            {tokens.length === 0 && (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">暂无 Token</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      )}

      {/* 生成 Token */}
      <Dialog open={showCreateToken} onOpenChange={setShowCreateToken}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>生成 API Token</DialogTitle>
          </DialogHeader>
          <form className="space-y-4" onSubmit={handleCreateToken}>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">名称</label>
              <Input placeholder="如：CI 部署、监控脚本" value={tokenName}
                onChange={(e) => setTokenName(e.target.value)} autoFocus required />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">备注</label>
              <Input placeholder="补充说明（可选）" value={tokenDesc}
                onChange={(e) => setTokenDesc(e.target.value)} />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm text-muted-foreground">有效期</label>
              <Select value={tokenExpiry} onValueChange={setTokenExpiry}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="never">永不过期</SelectItem>
                  <SelectItem value="7d">7 天</SelectItem>
                  <SelectItem value="30d">30 天</SelectItem>
                  <SelectItem value="90d">90 天</SelectItem>
                  <SelectItem value="365d">1 年</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <DialogFooter>
              <Button variant="ghost" type="button" onClick={() => setShowCreateToken(false)}>取消</Button>
              <Button type="submit" disabled={creatingToken}>{creatingToken ? '生成中...' : '生成'}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Token 展示 */}
      <Dialog open={!!newToken} onOpenChange={() => setNewToken('')}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Token 已生成</DialogTitle>
            <DialogDescription>⚠️ Token 只显示一次，请立即复制保存</DialogDescription>
          </DialogHeader>
          <code className="block text-sm bg-muted px-3 py-2 rounded-md break-all select-all">{newToken}</code>
          <DialogFooter>
            <Button onClick={() => { navigator.clipboard.writeText(newToken); }}>复制</Button>
            <Button variant="ghost" onClick={() => setNewToken('')}>关闭</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 删除确认 */}
      <Dialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>删除 Token</DialogTitle>
            <DialogDescription>
              确认删除 Token <strong>{deleteTarget?.name}</strong>？使用此 Token 的 CLI/脚本将无法访问。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setDeleteTarget(null)}>取消</Button>
            <Button variant="destructive" onClick={async () => {
              try {
                await request('/auth/token/delete', { method: 'POST', body: JSON.stringify({ id: deleteTarget.id }) });
                setDeleteTarget(null);
                loadTokens();
              } catch (err) { setError(err.message); setDeleteTarget(null); }
            }}>删除</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
