import { useEffect, useState } from 'react';
import { Dialog, DialogContent, DialogTitle, DialogDescription } from '../components/ui/dialog.jsx';
import { makeClusterSpec } from '../lib/customCluster.js';

const control = 'w-full rounded-md border border-gray-200 bg-white px-3 py-2 text-sm disabled:bg-gray-50';
const button = 'rounded-md bg-gray-900 px-4 py-2 text-sm text-white disabled:opacity-40';
function Field({ title, children }) { return <label className="grid gap-1.5 text-sm text-gray-600">{title}{children}</label>; }
function Shell({ title, description, onClose, busy, children }) {
  return <Dialog open onOpenChange={v => { if (!v && !busy) onClose(); }}><DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
    <DialogTitle>{title}</DialogTitle><DialogDescription>{description}</DialogDescription>{children}
  </DialogContent></Dialog>;
}

export function CreateCustomClusterModal({ request, servers, clusters, onClose, onCreated }) {
  const [form, setForm] = useState({ id: '', engine: 'kvrocks', gateway_id: '', node_id: '', count: 1, servers: [], cold_memory: '256Mi', cold_cpus: '0.25', cold_disk: '', cache: true, hot_memory: '128Mi', hot_cpus: '0.25', hot_server: '', write_policy: 'invalidate' });
  const [options, setOptions] = useState(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    let ignore = false;
    request('/api/v1/custom-clusters/create-options').then(v => { if (!ignore) setOptions(v.gateway_ids || []); }).catch(e => { if (!ignore) setError(e.message); });
    return () => { ignore = true; };
  }, [request]);
  const set = (key, value) => setForm(f => ({ ...f, [key]: value }));
  const nodeOptions = (Array.isArray(servers) ? servers.map(s => s.name) : Object.keys(servers)).sort();
  const available = options?.filter(id => !clusters.some(c => c.gateway_id === id)) || [];
  const input = (key, title, type = 'text') => <Field title={title}><input className={control} type={type} step={type === 'number' ? 'any' : undefined} required value={form[key]} onChange={e => set(key, e.target.value)} /></Field>;
  const nodeSelect = (value, change, automatic = true) => <select className={control} value={value} onChange={e => change(e.target.value)} required={!automatic}><option value="">{automatic ? '自动调度' : '请选择节点'}</option>{nodeOptions.map(n => <option key={n}>{n}</option>)}</select>;
  async function submit(e) {
    e.preventDefault(); setError('');
    let spec;
    try { spec = makeClusterSpec(form); } catch (e) { setError(e.message); return; }
    setBusy(true);
    try {
      const result = await request('/api/v1/custom-clusters/create', { method: 'POST', body: JSON.stringify(spec) });
      onCreated(result);
    } catch (e) { setError(`${e.message}。若响应丢失，请使用相同名称和规格重试，系统会复用原任务。`); }
    finally { setBusy(false); }
  }
  return <Shell title="创建分片集群" description="支持 Redis 分片、Kvrocks 分片及冷热分离。资源创建完成后仍需独立部署 Gateway 并配置统一入口。" busy={busy} onClose={onClose}>
    <form onSubmit={submit} className="space-y-5"><fieldset disabled={busy} className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-2">{input('id', '集群名称')}<Field title="Gateway（已配置机器凭据）"><select className={control} required value={form.gateway_id} onChange={e => set('gateway_id', e.target.value)}><option value="">请选择 Gateway</option>{available.map(id => <option key={id}>{id}</option>)}</select></Field>
        <Field title="Gateway / Proxy 所在节点">{nodeSelect(form.node_id, v => set('node_id', v), false)}</Field>
        <Field title="存储引擎"><select className={control} value={form.engine} onChange={e => setForm(f => ({ ...f, engine: e.target.value, cache: e.target.value === 'redis' ? false : f.cache }))}><option value="redis">Redis</option><option value="kvrocks">Kvrocks</option></select></Field>
        <Field title="分片数量"><input className={control} type="number" min="1" max="128" step="1" required value={form.count} onChange={e => set('count', e.target.value)} /></Field>
        <Field title="每分片副本数"><select className={control} value={form.replicas || 0} onChange={e => set('replicas', e.target.value)}><option value="0">无副本</option><option value="1">1 个副本</option><option value="2">2 个副本</option></select></Field>
      </div>
      {options && !available.length && <p role="status" className="text-sm text-amber-700">没有空闲 Gateway 标识。请先在 Server 配置独立机器凭据；这里不创建或展示密码。</p>}
      <section className="space-y-3 border-t pt-4"><h3 className="font-medium">存储分片 · {form.engine === 'redis' ? 'Redis' : 'Kvrocks'}</h3><div className="grid gap-3 sm:grid-cols-3">{input('cold_memory', '每分片内存')}{input('cold_cpus', '每分片 CPU', 'number')}<Field title="每分片磁盘预算（可选）"><input className={control} placeholder="如 10Gi" value={form.cold_disk} onChange={e => set('cold_disk', e.target.value)} /></Field></div>
        <div className="grid gap-3 sm:grid-cols-2">{Array.from({ length: Math.min(128, Math.max(0, Number(form.count) || 0)) }, (_, i) => <Field key={i} title={`分片 s${i + 1} 部署节点`}>{nodeSelect(form.servers[i] || '', v => setForm(f => { const values = [...f.servers]; values[i] = v; return { ...f, servers: values }; }))}</Field>)}</div>
        <p className="text-xs text-gray-500">副本与主库使用相同规格，并部署到不同服务器；选择副本需要已启用 Sentinel。不同分片允许共用服务器，磁盘预算不是硬配额。故障切换遵循原生异步复制边界，可能丢失最近写入。</p>
        {form.engine === 'redis' && <p className="text-xs text-gray-500">Redis 分片保存权威数据，禁止自动淘汰，启用 AOF 同步落盘及 RDB；内存不足时可能拒绝写入。此配置优先保证存储可靠性，会增加写入延迟。</p>}
      </section>
      {form.engine === 'kvrocks' && <section className="space-y-3 border-t pt-4"><label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={form.cache} onChange={e => set('cache', e.target.checked)} />启用 Redis 缓存加速</label>
        {form.cache && <><div className="grid gap-3 sm:grid-cols-3">{input('hot_memory', '热层内存')}{input('hot_cpus', '热层 CPU', 'number')}<Field title="热层节点">{nodeSelect(form.hot_server, v => set('hot_server', v))}</Field></div>
          <Field title="写后缓存策略"><select className={control} value={form.write_policy} onChange={e => set('write_policy', e.target.value)}><option value="invalidate">删除缓存 · 写冷后失效，读时回温</option><option value="update_existing">双写更新 · 安全更新已有完整缓存</option></select></Field>
          <p className="text-xs text-gray-500">热层是缓存，不是冷层副本。双写更新不会为每次写入强制建立完整缓存；异常时允许失效或绕过缓存。缓存开关和策略在创建时确定。</p></>}
      </section>}
      {error && <p role="alert" className="text-sm text-red-700">{error}</p>}
      <div className="flex justify-end gap-3"><button type="button" onClick={onClose} className="text-sm">取消</button><button className={button} disabled={busy || !available.length}>{busy ? '正在提交…' : '创建集群'}</button></div>
    </fieldset></form>
  </Shell>;
}

export function ClusterAccessModal({ cluster, mode, request, onClose, onSaved }) {
  const exportMode = mode === 'config';
  const [form, setForm] = useState({ server_endpoint: '', listen: cluster.ingress?.gateway_address || '', admin_listen: '', state_dir: `/var/lib/redis-gateway-${cluster.id}`, token_file: `/etc/redis-gateway-${cluster.id}/control.secret`, gateway_address: '', password: '' });
  const [yaml, setYAML] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(e) {
    e.preventDefault(); setBusy(true); setError(''); setYAML('');
    const body = exportMode ? Object.fromEntries(['server_endpoint', 'listen', 'admin_listen', 'state_dir', 'token_file'].map(k => [k, form[k]])) : { gateway_address: form.gateway_address, password: form.password };
    try {
      const result = await request(`/api/v1/custom-clusters/${encodeURIComponent(cluster.id)}/${exportMode ? 'gateway-config' : 'ingress'}`, { method: 'POST', body: JSON.stringify(body) });
      if (exportMode) setYAML(result.yaml); else { setForm(f => ({ ...f, password: '' })); onSaved(); }
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  }
  function download() {
    const url = URL.createObjectURL(new Blob([yaml], { type: 'text/yaml;charset=utf-8' }));
    const a = document.createElement('a'); a.href = url; a.download = `${cluster.id}-gateway.yaml`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  const fields = exportMode ? [['server_endpoint', 'Server 地址（从 Gateway 主机访问）', 'https://pilot.example.internal'], ['listen', 'Gateway 回环监听', '127.0.0.1:17679'], ['admin_listen', '管理回环监听', '127.0.0.1:19600'], ['state_dir', '独占状态根目录', ''], ['token_file', '机器凭据文件路径', '']] : [['gateway_address', '已部署 Gateway 的回环监听', '127.0.0.1:17679'], ['password', '业务 Proxy 密码', '设置客户端接入密码']];
  return <Shell title={`${cluster.id} · ${exportMode ? '导出 Gateway 配置' : '配置统一入口'}`} description={exportMode ? '仅生成首次安装配置，不自动部署、初始化或覆盖运行状态。' : '业务密码只用于 Proxy。登记后仍须等待 Gateway 确认，不代表立即可用。'} busy={busy} onClose={onClose}>
    <form onSubmit={submit} className="space-y-4"><fieldset disabled={busy} className="grid gap-4 sm:grid-cols-2">{fields.map(([key, label, placeholder]) => <Field key={key} title={label}><input className={control} required type={key === 'password' ? 'password' : 'text'} autoComplete={key === 'password' ? 'new-password' : 'off'} value={form[key]} placeholder={placeholder} onChange={e => { setForm(f => ({ ...f, [key]: e.target.value })); setYAML(''); }} /></Field>)}</fieldset>
      <p className="text-xs text-gray-500">{exportMode ? '远程控制面需要 HTTPS，本机可用回环 HTTP。凭据文件由运维安全安装；首次初始化只执行一次，正常 systemd 启动不能带初始化参数。端口是否占用、防火墙和目录权限仍需核对。' : '入口地址创建后不可修改。请填写实际 Gateway 监听地址，而非数据库地址；密码不会在集群详情中回显。'}</p>
      {error && <p role="alert" className="text-sm text-red-700">{error}</p>}
      <div className="flex justify-end"><button className={button} disabled={busy}>{busy ? '处理中…' : exportMode ? '生成配置' : '登记入口'}</button></div>
    </form>
    {yaml && <section className="space-y-2"><button type="button" onClick={download} className={button}>下载 YAML</button><pre className="max-h-56 overflow-auto rounded-md bg-gray-50 p-3 text-xs">{yaml}</pre><p className="text-xs text-gray-500">下一步：独立部署 Gateway → 检查就绪 → 登记统一入口 → 从 Proxy 验证读写。不要删除已有状态重新初始化。</p></section>}
  </Shell>;
}
