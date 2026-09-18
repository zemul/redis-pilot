// Local-only interactive UI fixture. No real backend or credentials; never deploy.
// Build dashboard first, then: node web/dashboard/test/cluster-ui-fixture.mjs
import http from 'node:http';
import fs from 'node:fs/promises';
const root = new URL('../../../internal/server/dashboard_dist/', import.meta.url);
const cluster = { id: 'demo-update', engine: 'kvrocks', cache_enabled: true, write_policy: 'update_existing', gateway_id: 'gw-demo', node_id: 'redis01', revision: 1, hot_instance: 'demo-hot', hot_address: '10.0.0.2:6380', shards: [{ id: 's1', instance: 'demo-cold', address: '10.0.0.2:6381' }], ingress: { node_id: 'redis01', port: 16509, ready: true, has_password: true, gateway_address: '127.0.0.1:17679' } };
const clusters = [cluster], pending = [];
const instances = { instances: {}, groups: {} };
function member(name, engine) {
  instances.instances[name] = { group: name, engine, server: 'redis02', role: 'master', status: 'running', memory: '256Mi', cpus: 0.25, port: 6380, desired_state: 'running' };
  instances.groups[name] = { engine, type: 'standalone', category: 'persistent', current_master: name, topology_status: 'healthy' };
}
member('ordinary-redis', 'redis'); member('demo-cold', 'kvrocks'); member('demo-hot', 'redis');
http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://127.0.0.1');
    let body = ''; for await (const chunk of req) body += chunk;
    const input = body ? JSON.parse(body) : {};
    const reply = (data, status = 200) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ success: status < 400, data, ...(status >= 400 ? { error: 'fixture rejection' } : {}) })); };
    if (url.pathname.startsWith('/dashboard/assets/')) {
      const name = url.pathname.split('/').pop();
      res.setHeader('Content-Type', name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' : 'application/octet-stream');
      return res.end(await fs.readFile(new URL('assets/'+name, root)));
    }
    if (url.pathname.startsWith('/dashboard')) { res.setHeader('Content-Type', 'text/html'); return res.end(await fs.readFile(new URL('index.html', root))); }
    if (url.pathname === '/auth/login') return reply({ token: 'fixture.'+Buffer.from(JSON.stringify({ role: input.username === 'viewer' ? 'viewer' : 'admin' })).toString('base64url')+'.fixture', username: input.username });
    if (url.pathname === '/node/list') return reply({ servers: { redis01: { endpoint: '10.0.0.1' }, redis02: { endpoint: '10.0.0.2' } } });
    if (url.pathname === '/instance/list') return reply(instances);
    if (url.pathname === '/server/info') return reply({ proxy_public_endpoint: '10.0.0.1' });
    if (url.pathname === '/api/v1/custom-clusters') return reply(clusters);
    if (url.pathname === '/api/v1/custom-clusters/pending') return reply({ records: pending, total: pending.length, page_size: 100 });
    if (url.pathname === '/api/v1/custom-clusters/create-options') return reply({ gateway_ids: ['gw-demo', 'gw-free'] });
    if (url.pathname === '/api/v1/gateway/runtime') return reply({ observations: [{ gateway_id: 'gw-demo', cluster_id: cluster.id, node_id: 'redis01', status: 'ready', serving: true, backend_ready: true }] });
    if (url.pathname === '/api/v1/custom-clusters/create') {
      if (!['redis01','redis02'].includes(input.node_id)) return reply(null, 400);
      const spec = { id: input.id, engine: 'kvrocks', gateway_id: input.gateway_id, node_id: input.node_id, cache_enabled: !!input.cache, write_policy: input.cache?.write_policy, hot_instance: input.cache ? input.id+'-hot' : '', shards: Array.from({ length: input.cold.count }, (_, i) => ({ id: 's'+(i+1), instance: input.id+'-cold-'+(i+1) })) };
      const names = [...spec.shards.map(s => s.instance), ...(spec.hot_instance ? [spec.hot_instance] : [])];
      names.forEach(n => member(n, n.endsWith('hot') ? 'redis' : 'kvrocks'));
      const task = { id: 'fixture-parent', phase: 'waiting_resources', status: 'running' };
      pending.push({ spec, task, children: names.map(n => ({ instance_name: n, server_name: 'redis02', phase: 'retry_wait', message: '模拟：Agent 暂不可达，等待自动重试' })) });
      setTimeout(() => { pending.splice(0); clusters.push({ ...spec, revision: 1 }); }, 20000);
      return reply({ task_id: task.id, cluster_id: spec.id }, 202);
    }
    if (url.pathname.endsWith('/gateway-config')) return reply({ yaml: '# UI fixture only\nmode: cache\nservice_id: demo-update\n' });
    if (url.pathname.endsWith('/ingress')) {
      const c = clusters.find(c => c.id === url.pathname.split('/')[4]);
      if (!c || !input.password) return reply(null, 400);
      c.ingress = { port: 16510, node_id: c.node_id, gateway_address: input.gateway_address, ready: false, has_password: true };
      return reply(c.ingress);
    }
    return reply({ records: [], total: 0 });
  } catch (e) { res.writeHead(500); res.end(String(e)); }
}).listen(18777, '127.0.0.1', () => console.log('UI fixture http://127.0.0.1:18777/dashboard/instances'));
