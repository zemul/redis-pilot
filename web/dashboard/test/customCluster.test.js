import test from 'node:test';
import assert from 'node:assert/strict';
import { usesCache, clusterMembers, clusterRuntime, aggregateClusters, ordinaryGroups, makeClusterSpec, cachePolicy, clusterListGroups } from '../src/lib/customCluster.js';

test('clusters project into the shared instance filters without ordinary instance actions', () => {
  const c = { id: 'demo', gateway_id: 'g', node_id: 'n', cache_enabled: false, shards: [{ instance: 'cold' }], ingress: { ready: true } };
  const instances = new Map([['cold', { name: 'cold', server: 'redis02', labels: ['开发'] }]]);
  const observations = [{ gateway_id: 'g', cluster_id: 'demo', node_id: 'n', status: 'ready', serving: true, backend_ready: true }];
  const [row] = clusterListGroups([c], instances, observations, '');
  assert.equal(row.cluster, c);
  assert.equal(row.type, 'sharded');
  assert.equal(row.engine, 'kvrocks');
  assert.equal(row.status, 'running');
  assert.deepEqual(row.labels, ['开发']);
  assert.equal(row.instances[0].server, 'redis02');
  assert.equal(clusterListGroups([c], instances, [], '')[0].status, 'pending');
  assert.equal(clusterListGroups([c], instances, observations, 'unavailable')[0].status, 'unknown');
  assert.equal(clusterListGroups([{ ...c, creation: {} }], instances, [], '')[0].status, 'creating');
  assert.equal(clusterListGroups([c], new Map(), [], '')[0].instances[0].name, 'cold');
});

test('legacy cluster retains cache; disabled mode has no hot member', () => {
  const cluster = { hot_instance: 'hot', hot_address: '127.0.0.1:1', shards: [{ id: 'a', instance: 'cold' }] };
  assert.equal(usesCache(cluster), true);
  assert.equal(clusterMembers(cluster).length, 2);
  assert.equal(clusterMembers({ ...cluster, cache_enabled: false }).length, 1);
});

test('aggregate pending and completed clusters without duplicate member rows', () => {
  const spec = { id: 'c', hot_instance: 'hot', shards: [{ id: 's1', instance: 'cold' }] };
  const pending = [{ spec, task: { id: 'parent', phase: 'retry_wait' }, children: [{ instance_name: 'cold' }] }];
  assert.equal(aggregateClusters([], pending)[0].creation.phase, 'retry_wait');
  const rows = aggregateClusters([{ ...spec, revision: 1, task: { target: [{ instance: 'new' }] } }], pending);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].creation, undefined);
  const groups = ['hot','cold','new','ordinary'].map(name => ({ groupName: name, instances: [{ name }] }));
  assert.deepEqual(ordinaryGroups(groups, rows).map(g => g.groupName), ['ordinary']);
});

const form = { id: 'orders', gateway_id: 'g', node_id: 'n', count: 2, servers: ['n'], cold_memory: '256Mi', cold_cpus: '0.25', cold_disk: '1Gi', cache: true, hot_memory: '128Mi', hot_cpus: '0.25', hot_server: '', write_policy: 'update_existing' };
test('creation form builds exact three-mode API shapes, truncating removed shards', () => {
  const result = makeClusterSpec(form);
  assert.equal(result.cold.cpus, 0.25);
  assert.deepEqual(result.cold.servers, ['n', '']);
  assert.equal(result.cache.write_policy, 'update_existing');
  assert.equal(makeClusterSpec({ ...form, cache: false }).cache, undefined);
  const redis = makeClusterSpec({ ...form, engine: 'redis', cache: false });
  assert.equal(redis.engine, 'redis');
  assert.equal(redis.cache, undefined);
  assert.equal(redis.cold.count, 2);
  assert.equal(makeClusterSpec({ ...form, write_policy: 'invalidate' }).cache.write_policy, 'invalidate');
  assert.deepEqual(makeClusterSpec({ ...form, count: 1, servers: ['a','b'] }).cold.servers, ['a']);
  assert.equal(cachePolicy({ cache_enabled: false }), '无缓存');
});
test('creation validation rejects invalid resource choices', () => {
  assert.throws(() => makeClusterSpec({ ...form, engine: 'redis' }));
  assert.throws(() => makeClusterSpec({ ...form, engine: 'unknown' }));
  for (const changed of [{ id: '../x' }, { node_id: '' }, { gateway_id: '' }, { count: 0 }, { count: 129 }, { count: 1.5 }, { cold_memory: '0Gi' }, { cold_cpus: '-1' }, { cold_cpus: 'NaN' }, { cold_disk: 'garbage' }]) {
    assert.throws(() => makeClusterSpec({ ...form, ...changed }));
  }
});

test('published migration keeps source members out of ordinary operations until cleanup finishes', () => {
  const groups = ['old','new'].map(name => ({ instances: [{ name }], groupName: name }));
  const c = { cache_enabled: false, shards: [{ instance: 'new' }], task: { phase: 'published', source: [{ instance: 'old' }], target: [{ instance: 'new' }] } };
  assert.deepEqual(ordinaryGroups(groups, [c]), []);
  assert.deepEqual(ordinaryGroups(groups, [{ ...c, task: { ...c.task, phase: 'complete' } }]).map(g => g.groupName), ['old']);
});
test('stale history and published ingress do not imply a healthy Gateway', () => {
  const c = { id: 'c', gateway_id: 'g', node_id: 'n', ingress: { ready: true } };
  const observation = { cluster_id: 'c', gateway_id: 'g', node_id: 'n', status: 'stale', serving: true, backend_ready: true };
  assert.equal(clusterRuntime(c, [observation]), '未收到有效心跳');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'ready' }]), '运行正常');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'ready', node_id: 'other' }]), '未收到有效心跳');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'conflict' }, { ...observation, status: 'ready' }]), '运行冲突');
});
