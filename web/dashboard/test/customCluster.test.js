import test from 'node:test';
import assert from 'node:assert/strict';
import { usesCache, clusterMembers, clusterRuntime } from '../src/lib/customCluster.js';

test('legacy cluster retains cache; disabled mode has no hot member', () => {
  const cluster = { hot_instance: 'hot', hot_address: '127.0.0.1:1', shards: [{ id: 'a', instance: 'cold' }] };
  assert.equal(usesCache(cluster), true);
  assert.equal(clusterMembers(cluster).length, 2);
  assert.equal(clusterMembers({ ...cluster, cache_enabled: false }).length, 1);
});
test('stale history and published ingress do not imply a healthy Gateway', () => {
  const c = { id: 'c', gateway_id: 'g', node_id: 'n', ingress: { ready: true } };
  const observation = { cluster_id: 'c', gateway_id: 'g', node_id: 'n', status: 'stale', serving: true, backend_ready: true };
  assert.equal(clusterRuntime(c, [observation]), '未收到有效心跳');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'ready' }]), '运行正常');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'ready', node_id: 'other' }]), '未收到有效心跳');
  assert.equal(clusterRuntime(c, [{ ...observation, status: 'conflict' }, { ...observation, status: 'ready' }]), '运行冲突');
});
