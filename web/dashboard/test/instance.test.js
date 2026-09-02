import { test } from 'node:test';
import assert from 'node:assert/strict';

import { buildInstanceGroups } from '../src/lib/instance.js';

// 故障转移未收敛：currentMaster 已指向新主（原从库，role 字段仍是 replica），
// 而旧主的 role 字段尚未收敛（仍是 master）。此时旧主既不是 currentMaster、
// role 又是 master，任何“用 role 反推 replica”的过滤都会把它整体丢掉。
test('failover 未收敛：旧主 role 仍为 master 时节点数守恒', () => {
  const state = {
    instances: {
      'order-a': { group: 'order', role: 'master' },   // 旧主，role 未收敛
      'order-b': { group: 'order', role: 'replica' },   // 新主，已被提升但 role 未收敛
    },
    groups: {
      order: { current_master: 'order-b', type: 'replication', category: 'persistent' },
    },
  };

  const [group] = buildInstanceGroups(state);
  assert.ok(group, 'group 应存在');

  // primary 应为 currentMaster 指向的新主
  assert.equal(group.master?.name, 'order-b');

  // 节点数守恒：master + replicas 必须覆盖全部实例，旧主不能消失
  const shown = new Set([group.master?.name, ...group.replicas.map((r) => r.name)]);
  assert.equal(shown.size, group.instances.length, '展示节点数应等于实例总数');
  assert.ok(shown.has('order-a'), '旧主 order-a 不应从界面消失');
  assert.deepEqual(
    group.replicas.map((r) => r.name),
    ['order-a'],
    '旧主应作为 replica 行展示'
  );
});

test('稳态：master + 多 replica 正常归类', () => {
  const state = {
    instances: {
      'order-a': { group: 'order', role: 'master' },
      'order-b': { group: 'order', role: 'replica' },
      'order-c': { group: 'order', role: 'replica' },
    },
    groups: {
      order: { current_master: 'order-a', type: 'replication', category: 'persistent' },
    },
  };

  const [group] = buildInstanceGroups(state);
  assert.equal(group.master?.name, 'order-a');
  assert.deepEqual(group.replicas.map((r) => r.name).sort(), ['order-b', 'order-c']);
  const shown = new Set([group.master?.name, ...group.replicas.map((r) => r.name)]);
  assert.equal(shown.size, group.instances.length);
});

test('standalone：单实例不产生多余 replica', () => {
  const state = {
    instances: {
      'cache-a': { group: 'cache', role: 'standalone' },
    },
    groups: {
      cache: { type: 'standalone', category: 'cache' },
    },
  };

  const [group] = buildInstanceGroups(state);
  assert.equal(group.master?.name, 'cache-a');
  assert.equal(group.replicas.length, 0);
});
