import { test } from 'node:test';
import assert from 'node:assert/strict';

import { summarizeTrace, formatDuration, formatClock } from '../src/lib/aiTrace.js';

test('连续相同工具合并为一组并统计失败，写操作单独列出', () => {
  const calls = [
    { type: 'tool_call', tool: 'list_instances', ok: true },
    { type: 'tool_call', tool: 'get_bigkeys', ok: true },
    { type: 'tool_call', tool: 'get_bigkeys', ok: false },
    { type: 'tool_call', tool: 'get_bigkeys', result_summary: 'error: timeout' },
    { type: 'tool_call', tool: 'describe_key', ok: true },
    { type: 'tool_call', tool: 'get_bigkeys', ok: true },
    { type: 'write_confirm', tool: 'start_instance', summary: '启动 x' },
    { type: 'run_meta', started_at: 1000, finished_at: 112000 },
  ];
  const s = summarizeTrace(calls);
  assert.equal(s.total, 6);
  assert.equal(s.failed, 2);
  assert.deepEqual(s.groups.map((g) => [g.tool, g.count, g.failed]), [
    ['list_instances', 1, 0],
    ['get_bigkeys', 3, 2],
    ['describe_key', 1, 0],
    ['get_bigkeys', 1, 0],
  ]);
  assert.equal(s.writes.length, 1);
  assert.equal(s.durationMs, 111000);
});

test('老消息无 run_meta 时耗时为 null，空输入安全', () => {
  assert.equal(summarizeTrace([{ type: 'tool_call', tool: 'x' }]).durationMs, null);
  assert.deepEqual(summarizeTrace(null), { groups: [], writes: [], total: 0, failed: 0, durationMs: null });
});

test('耗时与计时格式化', () => {
  assert.equal(formatDuration(300), '1 秒');
  assert.equal(formatDuration(12400), '12 秒');
  assert.equal(formatDuration(111000), '1 分 51 秒');
  assert.equal(formatDuration(120000), '2 分');
  assert.equal(formatClock(42000), '0:42');
  assert.equal(formatClock(3903000), '1:05:03');
});
