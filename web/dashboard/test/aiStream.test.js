import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createSSEParser, parseSSEBlock, messagesFromStore } from '../src/lib/aiStream.js';

test('解析带 id 的事件与无 id 的 run 首帧，忽略心跳注释', () => {
  const p = createSSEParser();
  const evs = p.feed(
    'event: run\ndata: {"id":"run-1","conversation_id":"c1"}\n\n' +
    ': ping\n\n' +
    'id: 1\nevent: token\ndata: {"text":"你"}\n\n'
  );
  assert.equal(evs.length, 2);
  assert.deepEqual(evs[0], { event: 'run', id: null, data: { id: 'run-1', conversation_id: 'c1' } });
  assert.deepEqual(evs[1], { event: 'token', id: 1, data: { text: '你' } });
});

test('跨 chunk 的半截事件缓存到下一次 feed', () => {
  const p = createSSEParser();
  assert.deepEqual(p.feed('id: 2\nevent: tok'), []);
  const evs = p.feed('en\ndata: {"text":"好"}\n\nid: 3\nevent: end\ndata: {"stopped":false}\n\n');
  assert.deepEqual(evs.map((e) => [e.id, e.event]), [[2, 'token'], [3, 'end']]);
});

test('纯注释块返回 null', () => {
  assert.equal(parseSSEBlock(': ping'), null);
});

test('历史消息隐藏自动续接提示与空 assistant 承载', () => {
  const msgs = messagesFromStore([
    { role: 'user', content: '建主从' },
    { role: 'assistant', content: '', tool_calls: '[{"type":"write_confirm","tool":"create_instance"}]' },
    { role: 'user', content: '[系统提示] 上一步写操作已成功执行' },
    { role: 'assistant', content: '', tool_calls: '[]' },
    { role: 'assistant', content: '完成', tool_calls: '[]' },
  ]);
  assert.deepEqual(msgs.map((m) => [m.role, m.content]), [['user', '建主从'], ['assistant', ''], ['assistant', '完成']]);
  assert.equal(msgs[1].tool_calls[0].tool, 'create_instance');
});
