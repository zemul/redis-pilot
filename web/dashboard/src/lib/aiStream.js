// AI 助手 SSE 解析（纯逻辑，无 React 依赖，便于单测）。
//
// 后端事件格式（§14）：
//   event: run\ndata: {"id","conversation_id"}          ← 每次订阅的首帧，无 id
//   id: <seq>\nevent: <name>\ndata: {...}               ← run 内编号事件
//   : ping                                              ← 心跳注释，忽略

// parseSSEBlock 解析一个事件块；纯注释/空块返回 null。
export function parseSSEBlock(raw) {
  let event = 'message';
  let id = null;
  let data = '';
  let hasField = false;
  for (const line of raw.split('\n')) {
    if (!line || line.startsWith(':')) continue;
    hasField = true;
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('id:')) id = Number(line.slice(3).trim());
    else if (line.startsWith('data:')) data += line.slice(5).trim();
  }
  if (!hasField) return null;
  let parsed = {};
  if (data) {
    try {
      parsed = JSON.parse(data);
    } catch {
      parsed = {};
    }
  }
  return { event, id: Number.isFinite(id) ? id : null, data: parsed };
}

// createSSEParser 返回一个增量解析器：feed(chunkText) → 本次解析出的完整事件数组。
// 跨 chunk 的半截事件会被缓存到下一次 feed。
export function createSSEParser() {
  let buf = '';
  return {
    feed(text) {
      buf += text.replace(/\r\n/g, '\n');
      const parts = buf.split('\n\n');
      buf = parts.pop() || '';
      const out = [];
      for (const part of parts) {
        const ev = parseSSEBlock(part);
        if (ev) out.push(ev);
      }
      return out;
    },
  };
}

// AUTO_CONTINUE_PREFIX 后端多步写操作自动续接注入的 user 消息前缀；界面上隐藏。
export const AUTO_CONTINUE_PREFIX = '[系统提示]';

// parseToolCalls 把后端 tool_calls JSON 字符串解析为数组（渲染留痕用）。
export function parseToolCalls(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

// messagesFromStore 把后端落库消息转成界面消息，并隐藏自动续接提示。
export function messagesFromStore(list) {
  return (list || [])
    .map((m) => ({
      role: m.role,
      content: m.content || '',
      tool_calls: parseToolCalls(m.tool_calls),
      interrupted: !!m.interrupted,
    }))
    .filter((m) => !(m.role === 'user' && m.content.startsWith(AUTO_CONTINUE_PREFIX)))
    .filter((m) => m.role !== 'assistant' || m.content || m.tool_calls.length || m.interrupted);
}
