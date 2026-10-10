import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { authHeaders } from '../Auth.jsx';
import { createSSEParser, messagesFromStore } from '../lib/aiStream.js';

// AiChatProvider 持有 AI 助手的全部会话/流式状态，挂在 AppShell 上（§14.4）。
//
// 与页面组件生命周期解耦：切 tab 卸载 AiChatPanel 不会丢输出；后端 run 与 HTTP 连接也已解耦，
// 刷新页面后按 localStorage 里记住的会话自动打开，若该会话仍在运行则从头回放并继续跟随。
// 断开订阅（切会话/卸载）只是不再看，不会停止后端 run；停止必须显式调 stop。

const AiChatContext = createContext(null);

// 有会话在运行时轮询会话列表的间隔（驱动侧栏转圈与导航角标）。
const POLL_MS = 3000;
// 订阅意外断开（网络抖动/代理超时）时的最大重连次数。
const MAX_RESUME_RETRIES = 5;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function apiJSON(path, opts = {}) {
  const resp = await fetch(path, {
    ...opts,
    headers: { ...authHeaders(), ...(opts.body ? { 'Content-Type': 'application/json' } : {}), ...(opts.headers || {}) },
  });
  const body = await resp.json().catch(() => null);
  if (!resp.ok || !body?.success) {
    const err = new Error(body?.error || `请求失败：${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return body.data;
}

export function useAiChat() {
  const ctx = useContext(AiChatContext);
  if (!ctx) throw new Error('useAiChat must be used within AiChatProvider');
  return ctx;
}

// useAiChatOptional 供导航角标等可选消费方使用（Provider 外返回 null）。
export function useAiChatOptional() {
  return useContext(AiChatContext);
}

export function AiChatProvider({ username, children }) {
  const storageKey = `redisPilotAiConv:${username || ''}`;
  const [conversations, setConversations] = useState([]);
  const [convId, setConvIdState] = useState('');
  const [messages, setMessages] = useState([]); // [{role, content, tool_calls?, interrupted?}]
  const [streaming, setStreaming] = useState(false); // 当前会话是否有 run 在跑（且本页正在跟随）
  const [current, setCurrent] = useState(''); // 当前 run 正在流式输出的文本
  const [toolStatus, setToolStatus] = useState(null); // {name, running, ok}
  const [liveStats, setLiveStats] = useState(null); // 运行中统计 {startedAt, calls}：状态条显示计时与调用次数
  const [error, setError] = useState('');
  const [pendingConfirm, setPendingConfirm] = useState(null); // {tool, args, summary}
  const [executing, setExecuting] = useState(false);
  const [loadingConv, setLoadingConv] = useState(false);
  const [allowWrite, setAllowWrite] = useState(false);

  const convIdRef = useRef('');
  const subRef = useRef(null); // 当前订阅 {ctrl, conv, runId, lastSeq, text, calls}

  const setConvId = useCallback((id) => {
    convIdRef.current = id;
    setConvIdState(id);
    try {
      if (id) localStorage.setItem(storageKey, id);
      else localStorage.removeItem(storageKey);
    } catch {
      /* localStorage 不可用时只是不能跨刷新恢复 */
    }
  }, [storageKey]);

  const refreshConversations = useCallback(async () => {
    try {
      setConversations((await apiJSON('/api/v1/ai/conversations')) || []);
    } catch {
      /* 会话列表拉取失败不阻断主流程 */
    }
  }, []);

  // detach 结束本页对当前 run 的跟随（不停止后端 run）。
  const detach = useCallback(() => {
    const sub = subRef.current;
    subRef.current = null;
    sub?.ctrl.abort();
    setStreaming(false);
    setCurrent('');
    setToolStatus(null);
    setLiveStats(null);
  }, []);

  // reloadMessages 只刷新已落库消息（不改订阅），用于 run 被其它标签页的新 run 取代等情况。
  const reloadMessages = useCallback(async (id) => {
    try {
      const data = await apiJSON(`/api/v1/ai/conversations/${id}`);
      if (convIdRef.current === id) setMessages(messagesFromStore(data?.messages));
    } catch {
      /* 忽略 */
    }
  }, []);

  // consume 读取一条 SSE 响应直到结束；返回是否收到 end（false=意外断开）。
  const consume = useCallback(async (resp, sub) => {
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    const parser = createSSEParser();
    const live = () => subRef.current === sub;
    while (true) {
      const { value, done } = await reader.read();
      if (done) return false;
      for (const ev of parser.feed(decoder.decode(value, { stream: true }))) {
        if (!live()) return true;
        if (ev.id != null) sub.lastSeq = ev.id;
        const d = ev.data || {};
        switch (ev.event) {
          case 'run': {
            if (sub.runId && d.id !== sub.runId) {
              // 断线期间该会话已被新 run 取代：后端从头回放新 run，旧 run 结果以落库为准。
              sub.text = '';
              sub.calls = [];
              sub.startedAt = null;
              setCurrent('');
              reloadMessages(sub.conv);
            }
            sub.runId = d.id;
            sub.startedAt = Number.isFinite(d.started_at) ? d.started_at : Date.now();
            setLiveStats({ startedAt: sub.startedAt, calls: sub.calls.filter((c) => c.type === 'tool_call').length });
            if (!sub.conv && d.conversation_id) {
              // 新建会话：绑定后端分配的 id，并刷新侧栏让它带着转圈出现。
              sub.conv = d.conversation_id;
              setConvId(d.conversation_id);
              refreshConversations();
            }
            break;
          }
          case 'token':
            sub.text += d.text || '';
            setCurrent(sub.text);
            setToolStatus(null);
            break;
          case 'tool_call':
            setToolStatus({ name: d.name, running: true });
            sub.calls.push({ type: 'tool_call', tool: d.name, args: d.args, ts: Date.now() });
            setLiveStats({ startedAt: sub.startedAt, calls: sub.calls.filter((c) => c.type === 'tool_call').length });
            break;
          case 'tool_result': {
            setToolStatus({ name: d.name, running: false, ok: d.ok });
            // 回填到最近一个同名、尚无结果的调用，供折叠摘要统计失败（与后端落库口径一致）。
            for (let i = sub.calls.length - 1; i >= 0; i--) {
              const c = sub.calls[i];
              if (c.type === 'tool_call' && c.tool === d.name && c.ok === undefined) {
                c.ok = !!d.ok;
                c.result_summary = d.ok ? 'ok' : `error: ${d.error || ''}`;
                break;
              }
            }
            break;
          }
          case 'confirm':
            setPendingConfirm({ tool: d.tool, args: d.args, summary: d.summary });
            sub.calls.push({ type: 'write_confirm', tool: d.tool, summary: d.summary, ts: Date.now() });
            break;
          case 'error':
            setError(d.message || 'AI 服务出错');
            break;
          case 'end': {
            const text = sub.text;
            const calls = sub.calls.length
              ? [...sub.calls, { type: 'run_meta', started_at: sub.startedAt || Date.now(), finished_at: Date.now() }]
              : sub.calls;
            if (text || calls.length) {
              setMessages((m) => [...m, { role: 'assistant', content: text, tool_calls: calls, interrupted: !!d.interrupted }]);
            }
            return true;
          }
          default:
            break;
        }
      }
    }
  }, [reloadMessages, refreshConversations, setConvId]);

  // follow 跟随会话的 run：firstResp 为 POST /ai/chat 的响应（已在流上）；为空则经 stream 端点订阅。
  // 意外断开时按 (run_id, last_seq) 断点续订；run 已不存在（204）时 onGone 决定后续。
  const follow = useCallback(async (conv, firstResp, { onGone = 'reload' } = {}) => {
    subRef.current?.ctrl.abort();
    const sub = { ctrl: new AbortController(), conv, runId: null, lastSeq: 0, text: '', calls: [], startedAt: null };
    subRef.current = sub;
    setStreaming(true);
    setCurrent('');
    setToolStatus(null);
    let resp = firstResp;
    let retries = 0;
    let gone = false;
    try {
      while (subRef.current === sub) {
        try {
          if (!resp) {
            const qs = sub.runId ? `?run=${encodeURIComponent(sub.runId)}&after=${sub.lastSeq}` : '';
            resp = await fetch(`/api/v1/ai/conversations/${encodeURIComponent(sub.conv)}/stream${qs}`, {
              headers: { ...authHeaders() },
              signal: sub.ctrl.signal,
            });
            if (resp.status === 204) {
              gone = true;
              break;
            }
            if (!resp.ok || !resp.body) throw new Error(`订阅失败：${resp.status}`);
          }
          if (await consume(resp, sub)) break;
        } catch (e) {
          if (e.name === 'AbortError' || subRef.current !== sub) return;
          if (retries >= MAX_RESUME_RETRIES) {
            setError(`与 AI 服务的连接中断：${e.message || e}`);
            break;
          }
        }
        // 意外断开：退避后断点续订。新会话尚未拿到 id 时无法续订。
        resp = null;
        if (!sub.conv) break;
        retries += 1;
        await sleep(Math.min(1000 * retries, 5000));
      }
    } finally {
      if (subRef.current === sub) {
        subRef.current = null;
        setStreaming(false);
        setCurrent('');
        setToolStatus(null);
        setLiveStats(null);
      }
      refreshConversations();
    }
    if (gone && onGone === 'reload' && convIdRef.current === sub.conv) {
      // run 已结束且已出内存保留期：以落库历史为准。
      reloadMessages(sub.conv);
    }
  }, [consume, refreshConversations, reloadMessages]);

  const loadConversation = useCallback(async (id, { force = false } = {}) => {
    if (!id || (!force && id === convIdRef.current)) return;
    detach();
    setConvId(id);
    setMessages([]);
    setPendingConfirm(null);
    setError('');
    setLoadingConv(true);
    try {
      const data = await apiJSON(`/api/v1/ai/conversations/${encodeURIComponent(id)}`);
      if (convIdRef.current !== id) return; // 加载期间用户已切到别的会话
      setMessages(messagesFromStore(data?.messages));
      const p = data?.pending;
      setPendingConfirm(p ? { tool: p.tool, args: p.args, summary: p.summary } : null);
      if (data?.run?.running) follow(id, null);
    } catch (e) {
      if (convIdRef.current !== id) return;
      if (e.status === 404) {
        // 会话已删除/失效：退回空白新会话。
        setConvId('');
      } else {
        setError(e.message || String(e));
      }
    } finally {
      if (convIdRef.current === id || !convIdRef.current) setLoadingConv(false);
    }
  }, [detach, follow, setConvId]);

  const newConversation = useCallback(() => {
    detach();
    setConvId('');
    setMessages([]);
    setPendingConfirm(null);
    setError('');
  }, [detach, setConvId]);

  const send = useCallback(async (text) => {
    const q = (text || '').trim();
    if (!q || subRef.current || streaming) return false;
    setError('');
    setPendingConfirm(null);
    setMessages((m) => [...m, { role: 'user', content: q }]);
    setStreaming(true);
    const conv = convIdRef.current;
    try {
      const resp = await fetch('/api/v1/ai/chat', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: q, conversation_id: conv }),
      });
      if (!resp.ok || !resp.body) {
        const body = await resp.json().catch(() => null);
        throw new Error(body?.error || `请求失败：${resp.status}`);
      }
      if (convIdRef.current !== conv) {
        // 请求期间已切走：run 已在后台启动，这里不跟随，侧栏会显示转圈。
        resp.body.cancel().catch(() => {});
        refreshConversations();
        return true;
      }
      follow(conv, resp);
      return true;
    } catch (e) {
      setStreaming(false);
      setMessages((m) => (m.length && m[m.length - 1].role === 'user' && m[m.length - 1].content === q ? m.slice(0, -1) : m));
      setError(e.message || String(e));
      return false;
    }
  }, [follow, refreshConversations, streaming]);

  const stop = useCallback(async () => {
    const conv = subRef.current?.conv || convIdRef.current;
    if (!conv) return;
    try {
      await apiJSON(`/api/v1/ai/conversations/${encodeURIComponent(conv)}/stop`, { method: 'POST' });
    } catch (e) {
      setError(e.message || String(e));
    }
    // 结果（已生成部分 + 中断标记）随订阅里的 end 事件到达。
  }, []);

  // 确认执行：调独立执行端点（不走 LLM）。成功后后端自动续接下一轮 run，这里只负责订阅。
  const confirmExecute = useCallback(async () => {
    if (!pendingConfirm || executing) return;
    const conv = convIdRef.current;
    const { tool, args, summary } = pendingConfirm;
    let argsObj = {};
    try {
      argsObj = args ? JSON.parse(args) : {};
    } catch {
      argsObj = {};
    }
    setExecuting(true);
    setError('');
    try {
      await apiJSON('/api/v1/ai/execute', {
        method: 'POST',
        body: JSON.stringify({ tool, args: argsObj, conversation_id: conv }),
      });
      if (convIdRef.current !== conv) return;
      setMessages((m) => [...m, { role: 'system', content: `已完成 · ${summary}` }]);
      setPendingConfirm(null);
      setExecuting(false);
      if (conv) follow(conv, null, { onGone: 'ignore' });
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setExecuting(false);
    }
  }, [executing, follow, pendingConfirm]);

  const cancelConfirm = useCallback(async () => {
    if (!pendingConfirm || executing) return;
    const conv = convIdRef.current;
    setMessages((m) => [...m, { role: 'system', content: `已取消：${pendingConfirm.summary}` }]);
    setPendingConfirm(null);
    if (conv) {
      apiJSON(`/api/v1/ai/conversations/${encodeURIComponent(conv)}/dismiss`, { method: 'POST' })
        .then(refreshConversations)
        .catch(() => {});
    }
  }, [executing, pendingConfirm, refreshConversations]);

  const deleteConversation = useCallback(async (id) => {
    try {
      await apiJSON(`/api/v1/ai/conversations/${encodeURIComponent(id)}`, { method: 'DELETE' });
      setConversations((cs) => cs.filter((c) => c.id !== id));
      if (id === convIdRef.current) newConversation();
    } catch (e) {
      setError(e.message || String(e));
    }
  }, [newConversation]);

  // 挂载：拉会话列表、探测写能力、恢复上次打开的会话（刷新后自动回到该会话）。
  useEffect(() => {
    refreshConversations();
    apiJSON('/api/v1/ai/capabilities')
      .then((d) => setAllowWrite(!!d?.allow_write))
      .catch(() => {});
    let saved = '';
    try {
      saved = localStorage.getItem(storageKey) || '';
    } catch {
      saved = '';
    }
    if (saved) loadConversation(saved, { force: true });
    return () => {
      // 卸载（登出）：只停止跟随，不停止后端 run。
      const sub = subRef.current;
      subRef.current = null;
      sub?.ctrl.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 有会话在运行（含后台、非当前会话）时轮询列表，驱动转圈；全部结束后停止轮询。
  const anyBackgroundRunning = conversations.some((c) => c.running);
  useEffect(() => {
    if (!anyBackgroundRunning && !streaming) return undefined;
    const timer = setInterval(refreshConversations, POLL_MS);
    return () => clearInterval(timer);
  }, [anyBackgroundRunning, streaming, refreshConversations]);

  // 侧栏单项状态：当前会话以本页实时状态为准，其余以列表轮询为准。
  const conversationStatus = useCallback((c) => {
    if (c.id === convId) {
      return { running: streaming || (!subRef.current && !!c.running), pending: !!pendingConfirm };
    }
    return { running: !!c.running, pending: !!c.pending_confirm };
  }, [convId, pendingConfirm, streaming]);

  const anyRunning = streaming || conversations.some((c) => c.running);
  const anyPending = !!pendingConfirm || conversations.some((c) => c.id !== convId && c.pending_confirm);

  const value = useMemo(() => ({
    conversations, convId, messages, streaming, current, toolStatus, liveStats, error, pendingConfirm,
    executing, loadingConv, allowWrite, anyRunning, anyPending,
    send, stop, loadConversation, newConversation, deleteConversation, confirmExecute, cancelConfirm,
    conversationStatus, setError,
  }), [
    conversations, convId, messages, streaming, current, toolStatus, liveStats, error, pendingConfirm,
    executing, loadingConv, allowWrite, anyRunning, anyPending,
    send, stop, loadConversation, newConversation, deleteConversation, confirmExecute, cancelConfirm,
    conversationStatus,
  ]);

  return <AiChatContext.Provider value={value}>{children}</AiChatContext.Provider>;
}
