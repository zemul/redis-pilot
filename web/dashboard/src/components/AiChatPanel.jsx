import { useState, useRef, useEffect } from 'react';
import { Sparkles, Send, Loader2, Wrench, CheckCircle2, AlertCircle, Square, ShieldCheck } from 'lucide-react';
import { authHeaders } from '../Auth.jsx';

// 工具名 → 中文展示（执行状态条用）
const TOOL_LABELS = {
  list_instances: '列出实例',
  get_instance_status: '查询实例状态',
  diagnose_instance: '深度诊断',
  get_slowlog: '获取慢查询日志',
  get_bigkeys: '扫描大 key',
  get_instance_config: '读取实例配置',
  get_metrics_history: '获取历史指标',
  list_nodes: '列出服务器节点',
  query_audit: '查询操作审计',
  load_skill: '加载技能手册',
};

// 空对话时的快捷入口，点击直接发送预置问题
const QUICK_PROMPTS = [
  '列出所有实例及其运行状态',
  '有哪些实例内存水位偏高？',
  '帮我巡检一下集群整体健康状况',
];

// 解析一个 SSE 事件块（形如 "event: token\ndata: {...}"）
function parseEvent(raw) {
  let event = 'message';
  let data = '';
  for (const line of raw.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) data += line.slice(5).trim();
  }
  if (!data) return { event, data: {} };
  try {
    return { event, data: JSON.parse(data) };
  } catch {
    return { event, data: {} };
  }
}

export default function AiChatPanel() {
  const [messages, setMessages] = useState([]); // 已完成的对话 [{role, content}]
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [current, setCurrent] = useState(''); // 当前正在流式输出的助手文本
  const [toolStatus, setToolStatus] = useState(null); // {name, running, ok}
  const [error, setError] = useState('');
  const scrollRef = useRef(null);
  const abortRef = useRef(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, current, toolStatus, error]);

  async function send(text) {
    const q = (text ?? input).trim();
    if (!q || streaming) return;
    setError('');
    setInput('');
    const history = [...messages, { role: 'user', content: q }];
    setMessages(history);
    setStreaming(true);
    setCurrent('');
    setToolStatus(null);

    const controller = new AbortController();
    abortRef.current = controller;
    let assistantText = '';
    try {
      const resp = await fetch('/api/v1/ai/chat', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history }),
        signal: controller.signal,
      });
      if (!resp.ok || !resp.body) {
        const body = await resp.json().catch(() => null);
        throw new Error(body?.error || `请求失败：${resp.status}`);
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const parts = buf.split('\n\n');
        buf = parts.pop() || '';
        for (const part of parts) {
          if (!part.trim()) continue;
          const { event, data } = parseEvent(part);
          if (event === 'token') {
            assistantText += data.text || '';
            setCurrent(assistantText);
            setToolStatus(null);
          } else if (event === 'tool_call') {
            setToolStatus({ name: data.name, running: true });
          } else if (event === 'tool_result') {
            setToolStatus({ name: data.name, running: false, ok: data.ok });
          } else if (event === 'error') {
            throw new Error(data.message || 'AI 服务出错');
          }
        }
      }
      if (assistantText) {
        setMessages((m) => [...m, { role: 'assistant', content: assistantText }]);
      }
    } catch (e) {
      if (e.name !== 'AbortError') setError(e.message || String(e));
    } finally {
      setStreaming(false);
      setCurrent('');
      setToolStatus(null);
      abortRef.current = null;
    }
  }

  function stop() {
    abortRef.current?.abort();
  }

  function onKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  }

  const empty = messages.length === 0 && !streaming;

  return (
    <div className="border border-gray-200 rounded-lg bg-white flex flex-col h-[calc(100vh-180px)] min-h-[480px]">
      {/* 头部：标题 + 只读声明 */}
      <div className="px-5 py-3.5 border-b border-gray-200 flex items-center gap-2.5">
        <div className="w-8 h-8 rounded-md grid place-items-center bg-gray-900 text-white shrink-0">
          <Sparkles size={18} />
        </div>
        <div className="min-w-0">
          <div className="text-[15px] font-semibold leading-tight text-gray-900">AI 运维助手</div>
          <div className="text-xs text-gray-400 mt-0.5">基于实例诊断数据与运维技能手册的智能分析</div>
        </div>
        <span className="ml-auto inline-flex items-center gap-1 text-xs text-emerald-700 bg-emerald-50 border border-emerald-200 rounded px-2 py-1">
          <ShieldCheck size={13} /> 只读 · 仅分析不修改
        </span>
      </div>

      {/* 消息区 */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
        {empty && (
          <div className="h-full flex flex-col items-center justify-center text-center gap-4">
            <div className="w-12 h-12 rounded-full grid place-items-center bg-gray-100 text-gray-400">
              <Sparkles size={24} />
            </div>
            <div className="text-gray-500 text-sm max-w-md">
              用自然语言提问，我会调用平台的诊断能力采集数据并分析。例如：
            </div>
            <div className="flex flex-col gap-2 w-full max-w-md">
              {QUICK_PROMPTS.map((p) => (
                <button
                  key={p}
                  onClick={() => send(p)}
                  className="text-left text-[13px] text-gray-700 border border-gray-200 rounded-md px-3 py-2.5 bg-white hover:bg-gray-50 hover:border-gray-300 cursor-pointer transition-colors"
                >
                  {p}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <MessageBubble key={i} role={m.role} content={m.content} />
        ))}

        {/* 正在进行的助手回复 */}
        {streaming && (
          <div className="flex flex-col gap-2">
            {toolStatus && <ToolStatus status={toolStatus} />}
            {current ? (
              <MessageBubble role="assistant" content={current} />
            ) : (
              !toolStatus && (
                <div className="inline-flex items-center gap-2 text-gray-400 text-[13px]">
                  <Loader2 size={15} className="animate-spin" /> 正在思考…
                </div>
              )
            )}
          </div>
        )}

        {error && (
          <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]">
            <AlertCircle size={16} className="shrink-0" /> {error}
          </div>
        )}
      </div>

      {/* 输入区 */}
      <div className="border-t border-gray-200 p-3.5">
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onKeyDown}
            rows={1}
            placeholder="输入问题，Enter 发送，Shift+Enter 换行"
            className="flex-1 resize-none max-h-32 min-h-[40px] rounded-md border border-gray-200 px-3 py-2.5 text-sm text-gray-900 outline-none focus:border-gray-400 focus:ring-1 focus:ring-gray-300"
          />
          {streaming ? (
            <button
              onClick={stop}
              className="h-[40px] px-3.5 rounded-md bg-gray-100 text-gray-700 border border-gray-200 inline-flex items-center gap-1.5 text-sm cursor-pointer hover:bg-gray-200"
            >
              <Square size={15} /> 停止
            </button>
          ) : (
            <button
              onClick={() => send()}
              disabled={!input.trim()}
              className="h-[40px] px-3.5 rounded-md bg-gray-900 text-white inline-flex items-center gap-1.5 text-sm cursor-pointer hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <Send size={15} /> 发送
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function MessageBubble({ role, content }) {
  const isUser = role === 'user';
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[85%] rounded-lg px-3.5 py-2.5 text-[13px] leading-relaxed whitespace-pre-wrap break-words ${
          isUser ? 'bg-gray-900 text-white' : 'bg-gray-50 border border-gray-200 text-gray-800'
        }`}
      >
        {content}
      </div>
    </div>
  );
}

function ToolStatus({ status }) {
  const label = TOOL_LABELS[status.name] || status.name;
  return (
    <div className="inline-flex items-center gap-2 text-[13px] text-gray-500 bg-gray-50 border border-gray-200 rounded-md px-3 py-2 w-fit">
      {status.running ? (
        <Loader2 size={14} className="animate-spin text-gray-400" />
      ) : status.ok ? (
        <CheckCircle2 size={14} className="text-emerald-500" />
      ) : (
        <AlertCircle size={14} className="text-red-500" />
      )}
      <Wrench size={13} className="text-gray-400" />
      {status.running ? `正在${label}…` : `${label} 完成`}
    </div>
  );
}
