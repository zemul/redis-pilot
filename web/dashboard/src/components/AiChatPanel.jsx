import { useState, useRef, useEffect } from 'react';
import { Sparkles, Send, Loader2, Wrench, CheckCircle2, AlertCircle, Square, ShieldAlert, Plus, Trash2, MessageSquare, Gauge, Stethoscope, ScanSearch, Database, LineChart, ChevronDown } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { authHeaders } from '../Auth.jsx';

// Markdown 渲染（assistant 回复）。remark-gfm 支持表格/删除线/任务列表；
// 各元素用 tailwind 定制样式，避免依赖 @tailwindcss/typography。
const MD_COMPONENTS = {
  h1: (p) => <h1 className="text-[16px] font-semibold mt-3 mb-1.5" {...p} />,
  h2: (p) => <h2 className="text-[15px] font-semibold mt-3 mb-1.5" {...p} />,
  h3: (p) => <h3 className="text-[14px] font-semibold mt-2.5 mb-1" {...p} />,
  p: (p) => <p className="my-1.5" {...p} />,
  ul: (p) => <ul className="list-disc pl-5 my-1.5 space-y-0.5" {...p} />,
  ol: (p) => <ol className="list-decimal pl-5 my-1.5 space-y-0.5" {...p} />,
  li: (p) => <li className="leading-relaxed" {...p} />,
  a: (p) => <a className="text-blue-600 underline" target="_blank" rel="noreferrer" {...p} />,
  strong: (p) => <strong className="font-semibold" {...p} />,
  blockquote: (p) => <blockquote className="border-l-2 border-gray-300 pl-3 my-1.5 text-gray-500" {...p} />,
  // react-markdown v9 + remark-gfm 不再传 `inline` prop：改用「内容是否含换行/是否带 language- className」
  // 判断行内 vs 块级。避免把行内代码（如 `master_link_status: up`）误渲染成整行大黑块。
  code: ({ className, children, ...p }) => {
    const text = String(children ?? '');
    const isBlock = /\n/.test(text.trimEnd()) || /language-/.test(className || '');
    return isBlock ? (
      <code className={`block p-2.5 my-1.5 rounded bg-gray-800 text-gray-100 text-[12px] font-mono overflow-x-auto ${className || ''}`} {...p}>
        {children}
      </code>
    ) : (
      <code className="px-1 py-0.5 rounded bg-gray-200 text-[12px] font-mono" {...p}>
        {children}
      </code>
    );
  },
  table: (p) => (
    <div className="my-2 overflow-x-auto">
      <table className="border-collapse text-[12px] w-full" {...p} />
    </div>
  ),
  thead: (p) => <thead className="bg-gray-100" {...p} />,
  th: (p) => <th className="border border-gray-300 px-2 py-1 text-left font-semibold" {...p} />,
  td: (p) => <td className="border border-gray-300 px-2 py-1" {...p} />,
};

function Markdown({ children }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={MD_COMPONENTS}>
      {children}
    </ReactMarkdown>
  );
}

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
  create_instance: '创建实例',
  start_instance: '启动实例',
  stop_instance: '停止实例',
  failover: '故障转移',
  backup_exec: '立即备份',
  config_set: '修改配置',
  set_access_mode: '设置访问模式',
  replicate: '添加副本',
  backup_set_schedule: '设置备份计划',
};

// 空对话时的快捷入口：按能力大类分组，点分类切换下方预置问题，点问题直接发送。
// 分类贴合后端只读工具集（diagnose/slowlog/bigkeys/list/status/metrics/config/audit）。
// 预置问题刻意不写死具体实例名——面向全部实例或让 AI 先列出再定位，用户点了即可直接跑，
// 无需先改名（避免引用用户环境里并不存在的实例）。
const PROMPT_CATEGORIES = [
  {
    key: 'perf',
    label: '性能排查',
    icon: Gauge,
    prompts: [
      '哪些实例有慢查询？帮我看看有没有耗时的命令',
      '扫描一下哪些实例存在大 key',
      '有没有实例内存碎片率偏高、需要优化',
    ],
  },
  {
    key: 'diagnose',
    label: '异常诊断',
    icon: Stethoscope,
    prompts: [
      '帮我对有异常的实例做一次深度诊断',
      '有没有实例主从复制断开或延迟异常',
      '当前有没有运行状态不正常的实例',
    ],
  },
  {
    key: 'inspect',
    label: '实例巡检',
    icon: ScanSearch,
    prompts: [
      '帮我巡检一下集群整体健康状况',
      '有哪些实例内存水位偏高',
      '列出所有实例及其运行状态',
    ],
  },
  {
    key: 'resource',
    label: '资源查询',
    icon: Database,
    prompts: [
      '列出所有实例及其规格',
      '有哪些实例的逐出策略不是默认值',
      '当前有哪些服务器节点',
    ],
  },
  {
    key: 'event',
    label: '事件分析',
    icon: LineChart,
    prompts: [
      '查一下最近的操作审计记录',
      '最近一段时间集群的内存和 QPS 走势如何',
      '最近有没有故障转移或重启事件',
    ],
  },
  // 运维操作类含写操作，仅在后端 allow_write 开启且当前用户为 admin 时展示（见 capabilities 探测）。
  // 这些问题会让 AI「提议」写操作，仍需用户在确认卡片二次确认后才真正执行。
  // 同样不写死实例名：创建类给规格描述让用户补名字，其余让用户点后自然补充目标实例。
  {
    key: 'ops',
    label: '运维操作',
    icon: Wrench,
    write: true,
    prompts: [
      '帮我创建一个 2Gi 的缓存实例',
      '帮我给某个主从实例加一个副本',
      '把某个实例组切换成读写分离模式',
      '给某个实例设置每天凌晨 2 点自动备份，保留 7 份',
    ],
  },
];

// 多步写操作自动续接的安全上限：单条用户意图链最多自动发起多少轮后续对话。
// 正常场景每步仍需用户点「确认执行」，此上限仅防御异常（如模型反复提议）。
const MAX_AUTO_CONTINUE = 25;

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

// 把后端 tool_calls JSON 字符串解析为数组（渲染留痕用）
function parseToolCalls(raw) {
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export default function AiChatPanel() {
  const [messages, setMessages] = useState([]); // 已完成的对话 [{role, content, tool_calls?, interrupted?}]
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [current, setCurrent] = useState(''); // 当前正在流式输出的助手文本
  const [toolStatus, setToolStatus] = useState(null); // {name, running, ok}
  const [error, setError] = useState('');
  const [pendingConfirm, setPendingConfirm] = useState(null); // {tool, args, summary} 待确认的写操作
  const [executing, setExecuting] = useState(false);
  const [conversations, setConversations] = useState([]); // 会话列表
  const [convId, setConvId] = useState(''); // 当前会话 id，''=新会话未落库
  const [loadingConv, setLoadingConv] = useState(false);
  const [promptCat, setPromptCat] = useState(PROMPT_CATEGORIES[0].key); // 空态选中的预置问题分类
  const [allowWrite, setAllowWrite] = useState(false); // 后端是否允许写操作（总闸 && admin），决定是否展示「运维操作」类
  const scrollRef = useRef(null);
  const abortRef = useRef(null);
  // 多步写操作自动续接的深度计数：每条用户意图链从 0 开始，每自动续接一轮 +1。
  // 纯防御上限，正常每步仍需用户点「确认执行」，几乎不会触顶。
  const continueDepthRef = useRef(0);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, current, toolStatus, error, pendingConfirm]);

  // 流式输出中拦截页面刷新/关闭，避免用户误操作丢失当前回答
  useEffect(() => {
    const handler = (e) => {
      if (streaming) { e.preventDefault(); e.returnValue = ''; }
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [streaming]);

  // 挂载时拉取会话列表 + 探测写操作能力
  useEffect(() => {
    refreshConversations();
    fetchCapabilities();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function fetchCapabilities() {
    try {
      const resp = await fetch('/api/v1/ai/capabilities', { headers: { ...authHeaders() } });
      const body = await resp.json().catch(() => null);
      if (resp.ok && body?.success) setAllowWrite(!!body.data?.allow_write);
    } catch {
      /* 探测失败按无写权限处理，只展示只读分类 */
    }
  }

  async function refreshConversations() {
    try {
      const resp = await fetch('/api/v1/ai/conversations', { headers: { ...authHeaders() } });
      const body = await resp.json().catch(() => null);
      if (resp.ok && body?.success) setConversations(body.data || []);
    } catch {
      /* 会话列表拉取失败不阻断主流程 */
    }
  }

  // 点击会话项：拉取历史并回填
  async function loadConversation(id) {
    if (streaming || id === convId) return;
    setLoadingConv(true);
    setError('');
    try {
      const resp = await fetch(`/api/v1/ai/conversations/${id}`, { headers: { ...authHeaders() } });
      const body = await resp.json().catch(() => null);
      if (!resp.ok || !body?.success) throw new Error(body?.error || `加载失败：${resp.status}`);
      const msgs = (body.data?.messages || []).map((m) => ({
        role: m.role,
        content: m.content,
        tool_calls: parseToolCalls(m.tool_calls),
        interrupted: m.interrupted,
      })).filter((m) => !(m.role === 'user' && m.content.startsWith('[系统提示]')));
      setMessages(msgs);
      setConvId(id);
      setCurrent('');
      setToolStatus(null);
      setPendingConfirm(null);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoadingConv(false);
    }
  }

  // 新建会话：清空对话区
  function newConversation() {
    if (streaming) return;
    setMessages([]);
    setConvId('');
    setCurrent('');
    setToolStatus(null);
    setPendingConfirm(null);
    setError('');
    setInput('');
  }

  // 删除会话
  async function deleteConversation(id, e) {
    e.stopPropagation();
    if (streaming) return;
    try {
      const resp = await fetch(`/api/v1/ai/conversations/${id}`, {
        method: 'DELETE',
        headers: { ...authHeaders() },
      });
      const body = await resp.json().catch(() => null);
      if (!resp.ok || !body?.success) throw new Error(body?.error || `删除失败：${resp.status}`);
      setConversations((cs) => cs.filter((c) => c.id !== id));
      if (id === convId) newConversation();
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  // streamChat 跑一轮完整对话：给定要发给后端的完整 history，处理 SSE 流。
  // 不负责把 user 消息 push 进 messages（由调用方决定是否显示），只在本轮结束时
  // 把 assistant 回复（含工具/写操作留痕）push 进 messages。
  async function streamChat(history) {
    setStreaming(true);
    setCurrent('');
    setToolStatus(null);

    const controller = new AbortController();
    abortRef.current = controller;
    let assistantText = '';
    const toolCalls = []; // 累积本轮工具/写操作留痕，落到 assistant 消息上
    let newConvId = ''; // 若本轮新建会话，记录后端下发的 convId
    try {
      const resp = await fetch('/api/v1/ai/chat', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history, conversation_id: convId }),
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
          if (event === 'conversation') {
            // 新建会话：后端下发 convId，绑定并稍后刷新侧栏
            newConvId = data.id || '';
            if (newConvId) setConvId(newConvId);
          } else if (event === 'token') {
            assistantText += data.text || '';
            setCurrent(assistantText);
            setToolStatus(null);
          } else if (event === 'tool_call') {
            setToolStatus({ name: data.name, running: true });
            toolCalls.push({ type: 'tool_call', tool: data.name, args: data.args });
          } else if (event === 'tool_result') {
            setToolStatus({ name: data.name, running: false, ok: data.ok });
          } else if (event === 'confirm') {
            // 模型提议写操作：不执行，弹出二次确认卡片（摘要由后端可信生成）
            setPendingConfirm({ tool: data.tool, args: data.args, summary: data.summary });
            toolCalls.push({ type: 'write_confirm', tool: data.tool, summary: data.summary });
          } else if (event === 'error') {
            throw new Error(data.message || 'AI 服务出错');
          }
        }
      }
      if (assistantText || toolCalls.length) {
        setMessages((m) => [...m, { role: 'assistant', content: assistantText, tool_calls: toolCalls }]);
      }
    } catch (e) {
      if (e.name !== 'AbortError') setError(e.message || String(e));
      else if (assistantText || toolCalls.length) {
        // 用户中断：保留已生成的部分并标记 interrupted
        setMessages((m) => [...m, { role: 'assistant', content: assistantText, tool_calls: toolCalls, interrupted: true }]);
      }
    } finally {
      setStreaming(false);
      setCurrent('');
      setToolStatus(null);
      abortRef.current = null;
      // 新建会话后刷新侧栏，使其出现在列表；续聊也刷新以更新 updated_at 排序
      refreshConversations();
    }
  }

  // 把已完成消息过滤成发给后端的 history（去掉本地 system 回显，避免污染上下文）。
  function historyFromMessages() {
    return messages.filter((m) => m.role !== 'system').map((m) => ({ role: m.role, content: m.content }));
  }

  async function send(text) {
    const q = (text ?? input).trim();
    if (!q || streaming) return;
    setError('');
    setInput('');
    setPendingConfirm(null); // 新一轮对话，清掉上一个未处理的确认
    continueDepthRef.current = 0; // 用户主动发问：重置自动续接链计数
    const history = [...historyFromMessages(), { role: 'user', content: q }];
    setMessages((m) => [...m, { role: 'user', content: q }]);
    await streamChat(history);
  }

  // continueAfterExecute 在写操作执行成功后自动发起新一轮对话，让 AI 接续多步意图的
  // 下一步（如主从创建：主库建好→自动续→AI 提议建从库）。续接的提示消息只作为本次
  // 请求 history 的最后一条 user 消息发给后端，不 push 进 messages、界面不显示——
  // 用户看到的是「✅ 已执行」灰条 + AI 自动接上的下一轮回复。
  async function continueAfterExecute(summary) {
    if (continueDepthRef.current >= MAX_AUTO_CONTINUE) {
      // 防御：异常情况下链条过长则停止自动续接，避免失控。
      setMessages((m) => [...m, { role: 'system', content: '已达到自动续接上限，如需继续请手动输入。' }]);
      return;
    }
    continueDepthRef.current += 1;
    const followup =
      `[系统提示] 上一步写操作已成功执行：${summary}。` +
      `如果用户最初的意图还需要后续写操作才能完成（例如“创建主从实例”在主库创建后还需创建从库并建立复制），` +
      `请继续提议下一步并简要说明；如果整个意图已经完成，请简要总结，不要再提议任何写操作。`;
    const history = [...historyFromMessages(), { role: 'user', content: followup }];
    await streamChat(history);
  }

  function stop() {
    abortRef.current?.abort();
  }

  // 用户点「确认执行」：调独立执行端点（不走 LLM）执行写操作。
  // 执行成功后自动发起新一轮对话（continueAfterExecute），让 AI 接续多步意图的下一步；
  // 失败则就地报错、不续接。
  async function confirmExecute() {
    if (!pendingConfirm || executing) return;
    setExecuting(true);
    setError('');
    const summary = pendingConfirm.summary;
    let argsObj = {};
    try {
      argsObj = pendingConfirm.args ? JSON.parse(pendingConfirm.args) : {};
    } catch {
      argsObj = {};
    }
    try {
      const resp = await fetch('/api/v1/ai/execute', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ tool: pendingConfirm.tool, args: argsObj, conversation_id: convId }),
      });
      const body = await resp.json().catch(() => null);
      if (!resp.ok) throw new Error(body?.error || `执行失败：${resp.status}`);
      setMessages((m) => [...m, { role: 'system', content: `已完成 · ${summary}` }]);
      setPendingConfirm(null);
      setExecuting(false);
      // 执行成功：自动发起下一轮，让 AI 判断是否还有后续步骤（多步写自动续接）。
      await continueAfterExecute(summary);
      return;
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setExecuting(false);
    }
  }

  function cancelConfirm() {
    if (executing) return;
    setMessages((m) => [...m, { role: 'system', content: `已取消：${pendingConfirm.summary}` }]);
    setPendingConfirm(null);
  }

  function onKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  }

  const empty = messages.length === 0 && !streaming;

  return (
    <div className="border border-gray-100 rounded-lg bg-white flex h-[calc(100vh-180px)] min-h-[480px] overflow-hidden">
      {/* 左侧会话列表侧栏 */}
      <aside className="w-60 shrink-0 border-r border-gray-100 flex flex-col bg-gray-50/60">
        <div className="p-3 border-b border-gray-100">
          <button
            onClick={newConversation}
            disabled={streaming}
            className="w-full h-9 rounded-md bg-gray-900 text-white inline-flex items-center justify-center gap-1.5 text-[13px] cursor-pointer hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed"
          >
            <Plus size={15} /> 新建会话
          </button>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {conversations.length === 0 ? (
            <div className="px-3 py-6 text-center text-xs text-gray-400">暂无历史会话</div>
          ) : (
            conversations.map((c) => (
              <div
                key={c.id}
                onClick={() => loadConversation(c.id)}
                className={`group flex items-center gap-2 px-3 py-2 mx-1.5 my-0.5 rounded-md cursor-pointer text-[13px] ${
                  c.id === convId ? 'bg-gray-200 text-gray-900' : 'text-gray-600 hover:bg-gray-100'
                }`}
              >
                <MessageSquare size={14} className="shrink-0 text-gray-400" />
                <span className="flex-1 truncate">{c.title || '未命名会话'}</span>
                <button
                  onClick={(e) => deleteConversation(c.id, e)}
                  disabled={streaming}
                  className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-red-500 shrink-0 disabled:cursor-not-allowed"
                  title="删除会话"
                >
                  <Trash2 size={14} />
                </button>
              </div>
            ))
          )}
        </div>
      </aside>

      {/* 右侧对话区 */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* 消息区 */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto px-5 py-4">
          <div className="max-w-5xl mx-auto space-y-4">
          {loadingConv && (
            <div className="inline-flex items-center gap-2 text-gray-400 text-[13px]">
              <Loader2 size={15} className="animate-spin" /> 加载会话历史…
            </div>
          )}

          {empty && !loadingConv && (
            <div className="pt-2">
              {/* 标题 */}
              <h2 className="text-[22px] font-semibold text-gray-900 mb-5">
                你好，我是 <span className="text-blue-600">AI 运维助手</span>
              </h2>

              {/* 分类：左对齐网格，选中项淡色底。写操作类仅在 allowWrite 时展示 */}
              <div className="text-[13px] text-gray-400 mb-3">我可以帮您</div>
              <div className="grid grid-cols-3 gap-y-3 gap-x-4 mb-5">
                {PROMPT_CATEGORIES.filter((cat) => !cat.write || allowWrite).map((cat) => {
                  const Icon = cat.icon;
                  const active = cat.key === promptCat;
                  return (
                    <button
                      key={cat.key}
                      onClick={() => setPromptCat(cat.key)}
                      className={`inline-flex items-center gap-2 px-3 py-2 rounded-lg text-[14px] cursor-pointer transition-colors justify-self-start ${
                        active
                          ? 'bg-blue-50 text-blue-600 font-medium'
                          : 'text-gray-600 hover:bg-gray-50'
                      }`}
                    >
                      <Icon size={16} className={active ? 'text-blue-500' : 'text-gray-400'} /> {cat.label}
                    </button>
                  );
                })}
              </div>

              {/* 选中分类的预置问题：通栏宽卡片 */}
              <div className="flex flex-col gap-2.5">
                {(PROMPT_CATEGORIES.find((c) => c.key === promptCat)?.prompts || []).map((p) => (
                  <button
                    key={p}
                    onClick={() => send(p)}
                    className="w-full text-left text-[14px] text-gray-700 border border-gray-200 rounded-xl px-4 py-3.5 bg-white hover:bg-gray-50 hover:border-gray-300 cursor-pointer transition-colors"
                  >
                    {p}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((m, i) => (
            <MessageBubble key={i} role={m.role} content={m.content} toolCalls={m.tool_calls} interrupted={m.interrupted} />
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

          {/* 写操作二次确认卡片：模型提议 → 用户人工确认后才执行 */}
          {pendingConfirm && (
            <ConfirmCard
              summary={pendingConfirm.summary}
              executing={executing}
              onConfirm={confirmExecute}
              onCancel={cancelConfirm}
            />
          )}

          {error && (
            <div className="border border-red-200 rounded-md px-3 py-2.5 flex items-center gap-2 bg-red-50 text-red-700 text-[13px]">
              <AlertCircle size={16} className="shrink-0" /> {error}
            </div>
          )}
          </div>
        </div>

        {/* 输入区 */}
        <div className="p-3.5">
          <div className="max-w-3xl mx-auto flex items-end gap-2">
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
    </div>
  );
}

// ToolCallTrace 渲染 assistant 消息里持久化的工具调用/写操作留痕（§13.8）。
// 竖排一行一个，点击可展开查看该次调用的参数（tool_call/write_confirm）。
function ToolCallTrace({ calls }) {
  const [open, setOpen] = useState({}); // 索引 → 是否展开
  if (!calls || calls.length === 0) return null;

  // 把 args 规整为可读的 JSON 文本（后端 args 可能是 JSON 字符串或对象）
  const prettyArgs = (args) => {
    if (args == null || args === '') return '';
    let obj = args;
    if (typeof args === 'string') {
      try {
        obj = JSON.parse(args);
      } catch {
        return args; // 非 JSON 字符串，原样展示
      }
    }
    try {
      return JSON.stringify(obj, null, 2);
    } catch {
      return String(args);
    }
  };

  return (
    <div className="flex flex-col gap-0.5 mt-2">
      {calls.map((el, i) => {
        const label = TOOL_LABELS[el.tool] || el.tool;
        const isConfirm = el.type === 'write_confirm';
        const isExecute = el.type === 'write_execute';
        // 可展开内容：tool_call 看参数；write_confirm 看摘要；write_execute 无更多信息
        const detail = isConfirm ? el.summary : prettyArgs(el.args);
        const expandable = !!detail;
        const isOpen = !!open[i];

        let Icon = Wrench;
        let iconCls = 'text-gray-400';
        let head = `已使用工具: ${label}`;
        if (isExecute) {
          Icon = CheckCircle2;
          iconCls = 'text-emerald-500';
          head = `已执行 ${label}`;
        } else if (isConfirm) {
          Icon = ShieldAlert;
          iconCls = 'text-amber-500';
          head = `提议 ${label}`;
        }

        return (
          <div key={i} className="text-[12px]">
            <button
              type="button"
              onClick={() => expandable && setOpen((o) => ({ ...o, [i]: !o[i] }))}
              className={`inline-flex items-center gap-1.5 text-gray-600 ${expandable ? 'cursor-pointer hover:text-gray-900' : 'cursor-default'}`}
            >
              <Icon size={13} className={iconCls} />
              <span>{head}</span>
              {expandable && <ChevronDown size={13} className={`text-gray-400 transition-transform ${isOpen ? 'rotate-180' : ''}`} />}
            </button>
            {expandable && isOpen && (
              <div className="mt-1 mb-1.5 ml-5">
                <div className="text-[11px] text-gray-400 mb-1">{isConfirm ? '摘要' : '参数'}</div>
                {isConfirm ? (
                  <div className="px-2.5 py-2 rounded bg-gray-50 text-[12px] text-gray-700 leading-relaxed">
                    {detail}
                  </div>
                ) : (
                  <pre className="px-2.5 py-2 rounded bg-gray-50 text-[11px] font-mono text-gray-700 overflow-x-auto whitespace-pre-wrap break-words">{detail}</pre>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function MessageBubble({ role, content, toolCalls, interrupted }) {
  if (role === 'system') {
    // 写操作执行/取消的结果回显（不参与后续对话）
    return (
      <div className="flex justify-center">
        <div className="max-w-[90%] rounded-md px-3 py-1.5 text-xs text-gray-500 bg-gray-100">
          {content}
        </div>
      </div>
    );
  }
  const isUser = role === 'user';
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div className={isUser ? 'max-w-[85%]' : 'w-full'}>
        {content &&
          (isUser ? (
            // 用户消息：淡蓝气泡（对齐 Tair）
            <div className="rounded-2xl px-3.5 py-2.5 text-[15px] leading-relaxed break-words whitespace-pre-wrap bg-blue-50 text-gray-800">
              {content}
            </div>
          ) : (
            // AI 回复：无气泡、无边框，纯文本铺开（对齐 Tair）
            <div className="text-[15px] leading-relaxed break-words text-gray-800">
              <Markdown>{content}</Markdown>
            </div>
          ))}
        {!isUser && <ToolCallTrace calls={toolCalls} />}
        {interrupted && (
          <div className="text-[11px] text-gray-400 mt-1 inline-flex items-center gap-1">
            <Square size={10} /> 该回复已中断
          </div>
        )}
      </div>
    </div>
  );
}

// ConfirmCard 展示 AI 提议的写操作，需用户人工点击确认后才经 /ai/execute 执行。
function ConfirmCard({ summary, executing, onConfirm, onCancel }) {
  return (
    <div className="border border-amber-300 bg-amber-50 rounded-lg px-4 py-3.5">
      <div className="flex items-center gap-2 mb-1.5">
        <ShieldAlert size={16} className="text-amber-600" />
        <span className="text-[13px] font-semibold text-amber-800">AI 提议执行写操作</span>
        <span className="ml-auto text-[11px] font-medium text-amber-700 bg-amber-100 border border-amber-300 rounded px-1.5 py-0.5">
          WRITE
        </span>
      </div>
      <div className="text-[13px] text-gray-800 mb-3 pl-6">{summary}</div>
      <div className="flex gap-2 pl-6">
        <button
          onClick={onConfirm}
          disabled={executing}
          className="px-3.5 h-8 rounded-md bg-amber-600 text-white text-[13px] inline-flex items-center gap-1.5 cursor-pointer hover:bg-amber-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {executing ? <Loader2 size={14} className="animate-spin" /> : <CheckCircle2 size={14} />}
          {executing ? '执行中…' : '确认执行'}
        </button>
        <button
          onClick={onCancel}
          disabled={executing}
          className="px-3.5 h-8 rounded-md bg-white text-gray-700 border border-gray-300 text-[13px] cursor-pointer hover:bg-gray-50 disabled:opacity-50"
        >
          取消
        </button>
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
