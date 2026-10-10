import { useState, useRef, useEffect } from 'react';
import { useAiChat, useAiChatOptional } from './AiChatProvider.jsx';
import { summarizeTrace, formatDuration, formatClock, isFailed } from '../lib/aiTrace.js';
import { Sparkles, Send, Loader2, Wrench, CheckCircle2, AlertCircle, Square, ShieldAlert, Plus, Trash2, MessageSquare, Gauge, Stethoscope, ScanSearch, Database, LineChart, ChevronDown } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

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
  query_tasks: '查询运维任务',
  get_task_detail: '查看任务详情',
  query_incidents: '查询异常记录',
  describe_key: '查看 Key 信息',
  get_key: '读取 Key 内容',
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
  resize_instance: '调整实例规格',
  scan_bigkeys: '发起大 key 扫描',
  cancel_task: '取消任务',
  write_key: '写入 Key',
  delete_key: '删除 Key',
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

// 会话状态全部在 AiChatProvider（挂在 AppShell 上），本组件只负责展示与交互：
// 切 tab 卸载本组件不影响后台 run 与流式状态，切回来即是离开时的样子。
export default function AiChatPanel() {
  const {
    conversations, convId, messages, streaming, current, toolStatus, liveStats, error, pendingConfirm,
    executing, loadingConv, allowWrite,
    send, stop, loadConversation, newConversation, deleteConversation, confirmExecute, cancelConfirm,
    conversationStatus,
  } = useAiChat();
  const [input, setInput] = useState('');
  const [promptCat, setPromptCat] = useState(PROMPT_CATEGORIES[0].key); // 空态选中的预置问题分类
  const scrollRef = useRef(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, current, toolStatus, error, pendingConfirm, convId]);

  async function submit(text) {
    const q = (text ?? input).trim();
    if (!q || streaming) return;
    setInput('');
    const ok = await send(q);
    if (!ok && text == null) setInput(q); // 发送失败（如会话正在回复中）把输入还给用户
  }

  function onKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      submit();
    }
  }

  const empty = messages.length === 0 && !streaming && !loadingConv;

  return (
    <div className="border border-gray-100 rounded-lg bg-white flex h-[calc(100vh-180px)] min-h-[480px] overflow-hidden">
      {/* 左侧会话列表侧栏 */}
      <aside className="w-60 shrink-0 border-r border-gray-100 flex flex-col bg-gray-50/60">
        <div className="p-3 border-b border-gray-100">
          <button
            onClick={newConversation}
            className="w-full h-9 rounded-md bg-gray-900 text-white inline-flex items-center justify-center gap-1.5 text-[13px] cursor-pointer hover:bg-gray-800"
          >
            <Plus size={15} /> 新建会话
          </button>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {conversations.length === 0 ? (
            <div className="px-3 py-6 text-center text-xs text-gray-400">暂无历史会话</div>
          ) : (
            conversations.map((c) => {
              const st = conversationStatus(c);
              return (
                <div
                  key={c.id}
                  onClick={() => loadConversation(c.id)}
                  className={`group flex items-center gap-2 px-3 py-2 mx-1.5 my-0.5 rounded-md cursor-pointer text-[13px] ${
                    c.id === convId ? 'bg-gray-200 text-gray-900' : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  {st.running ? (
                    <Loader2 size={14} className="shrink-0 text-blue-500 animate-spin" aria-label="回复中" />
                  ) : st.pending ? (
                    <ShieldAlert size={14} className="shrink-0 text-amber-500" aria-label="待确认" />
                  ) : (
                    <MessageSquare size={14} className="shrink-0 text-gray-400" />
                  )}
                  <span className="flex-1 truncate" title={st.running ? '回复中' : st.pending ? '有待确认的写操作' : undefined}>
                    {c.title || '未命名会话'}
                  </span>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      deleteConversation(c.id);
                    }}
                    className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-red-500 shrink-0"
                    title={st.running ? '删除会话（将停止正在进行的回复）' : '删除会话'}
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              );
            })
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

          {empty && (
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
                    onClick={() => submit(p)}
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
              {toolStatus && <ToolStatus status={toolStatus} stats={liveStats} />}
              {current ? (
                <MessageBubble role="assistant" content={current} />
              ) : (
                !toolStatus && (
                  <div className="inline-flex items-center gap-2 text-gray-400 text-[13px]">
                    <Loader2 size={15} className="animate-spin" /> 正在思考…
                    <LiveMeta stats={liveStats} />
                  </div>
                )
              )}
            </div>
          )}

          {/* 写操作二次确认卡片：模型提议 → 用户人工确认后才执行。刷新页面后由后端恢复 */}
          {pendingConfirm && !streaming && (
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
              aria-label="输入问题"
              placeholder={streaming ? 'AI 正在回复，可随时切换页面，回复会在后台继续…' : '输入问题，Enter 发送，Shift+Enter 换行'}
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
                onClick={() => submit()}
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

// AiNavIndicator 主导航「AI 助手」项上的状态：任一会话回复中→转圈；有待确认写操作→琥珀点。
// 放在本文件以便与面板的状态展示保持一致；Provider 外渲染时不显示。
export function AiNavIndicator({ active }) {
  const ai = useAiChatOptional();
  if (!ai) return null;
  if (ai.anyRunning) {
    return <Loader2 size={14} className={`ml-auto animate-spin ${active ? 'text-white' : 'text-blue-500'}`} aria-label="AI 回复中" />;
  }
  if (ai.anyPending) {
    return <span className="ml-auto w-2 h-2 rounded-full bg-amber-500" aria-label="AI 有待确认的写操作" />;
  }
  return null;
}

// prettyArgs 把 args 规整为可读的 JSON 文本（后端 args 可能是 JSON 字符串或对象）。
function prettyArgs(args) {
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
}

// ToolCallTrace 渲染 assistant 消息里持久化的工具调用/写操作留痕（§13.8、§14.6）。
// 默认折叠成一行摘要「已处理 X 秒 · 调用 N 次工具（M 次失败）」，点开是按时间顺序的
// 时间线，连续相同工具合并为「×N」，再点某组看每次调用的参数与结果。
// 写操作提议/执行数量少且关键，始终在摘要下方单独展示。
function ToolCallTrace({ calls }) {
  const [open, setOpen] = useState(false);
  const [openGroup, setOpenGroup] = useState({});
  const { groups, writes, total, failed, durationMs } = summarizeTrace(calls);
  if (groups.length === 0 && writes.length === 0) return null;

  const head = [durationMs != null ? `已处理 ${formatDuration(durationMs)}` : null, total ? `调用 ${total} 次工具` : null]
    .filter(Boolean)
    .join(' · ');

  return (
    <div className="flex flex-col gap-0.5 mt-2 text-[12px]">
      {total > 0 && (
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
          className="inline-flex items-center gap-1.5 text-gray-500 hover:text-gray-900 cursor-pointer w-fit"
        >
          {failed ? <AlertCircle size={13} className="text-red-500" /> : <Wrench size={13} className="text-gray-400" />}
          <span>{head}</span>
          {failed > 0 && <span className="text-red-600">（{failed} 次失败）</span>}
          <ChevronDown size={13} className={`text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} />
        </button>
      )}

      {open && (
        <div className="ml-1.5 pl-3 border-l border-gray-200 flex flex-col gap-0.5 my-1">
          {groups.map((g, i) => {
            const label = TOOL_LABELS[g.tool] || g.tool;
            const gOpen = !!openGroup[i];
            return (
              <div key={i}>
                <button
                  type="button"
                  onClick={() => setOpenGroup((o) => ({ ...o, [i]: !o[i] }))}
                  aria-expanded={gOpen}
                  className="inline-flex items-center gap-1.5 text-gray-600 hover:text-gray-900 cursor-pointer"
                >
                  {g.failed ? <AlertCircle size={12} className="text-red-500" /> : <CheckCircle2 size={12} className="text-gray-300" />}
                  <span>{label}</span>
                  {g.count > 1 && <span className="text-gray-400">×{g.count}</span>}
                  {g.failed > 0 && <span className="text-red-600">{g.failed} 次失败</span>}
                  <ChevronDown size={12} className={`text-gray-400 transition-transform ${gOpen ? 'rotate-180' : ''}`} />
                </button>
                {gOpen && (
                  <div className="ml-5 mt-1 mb-1.5 flex flex-col gap-1">
                    {g.items.map((c, j) => {
                      const detail = prettyArgs(c.args);
                      const bad = isFailed(c);
                      return (
                        <div key={j}>
                          {g.count > 1 && <div className="text-[11px] text-gray-400 mb-0.5">第 {j + 1} 次{bad ? ' · 失败' : ''}</div>}
                          {detail && (
                            <pre className="px-2.5 py-2 rounded bg-gray-50 text-[11px] font-mono text-gray-700 overflow-x-auto whitespace-pre-wrap break-words">{detail}</pre>
                          )}
                          {bad && c.result_summary && <div className="text-[11px] text-red-600 mt-0.5 break-words">{c.result_summary}</div>}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {writes.map((w, i) => {
        const label = TOOL_LABELS[w.tool] || w.tool;
        const isExecute = w.type === 'write_execute';
        return (
          <div key={`w${i}`} className="inline-flex items-start gap-1.5 text-gray-600">
            {isExecute ? (
              <CheckCircle2 size={13} className="text-emerald-500 mt-px shrink-0" />
            ) : (
              <ShieldAlert size={13} className="text-amber-500 mt-px shrink-0" />
            )}
            <span>
              {isExecute ? `已执行 ${label}` : `提议 ${label}`}
              {!isExecute && w.summary && <span className="text-gray-400"> · {w.summary}</span>}
            </span>
          </div>
        );
      })}
    </div>
  );
}

// useElapsed 运行中每秒刷新一次的已用时长（毫秒）。
function useElapsed(startedAt) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!startedAt) return undefined;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [startedAt]);
  return startedAt ? Math.max(0, now - startedAt) : null;
}

// LiveMeta 运行中附加信息「· 已调用 N 次 · 0:42」。
function LiveMeta({ stats }) {
  const elapsed = useElapsed(stats?.startedAt);
  if (!stats) return null;
  return (
    <span className="text-gray-400">
      {stats.calls > 0 && ` · 已调用 ${stats.calls} 次`}
      {elapsed != null && ` · ${formatClock(elapsed)}`}
    </span>
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

function ToolStatus({ status, stats }) {
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
      <LiveMeta stats={stats} />
    </div>
  );
}
