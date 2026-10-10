// AI 回复的工具留痕摘要（纯逻辑，无 React 依赖，便于单测）。
//
// tool_calls 元素类型：
//   tool_call      {tool, args, ts?, ok?, result_summary?}   只读工具调用
//   write_confirm  {tool, summary, ts?}                      AI 提议的写操作
//   write_execute  {tool, task_id}                           用户确认后已执行
//   run_meta       {started_at, finished_at}                 本轮起止时间（毫秒），老消息没有

// isFailed 判断一次工具调用是否失败（兼容老消息只有 result_summary 的情况）。
export function isFailed(call) {
  if (call.ok === false) return true;
  return typeof call.result_summary === 'string' && call.result_summary.startsWith('error');
}

// summarizeTrace 把留痕整理成折叠摘要：
//   groups   连续相同的工具调用合并为一组 {type, tool, count, failed, items}
//   total    只读工具调用总次数
//   failed   失败次数
//   writes   写操作提议/执行（数量少且关键，折叠时也单独展示）
//   durationMs 本轮耗时；老消息无 run_meta 时为 null
export function summarizeTrace(calls) {
  const list = Array.isArray(calls) ? calls : [];
  const meta = list.find((c) => c && c.type === 'run_meta');
  const groups = [];
  const writes = [];
  let total = 0;
  let failed = 0;
  for (const c of list) {
    if (!c || c.type === 'run_meta') continue;
    if (c.type === 'write_confirm' || c.type === 'write_execute') {
      writes.push(c);
      continue;
    }
    total += 1;
    const bad = isFailed(c);
    if (bad) failed += 1;
    const last = groups[groups.length - 1];
    if (last && last.tool === c.tool) {
      last.count += 1;
      if (bad) last.failed += 1;
      last.items.push(c);
    } else {
      groups.push({ tool: c.tool, count: 1, failed: bad ? 1 : 0, items: [c] });
    }
  }
  const durationMs =
    meta && Number.isFinite(meta.started_at) && Number.isFinite(meta.finished_at)
      ? Math.max(0, meta.finished_at - meta.started_at)
      : null;
  return { groups, writes, total, failed, durationMs };
}

// formatDuration 毫秒 → 「12 秒」/「1 分 51 秒」；不足 1 秒按 1 秒。
export function formatDuration(ms) {
  if (!Number.isFinite(ms)) return '';
  const sec = Math.max(1, Math.round(ms / 1000));
  if (sec < 60) return `${sec} 秒`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return s ? `${m} 分 ${s} 秒` : `${m} 分`;
}

// formatClock 毫秒 → 运行中计时「0:42」/「1:05:03」。
export function formatClock(ms) {
  const total = Math.max(0, Math.floor((ms || 0) / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = String(total % 60).padStart(2, '0');
  return h ? `${h}:${String(m).padStart(2, '0')}:${s}` : `${m}:${s}`;
}
