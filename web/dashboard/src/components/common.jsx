// 共享 UI 原子组件：状态徽标、图标按钮、下拉菜单、进度条、空态、可复制文本等，
// 被多个页面和弹窗复用。业务/格式化逻辑从 lib/ 引入，保持组件层无重复计算。
import { useState, useEffect, useRef } from 'react';
import { ChevronDown, Key, LogOut, MoreVertical, Database, Check, Copy } from 'lucide-react';
import { statusMeta, formatBytes } from '../lib/format.js';
import { categoryLabel } from '../lib/instance.js';

// 状态色板：ok/warn/bad/neutral 四档，供状态徽标与圆点统一取色。
export const dotColor = { ok: 'bg-emerald-400', warn: 'bg-amber-500', bad: 'bg-red-500', neutral: 'bg-gray-300' };

export function StatusBadge({ status, children, metaFn = statusMeta }) {
  const meta = metaFn(status);
  const textTone = { ok: 'text-gray-500', warn: 'text-amber-600', bad: 'text-red-600', neutral: 'text-gray-500' };
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium whitespace-nowrap ${textTone[meta.tone] || textTone.neutral}`}>
      <span className={`w-2 h-2 rounded-full shrink-0 ${dotColor[meta.tone] || dotColor.neutral}`} />
      {children || meta.label}
    </span>
  );
}

export function StatusDot({ status }) {
  const meta = statusMeta(status);
  return <span className={`w-[7px] h-[7px] rounded-full shrink-0 ${dotColor[meta.tone] || dotColor.neutral}`} />;
}

export function IconButton({ icon: Icon, label, onClick, disabled = false }) {
  return (
    <button
      className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 hover:border-gray-300 hover:text-gray-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      disabled={disabled}
      title={typeof disabled === 'string' ? disabled : disabled ? `${label}暂未开放` : label}
      aria-label={label}
      onClick={onClick}
      type="button"
    >
      <Icon size={16} />
    </button>
  );
}

export function UserMenu({ username, onLogout, onNavigate }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (!open) return;
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  return (
    <div className="relative inline-flex" ref={ref}>
      <button
        type="button"
        className="h-[34px] rounded-md border border-gray-200 px-3 bg-white text-gray-700 font-medium text-[13px] inline-flex items-center gap-1.5 hover:bg-gray-50 hover:border-gray-300 transition-colors cursor-pointer"
        onClick={() => setOpen(!open)}
      >
        {username}
        <ChevronDown size={14} className={`text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-[calc(100%+4px)] z-50 min-w-[120px] bg-white border border-gray-200 rounded-md shadow-lg py-1">
          <button
            className="block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer text-gray-700 hover:bg-gray-100"
            onClick={() => { setOpen(false); onNavigate('tokens'); }}
          >
            <Key size={13} className="inline -mt-0.5 mr-1.5" />密钥管理
          </button>
          <button
            className="block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer text-red-600 hover:bg-red-50"
            onClick={() => { setOpen(false); onLogout(); }}
          >
            <LogOut size={13} className="inline -mt-0.5 mr-1.5" />退出登录
          </button>
        </div>
      )}
    </div>
  );
}

export function MoreMenu({ items }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const btnRef = useRef(null);
  const [pos, setPos] = useState({ top: 0, right: 0 });

  useEffect(() => {
    if (!open) return;
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  function toggle() {
    if (!open && btnRef.current) {
      const rect = btnRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 4, right: window.innerWidth - rect.right });
    }
    setOpen(!open);
  }

  const visibleItems = items.filter(Boolean);
  if (visibleItems.length === 0) return null;

  return (
    <div className="relative inline-flex" ref={ref}>
      <button ref={btnRef} className="w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-500 inline-flex items-center justify-center hover:bg-gray-50 hover:border-gray-300 hover:text-gray-900 transition-colors" title="更多操作" aria-label="更多操作" type="button" onClick={toggle}>
        <MoreVertical size={16} />
      </button>
      {open && (
        <div className="fixed z-50 min-w-[100px] bg-white border border-gray-200 rounded-md shadow-lg py-1" style={{ top: pos.top, right: pos.right }}>
          {visibleItems.map((mi, i) => (
            <button
              key={i}
              className={`block w-full px-3.5 py-1.5 text-left text-[13px] whitespace-nowrap border-0 bg-transparent cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${mi.danger ? 'text-red-600 hover:bg-red-50' : 'text-gray-700 hover:bg-gray-100'}`}
              disabled={mi.disabled}
              onClick={() => { setOpen(false); mi.onClick(); }}
            >
              {mi.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export function ProgressBar({ label, current, max, unit = '' }) {
  const percent = max > 0 ? Math.min(100, Math.round((current / max) * 100)) : 0;
  const barColor = percent >= 90 ? 'bg-red-500' : percent >= 80 ? 'bg-amber-500' : 'bg-gray-900';
  return (
    <div className="grid gap-1.5">
      <div className="flex items-center justify-between gap-3 text-xs text-gray-500">
        <span className="inline-flex items-center gap-1.5">{label}</span>
        <span className="font-mono tabular-nums">{current || 0}{unit} / {max || 0}{unit}</span>
      </div>
      <div className="h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <span className={`block h-full rounded-full transition-all duration-300 ${barColor}`} style={{ width: `${percent}%` }} />
      </div>
    </div>
  );
}

export function EmptyState({ icon: Icon = Database, title, detail }) {
  return (
    <div className="min-h-[200px] p-7 grid place-items-center content-center gap-2 text-center text-gray-400">
      <Icon size={36} className="text-gray-300" />
      <strong className="text-gray-500 font-medium">{title}</strong>
      {detail && <span className="text-[13px]">{detail}</span>}
    </div>
  );
}

export function SummaryCell({ label, value, icon: Icon, tone = 'neutral' }) {
  const toneStyles = {
    neutral: '',
    warn: '',
    alert: 'relative bg-red-50',
  };
  const valueTone = {
    neutral: 'text-gray-900',
    warn: 'text-amber-600',
    alert: 'text-red-700',
  };
  const labelTone = {
    neutral: 'text-gray-400',
    warn: 'text-gray-400',
    alert: 'text-red-600',
  };
  return (
    <div className={`px-5 py-4 border-r border-gray-200 last:border-r-0 flex flex-col gap-2.5 max-md:border-r-0 max-md:border-b max-md:last:border-b-0 ${toneStyles[tone] || ''}`}>
      {tone === 'alert' && <span className="absolute inset-x-0 top-0 h-0.5 bg-red-500" />}
      <span className={`inline-flex items-center gap-1.5 text-[13px] font-medium ${labelTone[tone] || 'text-gray-400'}`}><Icon size={15} /> {label}</span>
      <span className={`font-mono text-[34px] font-semibold leading-none tracking-tight tabular-nums ${valueTone[tone] || 'text-gray-900'}`}>{value}</span>
    </div>
  );
}

// 引擎品牌图标：Redis 红色方块 / Kvrocks 蓝色方块，其它引擎回退到通用数据库图标
export function EngineIcon({ engine, size = 20 }) {
  const key = String(engine || '').toLowerCase();
  if (key === 'redis') {
    return (
      <span className="inline-flex items-center justify-center rounded-md shrink-0" style={{ width: size, height: size, background: '#DC382D' }} title="Redis">
        <svg viewBox="0 0 24 24" width={size * 0.62} height={size * 0.62} fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <ellipse cx="12" cy="5.5" rx="8" ry="3" />
          <path d="M4 5.5v6c0 1.66 3.58 3 8 3s8-1.34 8-3v-6" />
          <path d="M4 11.5v6c0 1.66 3.58 3 8 3s8-1.34 8-3v-6" />
        </svg>
      </span>
    );
  }
  if (key === 'kvrocks') {
    return (
      <span className="inline-flex items-center justify-center rounded-md shrink-0 text-white font-bold" style={{ width: size, height: size, background: '#3B7EF0', fontSize: size * 0.56, lineHeight: 1 }} title="Kvrocks">K</span>
    );
  }
  return (
    <span className="inline-flex items-center justify-center rounded-md shrink-0 bg-gray-100 text-gray-500" style={{ width: size, height: size }}>
      <Database size={size * 0.6} />
    </span>
  );
}

// 类型标签：缓存（绿）/ 持久化（蓝）
export function CategoryBadge({ category }) {
  if (!category) return <span className="text-gray-300 text-[13px]">-</span>;
  const tone = category === 'persistent'
    ? 'bg-blue-50 text-blue-600 border-blue-100'
    : 'bg-emerald-50 text-emerald-600 border-emerald-100';
  return (
    <span className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[12px] font-medium ${tone}`}>
      {categoryLabel(category)}
    </span>
  );
}

// 内存使用：以百分比为主，下方附带 已用/总量 细节
export function MemoryUsage({ memory, memoryUsed }) {
  const totalBytes = memory * 1024 * 1024 * 1024;
  const pct = totalBytes > 0 && memoryUsed > 0 ? (memoryUsed / totalBytes) * 100 : 0;
  const pctLabel = pct >= 10 ? pct.toFixed(0) : pct > 0 ? pct.toFixed(2) : '0';
  const usedLabel = memoryUsed > 0 ? formatBytes(memoryUsed) : '0 MB';
  const totalLabel = formatBytes(totalBytes);
  const tone = pct > 80 ? 'bg-red-500' : pct > 60 ? 'bg-amber-500' : 'bg-emerald-500';
  return (
    <div className="flex flex-col gap-1 min-w-[120px]">
      <div className="flex items-center gap-2">
        <span className="font-semibold tabular-nums text-[13px] text-gray-900 shrink-0">{pctLabel}%</span>
        <div className="flex-1 h-1 rounded-full bg-gray-100 overflow-hidden">
          <div className={`h-full rounded-full transition-all duration-300 ${tone}`} style={{ width: `${pct > 0 ? Math.max(Math.min(pct, 100), 4) : 0}%` }} />
        </div>
      </div>
      <span className="tabular-nums text-[11px] text-gray-400">{usedLabel} / {totalLabel}</span>
    </div>
  );
}

export function CopyableText({ text, className = '', children }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(event) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {}
  }

  return (
    <span className={`inline-flex items-center gap-1 group ${className}`}>
      {children || <span>{text}</span>}
      <button
        type="button"
        className="w-4 h-4 inline-flex items-center justify-center text-gray-300 hover:text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer border-0 bg-transparent p-0"
        onClick={handleCopy}
        title="复制"
      >
        {copied ? <Check size={12} className="text-emerald-500" /> : <Copy size={12} />}
      </button>
    </span>
  );
}

export function CopyableEndpoint({ host, port, className = '' }) {
  const [copied, setCopied] = useState(false);
  const text = host ? `${host}:${port}` : String(port);

  async function handleCopy(event) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {
      // 剪贴板权限不可用时静默失败，不影响页面其他功能
    }
  }

  return (
    <span className={`inline-flex items-center gap-1 group ${className}`}>
      <span className="font-mono tabular-nums">{text}</span>
      <button
        type="button"
        className="w-4 h-4 inline-flex items-center justify-center text-gray-300 hover:text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer border-0 bg-transparent p-0"
        onClick={handleCopy}
        title="复制连接地址"
      >
        {copied ? <Check size={12} className="text-emerald-500" /> : <Copy size={12} />}
      </button>
    </span>
  );
}

export function ResourceValue({ memory, memoryUsed }) {
  const totalBytes = memory * 1024 * 1024 * 1024;
  const pct = totalBytes > 0 && memoryUsed > 0 ? (memoryUsed / totalBytes) * 100 : 0;
  const usedLabel = memoryUsed > 0 ? formatBytes(memoryUsed) : '0MB';
  const totalLabel = formatBytes(totalBytes);
  const tone = pct > 80 ? 'bg-red-500' : pct > 60 ? 'bg-amber-500' : 'bg-emerald-500';
  return (
    <div className="flex flex-col gap-0.5 min-w-[100px]">
      <div className="h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-300 ${tone}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span className="font-mono tabular-nums text-[11px] text-gray-500">{usedLabel} / {totalLabel}</span>
    </div>
  );
}
