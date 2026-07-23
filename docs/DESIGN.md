# Design

Redis Pilot 看板的视觉系统。定位见 [PRODUCT.md](PRODUCT.md):运维工具(product register),气质冷静可控、专业可信,亮底,参考 Vercel dashboard。

**色彩策略:Restrained(克制)** —— 真中性 + 语义状态色。交互层(导航/按钮/链接)只用黑白灰;绿/琥珀/红只表达健康状态。屏幕上出现颜色就一定承载信息。

---

## Color

全部用 OKLCH。真中性 = chroma 0。彻底去掉旧设计的偏蓝冷灰(`#f6f8fb`)和 SaaS 蓝(`#3157d5`)。

### 中性 / 表面 / 文字

| Token | OKLCH | 约等于 | 用途 |
|---|---|---|---|
| `--bg` | `oklch(1 0 0)` | `#ffffff` | 页面画布 |
| `--surface` | `oklch(0.985 0 0)` | `#fafafa` | 浅抬升:表头、inset 区、replica 行 |
| `--surface-2` | `oklch(0.968 0 0)` | `#f4f4f4` | 更深 inset:disabled、代码块底 |
| `--ink` | `oklch(0.205 0 0)` | `#1a1a1a` | 主文字 / 标题(近黑,非纯黑) |
| `--ink-secondary` | `oklch(0.44 0 0)` | `#5d5d5d` | 次级文字(≥6.5:1) |
| `--ink-muted` | `oklch(0.53 0 0)` | `#707070` | 标签 / 弱化文字(≥5:1,顶住对比) |
| `--border` | `oklch(0.922 0 0)` | `#e5e5e5` | 发丝级边框(主要靠它分隔,而非填色) |
| `--border-strong` | `oklch(0.87 0 0)` | `#d4d4d4` | hover / 强分隔 |

**禁止**:muted 文字降到 4.5:1 以下"求优雅"。这是旧设计可读性最大的隐患。

### 强调色(主操作)= 近黑,不是蓝

| Token | OKLCH | 约等于 | 用途 |
|---|---|---|---|
| `--accent` | `oklch(0.205 0 0)` | `#1a1a1a` | 主按钮背景、active 导航、focus ring 基色 |
| `--accent-hover` | `oklch(0.30 0 0)` | `#383838` | 主按钮 hover(提亮) |
| `--accent-ink` | `oklch(0.99 0 0)` | `#fcfcfc` | 黑底上的白字 |

### 语义状态色(唯一的彩色)

每个状态四件套:`solid`(圆点/进度条/图标)、`fg`(白底文字,AA)、`bg`(badge 软底)、`border`(badge 描边)。色相收敛、略去饱和,不要 bootstrap 那种生硬亮色。

**Healthy / Success(绿)**
- solid `oklch(0.62 0.15 150)` `#2f9e5f`
- fg `oklch(0.50 0.13 150)` `#2f7d4f`(≈4.8:1 ✓)
- bg `oklch(0.965 0.022 150)` · border `oklch(0.88 0.06 150)`

**Warn / Creating / Drain(琥珀)**
- solid `oklch(0.74 0.15 78)` `#d98a2b`
- fg `oklch(0.52 0.11 65)` `#8a5a25`(棕琥珀,白底可读 ≈5:1 ✓)
- bg `oklch(0.967 0.03 80)` · border `oklch(0.86 0.08 80)`

**Failed / Error / Critical(红)**
- solid `oklch(0.585 0.20 27)` `#d23f30`
- fg `oklch(0.52 0.20 27)` `#c0362a`(≈4.6:1 ✓)
- bg `oklch(0.965 0.02 27)` · border `oklch(0.87 0.07 27)`

**Neutral / Unknown**:圆点用 `--ink-muted`,badge 用 `--surface-2` + `--border`。

### 进度条多色(服务器资源)

旧设计用蓝/靛/天蓝区分 CPU/内存/磁盘——属于"无意义的彩色"。改为:三条统一用 `--ink`(中性),仅当某项 **超过阈值**(如 ≥85%)才转 `--warn-solid` / `--red-solid`。颜色只在"需要关注"时出现。

---

## Typography

字体搭配走 Vercel 同源的 **Geist**:Geist Sans(UI/正文)+ Geist Mono(技术数据)。在对比轴上配对(几何 grotesque + 等宽),不是两个相似 sans。

```
--font-sans: "Geist", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
--font-mono: "Geist Mono", ui-monospace, "SF Mono", "JetBrains Mono", Menlo, monospace;
```

**Mono 用在哪**(原则 2:数据如实呈现):IP / endpoint、端口、实例名、内存/磁盘数值、CPU 核数、INFO 原文、备份文件名、时间戳。这是 Geist Mono 的主场,工具的专业感来源。

### 字号 / 字重(收敛,告别旧设计满屏 700–850)

| 角色 | size / line-height | weight | letter-spacing |
|---|---|---|---|
| 页面标题(topbar h2) | 20px / 1.2 | 600 | -0.02em |
| 面板标题(h3) | 15px / 1.3 | 600 | -0.01em |
| 正文 | 14px / 1.5 | 400 | 0 |
| 表格单元 | 13px / 1.4 | 400 | 0 |
| 标签 / 次级 | 13px / 1.4 | 500 | 0 |
| 微标签 / caption | 12px / 1.4 | 500 | 0 |
| Mono 数据 | 13px | 450–500 | 0 |
| 大数字(指标) | 28px / 1 | 600 | -0.02em |

权重只用 **400 / 500 / 600**。表头不再 `text-transform: uppercase` + 800(旧设计的厚重感来源),改为 12px / 500 / `--ink-muted`,需要时加极轻 tracking(+0.02em),不全大写。

---

## Spacing

4px 基准。`4 · 8 · 12 · 16 · 20 · 24 · 32 · 40 · 48`。

- 内容区 padding:28px(桌面)/ 16px(移动)
- 面板内 padding:16–18px
- 栅格 gap:16px
- 用间距韵律和对比建立层级,**不靠到处套边框卡片**(原则 4)。同类信息留白收紧,不同区块留白拉开。

---

## Radius

收敛的小尺度,不再全局 8px 一刀切:

```
--r-control: 6px;   /* 按钮、输入框、badge、chip */
--r-panel:   8px;   /* 面板、卡片 */
--r-modal:   12px;  /* 模态 */
--r-full:    999px; /* 圆点、pill */
```

---

## Elevation / Shadow

去掉旧设计的 `0 10px 30px`(让所有面板悬浮的 AI 感)。**主要靠发丝边框定义表面**,阴影只给真正浮起的层:

```
--shadow-sm: 0 1px 2px rgba(0,0,0,0.04), 0 1px 3px rgba(0,0,0,0.06);  /* dropdown */
--shadow-md: 0 8px 24px rgba(0,0,0,0.10), 0 2px 6px rgba(0,0,0,0.06); /* modal */
```

面板:`border: 1px solid --border`,**无阴影**。

---

## Focus

可见、统一、不刺眼。黑色 ring,不是旧设计的蓝色 glow:

```
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
```

输入框聚焦:`border-color: --ink` + `box-shadow: 0 0 0 3px rgba(0,0,0,0.08)`(中性,非蓝)。

---

## Z-index(语义化,不用 999)

```
--z-dropdown: 10;
--z-sticky:   20;
--z-backdrop: 30;
--z-modal:    40;
--z-toast:    50;
--z-tooltip:  60;
```

---

## Motion

- 时长 150–200ms,缓动用 ease-out 指数曲线(`cubic-bezier(0.16, 1, 0.3, 1)`),无 bounce / elastic。
- 默认 hover/active 的颜色与背景过渡;模态淡入 + 轻微上移(8px)。
- **reduced-motion 必做**:`@media (prefers-reduced-motion: reduce)` 下全部降级为淡入或瞬切。

---

## Components(关键约定)

- **统计概览**:推翻旧的"四个一样的 hero-metric 卡"。改为更克制的形态(如一行内联的 label + mono 数字 + 状态点,或带语义色的紧凑摘要条),避免图标方块 + 大数字模板。
- **Badge / 状态**:色 + 图标 + 文字三者齐全(色盲友好,原则见 PRODUCT 无障碍)。
- **主按钮**:近黑底白字(`--accent`),hover 提亮。次按钮:白底 + `--border` 描边。危险动作(停止生产实例):红底,且保留前置确认模态。
- **表格**:发丝行分隔,无重描边;表头 surface 底 + muted 文字;mono 列(端口/IP/资源)用 `--font-mono`。
- **面板**:`--border` 发丝边 + `--r-panel`,无阴影,不嵌套卡片。

---

## 反 AI 味检查清单(动手时逐条过)

- [ ] 没有蓝色品牌色;交互层纯黑白灰
- [ ] 彩色只出现在状态(绿/琥珀/红),且必有含义
- [ ] 没有四连 hero-metric 统计卡
- [ ] 没有全局悬浮阴影;表面靠发丝边框
- [ ] 技术数据用 Geist Mono
- [ ] 字重只用 400/500/600,表头不全大写加粗
- [ ] muted 文字对比度 ≥4.5:1
- [ ] 圆角分级,非全局 8px
