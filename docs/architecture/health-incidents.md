# 异常记录功能设计

> 版本：v0.2（最小版）
> 日期：2026-08-31
> 状态：待评审
> 范围：Redis Pilot Server 与 Dashboard

## 1. 目标

把平台已经能发现的异常**记录下来、能查**：什么时候开始、持续多久、什么时候恢复。

当前所有异常信号(reconcile 告警、备份失败、Master 不一致)要么只在内存里去重打印日志
(`alertInstances`)、要么散落在 tasks/日志中，恢复后没有可查询的历史。本功能补上这段。

**非目标(首期不做)**：通知(邮件/飞书/短信)、告警确认/认领/静默、Prometheus 指标类异常、
规则在线编辑、根因分析、把每轮正常健康检查写库。

## 2. 数据模型

只加一张表：

```sql
CREATE TABLE health_incidents (
    id                 TEXT PRIMARY KEY,
    fingerprint        TEXT NOT NULL,           -- type:object_type:object_id，同一活动异常唯一
    incident_type      TEXT NOT NULL,           -- 见 §3
    status             TEXT NOT NULL,           -- open|resolved
    severity           TEXT NOT NULL,           -- warning|critical
    source             TEXT NOT NULL,           -- reconcile|task|topology
    object_type        TEXT NOT NULL,           -- instance|group|node
    object_id          TEXT NOT NULL,
    group_name         TEXT NOT NULL DEFAULT '',
    instance_name      TEXT NOT NULL DEFAULT '',
    node_name          TEXT NOT NULL DEFAULT '',
    summary            TEXT NOT NULL,           -- 一句话描述
    reason             TEXT NOT NULL DEFAULT '',-- 检测依据(已确认的事实，不是猜测)
    evidence           TEXT NOT NULL DEFAULT '{}', -- JSON：期望值/实际值等结构化证据
    first_observed_at  TEXT NOT NULL,           -- 第一次观测到异常
    resolved_at        TEXT NOT NULL DEFAULT '',-- 恢复时间(空=仍未恢复)
    last_seen_at       TEXT NOT NULL,           -- 最近一次仍观测到异常
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE INDEX idx_incidents_status ON health_incidents(status, first_observed_at DESC);
CREATE INDEX idx_incidents_instance ON health_incidents(instance_name, first_observed_at DESC);
CREATE INDEX idx_incidents_group ON health_incidents(group_name, first_observed_at DESC);

-- 同一对象同一类异常同一时刻只允许一条未恢复记录，靠它兜底幂等
CREATE UNIQUE INDEX idx_incidents_active_fingerprint
    ON health_incidents(fingerprint) WHERE status = 'open';
```

说明：

- 只有 `open` / `resolved` 两个状态。没有单独的“观测中/候选”持久化状态——去抖计数放内存。
- 持续时间不入库，查询时算：`open` 用 `now - first_observed_at`，`resolved` 用
  `resolved_at - first_observed_at`。
- 时间统一 RFC3339 UTC 存储，前端按本地时区展示。
- `evidence` 不存密码、Token、Redis Key、完整命令；错误文本截断脱敏。

## 3. 异常目录(第一阶段)

只覆盖现有代码已经能观测到的信号：

| incident_type | 对象 | 来源 | 触发 | 恢复 | 级别 |
|---------------|------|------|------|------|------|
| `instance_unexpected_down` | instance | reconcile | 期望 running，实际 stopped/missing，连续 3 轮 | 连续 2 轮 running | critical |
| `node_agent_unreachable` | node | reconcile | Agent 连接失败/超时/响应不可解析，连续 3 轮 | 连续 2 轮成功 | critical |
| `no_running_replica` | group | reconcile | replication 组内"应运行"的从节点全部 down，连续 3 轮 | 连续 2 轮至少 1 个 running | critical |
| `master_mismatch` | group | topology | 平台/Sentinel/运行时已知视角与 current_master 不一致，连续 3 次评估 | 连续 2 次评估一致 | critical |
| `failover_conflict` | group | topology | `failover_conflict` 标记为 true | 标记清除且连续 2 次评估一致 | critical |
| `backup_task_failed` | instance | task | `backup.create` 进入 failed/interrupted | 下次同类备份成功 | warning |
| `restore_task_failed` | instance | task | `backup.restore` 进入 failed/interrupted | 下次同类恢复成功 | critical |

> `replica_wrong_target`(运行时复制地址指向错误 Master)暂缓：reconcile 的
> replica-drift 逻辑会主动纠正它，恢复信号与自愈相互干扰，信号不够干净，留待后续。

规则说明：

- 用户主动停止实例(期望 stopped)不算异常；`no_running_replica` 也只统计"应运行"的从(排除主动 stop 的从)。
- `no_running_replica` 判为 critical：组内一个可用从都没有时，读流量无处分担，且 master 再挂将无从可提升(故障转移无目标)。级别不低于单个从 down 的 `instance_unexpected_down`。
- `no_running_replica` 观测不完整时(有从节点所在 server 不可达/未观测)本轮跳过，不判定也不恢复——该情形由 `node_agent_unreachable` 覆盖，避免重复与误判。
- Agent 不可达只记一条节点级异常，evidence 里列受影响实例，不给每个实例各记一条。
- Sentinel 未启用或某视角尚未观测时该视角为“未知”，不单独判定 master_mismatch。
- 备份/恢复失败是一次性事件，立即记 open，不走 3 轮去抖；`canceled`(用户主动取消)不记。
- 复制链路健康(`master_link_status`)、内存/磁盘水位等需要指标数据，第一阶段不做，留到后续。

## 4. 记录逻辑

一个服务，两个动作：

```go
// 周期来源(reconcile/topology)每轮调用。带去抖。
// active=true 表示这一轮观测到异常；active=false 表示这一轮正常。
func (s *IncidentService) Observe(obs Observation)

// 任务来源直接调用，不去抖。
func (s *IncidentService) Report(obs Observation)   // 记一条 open
func (s *IncidentService) Resolve(fingerprint string) // 标记 resolved
```

`Observation` 携带 type/object/severity/summary/reason/evidence 等字段。

去抖(仅周期来源)：

- 内存维护每个 fingerprint 的连续异常次数、连续正常次数;
- 连续 3 次异常 → 写一条 `open`(以 fingerprint 唯一索引兜底，已存在就更新 `last_seen_at`);
- `open` 期间连续 2 次正常 → 写 `resolved_at`;
- 阈值未到就恢复的，内存计数清零，不写库(不进历史);
- “这一轮没数据”(Agent 不可达导致拿不到某实例观测)不算正常，不推动恢复。

Server 重启：去抖计数在内存，重启后从 0 重新计；已经是 `open` 的记录留在库里不受影响
(靠唯一索引，不会重复建)。3 轮的重数窗口只有几十秒，可接受。

写库失败：记 error 日志，不阻断控制面操作，不谎报已记录。

## 5. 与现有模块集成

- **reconcile**(`runReconcile`)：现有逻辑不动，每轮把结果转成 `Observe` 调用；
  `alertInstances` 的日志去重可以保留或交给本服务。Agent 一轮失败只提交节点级 Observation。
- **topology/sentinel**：每轮拿到三视角 Master 后提交 group 级 Observe;failover_conflict 单独一类。
- **task tracker**：备份/恢复进入终态时，失败调 `Report`，后续同类成功调 `Resolve`。
- **audit_log**：本服务只记异常事实，不写审计;自动修复、故障转移等动作仍各自写审计。

## 6. API

只读，沿用现有鉴权。

```http
GET /incidents?status=open&severity=critical&instance=order-1&group=order&page=1&page_size=30
GET /incidents/:id
```

列表：默认 open 优先、再按 `first_observed_at DESC`;支持 status/severity/type/object/group/
instance/node/时间范围/分页筛选。`duration_ms` 查询时计算返回，不写库。

## 7. Dashboard

1. **总览页**：右侧加“当前异常”卡片，只列 open，critical 优先，最多 5 条，点进对应实例/节点详情;
   无异常时紧凑显示“当前无异常”。
2. **异常记录页**(由“查看全部”进入，首期不加左侧固定导航)：列表 + 筛选，列为
   `状态 | 级别 | 异常 | 对象 | 检测原因 | 发生时间 | 持续时间 | 恢复时间`;点击看详情
   (含 evidence 可读展示)。
3. **实例详情**：加一个“异常”查看(不强制合并现有 tab);按实例与所属 group 共同过滤。

## 8. 保留与安全

- `resolved` 记录默认保留 180 天，每日分批清理，避免长事务;
- evidence 单条 <= 16 KiB，超出截断;
- 敏感字段(密码/Token/Key/完整命令)不入 evidence、日志、页面;
- 首期无手工新建/编辑/删除异常的接口。

## 9. 验收标准

- 实例期望 running、连续 3 轮 stopped/missing → 产生一条 open；单次抖动恢复不进历史;
- 同一异常持续期间不重复建记录;连续 2 轮恢复后自动 resolved 且持续时间正确;
- resolved 后再次发生 → 新记录;Server 重启后 open 不丢、不重复;
- 用户主动停止实例不产生异常;Agent 不可达只产生一条节点级异常;
- 备份/恢复失败产生异常并可被后续成功结束;
- Dashboard 能看当前异常和历史(发生/恢复/持续时间);viewer 可查询。

## 10. 工作量估算

| 工作项 | 估算 |
|--------|------|
| 表、Store、迁移 | 0.5 人日 |
| IncidentService(记录/去抖/恢复) | 1 人日 |
| 接入 reconcile / topology / task tracker | 1.5 人日 |
| 查询 API | 0.5 人日 |
| Dashboard(当前异常卡片 + 异常记录页 + 实例详情) | 1.5 人日 |
| 测试与边界 | 1 人日 |

合计约 5～6 人日。

## 11. 后续可选增强(想清楚再做)

- 关键事件时间线表(记录异常期间的证据变化、关联任务/审计);
- incident ↔ task/audit 多对多显式关联表;
- Prometheus 指标类异常(内存/磁盘水位、复制延迟、复制链路 down);
- Proxy 无可用后端;
- 通知、确认、静默、规则在线编辑。
