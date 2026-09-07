# G5c：Gateway 登记与状态观测

本阶段仅实现 Gateway → Server 的观测闭环，不经过 Agent，不更改 Proxy 快照、实例入口或拓扑 generation。
正式产品目标是 **Cluster 的 Redis 热层 + Kvrocks 冷层**；现有单后端 passthrough 仅是开发验证基础，不扩展成单点/主从冷热产品。

## 身份与边界

Gateway 在第一次成功心跳时登记，每次启动生成随机 `boot_id`，每次发送递增 `sequence`。
报告带有 `gateway_id`、`node_id`、`cluster_id` 和 `shard_ids`（1～64 项、不可重复），允许一个 Gateway 声明多个逻辑分片。
这些 ID 是进程声明的规划标签，目前不校验实际集群/分片存在，也不建立冷热节点一对一关系。
**声明不是归属授权、租约或 fencing；尚不能用于选择哪个 Gateway 执行写请求。** 不同 Gateway ID 声明同一分片的冲突检测尚未实现。

报告还包含：

- 二进制构建版本 `build_version`、本地配置指纹 `local_config_digest`。
- `mode`、`serving`、`backend_ready`、`draining`。后端就绪来自当前探测结果，不是 Server 主动检查，也不是数据一致性证明。
- Server 接收时间 `received_at`，不接受客户端提供时钟。

本地配置指纹由解析后的配置字段（包含配置路径但不包含凭据文件内容）计算 SHA256，可能随版本/路径变化。不能用它证明密码相同，也不是 Server 草稿的版本确认。
查询中的 `config_source` 固定为 `local_file`，`applied_revision` 固定为空。
**配置发布、拉取、应用确认尚未实现，不能把 G5a 保存草稿成功冒充已生效。** 同一启动 ID 的集群/分片声明、版本、模式和配置指纹不可变；变更配置需要重启进程。

## API 与身份认证

| 接口 | 认证 | 作用 |
| --- | --- | --- |
| POST `/api/v1/gateway/heartbeat/:name` | 对应 Gateway 的独立 Bearer Token | 首次登记及后续心跳 |
| GET `/api/v1/gateway/runtime` | 现有 JWT / API Token，viewer 可读 | 查询进程观测列表 |

Server 新配置：

```yaml
# server.yaml 示例占位符；必须替换成独立随机凭据，保护配置文件权限。
gateway_tokens:
  gateway-redis01-dev: REPLACE_WITH_A_RANDOM_GATEWAY_TOKEN
```

Gateway 在现有配置中添加：

```yaml
control_plane:
  server_endpoint: http://127.0.0.1:8080
  token_file: /etc/redis-gateway/control.secret
  node_id: redis01
  cluster_id: hybrid-dev
  shard_ids: [shard-0]
  heartbeat_interval: 10s
  request_timeout: 3s
```

Server 配置键必须与 Gateway 顶层 `gateway_id` 相同。缺少配置、空 Token 或长度小于 16 字节均拒绝；不沿用 Proxy 的“空 internal_token 则跳过认证”。用户 JWT 不是机器凭据，各 Gateway 应配置不同的随机 Token。
Gateway Token 文件必须是绝对路径、普通文件、仅文件属主可访问，单行可打印 ASCII、至少 16 字节，读取上限 4KiB；不要与客户端或后端 Redis 密码复用。
当前启动时读取凭据，轮换需分别更新配置并重启，暂不提供无缝轮换。

控制面 URL 必须为 HTTPS；仅字面回环 IP 允许 HTTP，不接受 userinfo、路径前缀、query 或 fragment。不跟随 HTTP 重定向、不使用环境 HTTP 代理，不把 Token、原始 HTTP 错误或响应体写日志。
数据监听仍仅回环；支持 HTTPS 控制面上报不代表已放开跨机数据入口。
未设置 `control_plane.server_endpoint` 时完全关闭上报，保持现有行为；半配置的控制面字段会导致启动校验失败。

心跳体最多 16KiB，严格拒绝未知字段、多个 JSON 文档、路径/正文 Gateway 身份不一致。成功回复沿用 `APIResponse`，包含 `data.accepted_sequence`；400 参数错误、401 身份错误、409 序号/进程身份冲突或活跃启动 ID 超限、500 存储异常。
Server 不记录逐次心跳审计，避免高频膨胀；配置凭据仍通过运维管理，不提供注册管理 UI 或 CLI。

## 失联、乱序与冲突

SQLite 新增 `gateway_observations`，按 `(gateway_id, boot_id)` 保存最近一份报告，序号和状态同事务更新。
同一序号同内容重试可成功，但**不刷新接收时间**；更小序号或同序号不同内容拒绝；已上报 draining 的启动 ID 不能恢复为非 draining。
没有在线状态后台写入循环，查询时按 Server 时间推导：

| 状态 | 条件 |
| --- | --- |
| stale | 45 秒没有更新，或记录接收时间落在 Server 当前时间之后 |
| conflict | 同 Gateway ID 有两个及以上未过期、非 draining 的启动 ID |
| draining | 未过期，进程声明正在退出 |
| ready | 未过期、无上述冲突，passthrough 正在服务且后端探测就绪 |
| unready | 已登记且未过期，但未达到 ready 条件，包括 skeleton 模式 |

Server 重启会保留观测和接收时间，但不会重新“续在线”。判断依赖 Server 壁钟，需保持时钟同步；没有将它实现为严格分布式租约。
每个 Gateway 最多保留 8 个启动 ID；第 9 个登记仅在可删除一个过期历史记录时接受，否则返回 409，等待旧记录过期。旧历史清理后不再保留其序号防重放记录，所以这些状态不能作为写入 fencing。
停止前尽力发送 draining；异常退出、网络故障或最终上报失败由失联超时兜底。同名替换进程在旧记录过期前可能短暂显示 conflict，这是保守观测，不触发切流。

## 上报与数据面的隔离

单独一个上报协程，每次最多一个 HTTP 请求；默认每 10 秒发送，加 0～20% 抖动，配置范围 1～15 秒；请求超时至多 3 秒。失败后到下一周期发送更新的观测，没有无限队列或业务命令重试。
Server 不可用、鉴权失败、返回异常都不会关闭网关数据入口，也不改变 `/readyz` 的后端判断。
此行为只适用于当前透传验证；未来真实冷热写入权失效时如何阻断请求需要独立协议，不能照搬“控制面失联仍继续写”。

可通过 `/metrics` 检查：

- `redis_pilot_gateway_control_heartbeats_total{result="acknowledged|error"}`：固定维度计数。
- `redis_pilot_gateway_control_last_ack_timestamp_seconds`：最近一次有效确认时间；未确认时为 0。

## 验证与交付边界

测试覆盖 SQLite 重开、乱序、重复不续期、并发序号、失联、同名进程冲突、历史数量上限、凭据隔离、请求边界、权限和 Proxy 快照不变。
使用本地 HTTP Server + Gateway 实际监听完成登记/退出上报闭环；模拟控制面超时验证仍可转发 PING，模拟 503 后恢复验证周期重试，验证拒绝重定向和错误 ACK。

```sh
go test -race ./internal/gateway ./internal/store ./internal/server -run 'TestControlPlane|TestGatewayHeartbeat' -count=5 -timeout 90s
go test ./internal/gateway ./internal/store ./internal/server ./internal/proxy/... ./internal/agent ./pkg/apitypes ./cmd/gateway
go vet ./internal/gateway ./internal/store ./internal/server ./pkg/apitypes ./cmd/gateway
```

本阶段尚未部署：未更新 redis01 的二进制或配置，未迁移开发机 pilot.db，线上开发服务仍使用前一部署版本。
下一步优先单个逻辑分片的 Redis + Kvrocks 核心正确性验证；不继续铺设页面、CLI 或自动切流来替代冷热同步、回温、淘汰的验证。
