# 网关控制面接入：G5

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

> G5a 已实现但未发布到开发机 Server；G5b 已在 redis01 以独立二进制 + systemd 部署并通过基础验证。没有发布路由、接入冷热同步或改变现有实例入口。性能专项暂缓，不视为已通过生产验收。

## 分批范围

| 批次 | 内容 | 状态 |
| --- | --- | --- |
| G5a | 草稿模型、SQLite 持久化、管理 API、并发修改保护、审计 | 已实现 |
| G5b | 独立二进制 + systemd，运维配置及凭据安装、资源与端口检查 | redis01 隔离部署及基础验证已完成，非生产验收 |
| G5c | Gateway 直接登记、心跳、状态观测；区分本地配置与已应用版本 | 最小观测闭环已实现，未部署；配置发布/应用确认待做 |
| G5d | 测试实例组显式切流、Proxy 快照接入、撤回和旧连接排空 | 待实现，不能只修改一个地址 |

这套接入针对当前 **passthrough 单后端原型**，不代表热 Redis + 冷 Kvrocks 已可用。
先把规划信息与运行状态分开，避免“保存配置”被误认为“已经部署/生效”。

部署职责见 [独立二进制部署](../../gateway-deployment.md)。Agent 不参与 Gateway 生命周期；Server 草稿也不下发启停命令。

G5c 细节见 [登记与心跳](gateway-heartbeat.md)。正式冷热产品范围为 Cluster；G5a 的单后端草稿仍保留为开发验证数据，不作为单点/主从冷热产品交付，也不复用为正式集群归属授权。

## G5a 数据模型

`pilot.db` 新增独立表 `gateway_drafts`，保留既有表和数据；重复启动幂等建表。
模型见 [gateway.go](../../../../pkg/apitypes/gateway.go)，实现见 [gateway_store.go](../../../../internal/store/gateway_store.go) 和 [gateway_handler.go](../../../../internal/server/gateway_handler.go)。

返回字段：

- `name`：1～64 个 ASCII 字母/数字/下划线/连字符，首位为字母或数字；创建后不可改名。
- `spec.node`：计划部署节点名；`spec.group`：计划绑定的后端实例组名。二者必须已登记，最长各 128 字节。
- `spec.mode`：只支持 `passthrough`，省略时补此值；拒绝 `hybrid`。
- `spec.data_port` / `spec.admin_port`：两个不同的 1024～65535 端口，**仅为规划值，不预占也不保证可用**。
- `revision`：每次成功保存生成的新随机版本标识，不是递增整数；用于比较并交换（CAS）。删除再创建同名草稿也生成全新标识。
- `status`：固定 `draft`，不表示 online、ready 或已部署。
- `created_at` / `updated_at`：Server 生成的 UTC 时间。

保存时仅校验节点存在、后端为 Redis 的 standalone/replication 组，不需要先有健康主库，也不做网络请求。
草稿没有强运行时引用：节点/实例组后续修改或删除不会被草稿阻止，草稿也不会自动删除；下一次保存会重新校验，未来部署前必须重新校验完整拓扑。
没有给这些引用添加外键，是因为它们是未应用的规划，且现有状态更新会全量重写节点/实例组表。
草稿修改不增加现有拓扑 generation，不进入 Proxy 快照，不触发 Agent 调度或后台重试。

## 管理 API

统一使用现有 JWT / API Token 鉴权。viewer 可读，admin 可写；不是 Proxy 的 `internal_token` 接口。
沿用现有 `APIResponse` 包装，成功返回 HTTP 200；列表的 `data.drafts` 为空时返回 `[]`。

| 方法 | 路径 | 请求体 |
| --- | --- | --- |
| GET | `/api/v1/gateway/drafts` | 无 |
| GET | `/api/v1/gateway/drafts/:name` | 无 |
| POST | `/api/v1/gateway/drafts` | `{ "name": "...", "spec": {...} }` |
| PUT | `/api/v1/gateway/drafts/:name` | `{ "revision": "当前标识", "spec": {...} }`，完整替换 spec |
| DELETE | `/api/v1/gateway/drafts/:name` | `{ "revision": "当前标识" }` |

创建示例（节点与组须先存在）：

```json
{
  "name": "gateway-test",
  "spec": {
    "node": "redis01",
    "group": "dedicated-test-group",
    "mode": "passthrough",
    "data_port": 17379,
    "admin_port": 19200
  }
}
```

写请求最多 8KiB，拒绝未知字段和多个 JSON 文档。没有密码、Token、任意后端 URL 或文件路径字段；不会从后端实例组复制密码到查询结果或审计。
重复创建、版本过期返回 409；缺少版本/参数错误返回 400；不存在的草稿返回 404；存储异常返回通用 500，不向客户端暴露数据库错误。
修改/删除遇到 409 时，客户端必须先重新读取并让操作者确认，不得自动携带新版本重试旧内容。

到达存储操作的创建/修改/删除记录 `gateway.draft.create/update/delete` 审计及成功/失败结果；不记录完整请求体或凭据。
审计沿用现有 Logger，与状态写入不是同一事务；本阶段也没有配置历史版本恢复功能。**删除草稿不是停止网关，更不是撤回已发布的业务入口。**

## 后续必须解决的边界

1. 当前网关只允许回环监听。跨机 Proxy 接入需明确可信内网绑定、入口认证和防火墙策略；不得直接放开到公网。
2. 运维部署前必须检查节点状态、端口冲突和真实资源余量；草稿中的端口不算资源预留。端口与资源如何纳入现有统一分配器待定。
3. 后端绑定保存组名，不保存某一主库地址；部署时解析哪个主库、切主时如何更新和排空旧连接待定。现有网关只探测并拒绝非主节点，不自动完成切主。
4. 凭据来源、内部身份、文件权限、轮换与审计脱敏待定，不能把前端口令与 Redis 后端口令强行绑定。
5. G5c 已实现启动 ID、序号、Server 接收时间及状态过期判断；实际应用 revision 的发布/确认协议仍待定，当前只上报本地配置指纹。
6. Proxy→网关切流应只对明确选中的隔离测试组生效，普通组保持原状；旧连接、回滚及部分失败协议待定。有限命令白名单与现有客户端握手不完全兼容，不能直接替换普通实例入口。
7. 不提供 Gateway 容器镜像或 Agent 部署接口；CLI、前端界面未实现；冷热同步、回温、淘汰、一致性和 Cluster 迁移仍在后续阶段。

## 验证

新增测试覆盖持久化重开、幂等迁移、同名冲突、并发修改只有一个成功、旧版本删除拒绝、删除重建防误改、无运行时引用副作用、鉴权/RBAC、敏感字段拒绝、请求上限、审计及 Proxy 快照不变。

```sh
go test -race ./internal/store ./internal/server -run '^TestGatewayDraft' -count=1
go test ./internal/store ./internal/server ./internal/gateway ./internal/proxy/... ./pkg/apitypes
go vet ./internal/store ./internal/server ./pkg/apitypes
```

测试使用临时 SQLite 和内存 HTTP 服务，没有调用开发机 Agent、部署新服务或迁移开发机的 pilot.db。
