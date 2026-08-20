# Redis Pilot

## 项目概述
基于 GAL 的 Redis 多实例管理平台，支持单点/主从实例的创建、配置、备份、迁移、故障转移。

## 架构
```
GAL -> CLI -> Server -> Agent（各服务器）
```
- **Server**：状态管理层，状态和审计日志统一存储在 `pilot.db`（SQLite），暴露 HTTP API
- **Agent**：部署在每台服务器，管理本机 Redis/Kvrocks 容器（Podman），定时健康检查/指标采集
- **CLI**：原子操作命令行工具，调用 Server HTTP API
- **Proxy**：自研 Go 代理（`bin/redis-proxy`），通过 HTTP 长轮询 `/api/v1/proxy/snapshot` 从 Server 拉取快照，提供统一入口与读写分离

## 技术栈
- 语言：Go
- HTTP 框架：Gin
- 容器：Podman
- Redis 引擎：Redis 5 / 6.2 / 7 / Apache Kvrocks 2.15.0

## 目录结构
```
redis-pilot/
  ├── cmd/
  │   ├── server/   # Server 入口
  │   ├── agent/    # Agent 入口
  │   ├── proxy/    # Proxy 入口（自研 Go 读写分离代理）
  │   └── cli/      # CLI 入口
  ├── internal/
  │   ├── store/    # pilot.db（SQLite）读写：servers/instance_groups/instances/audit_log 等表
  │   ├── state/    # 状态辅助纯函数（锁、拓扑计算），状态读写直接使用 internal/store
  │   ├── server/   # Server HTTP handler、调度、代理层状态快照
  │   ├── proxy/    # Proxy：HTTP 长轮询拉取快照、Redis 读写分离路由
  │   ├── agent/    # Agent HTTP handler + 定时任务
  │   ├── audit/    # 审计日志（Logger 负责ID/时间戳生成，底层委托给 internal/store）
  │   ├── s3/       # S3 兼容对象存储客户端（备份上传/下载/列表/清理）
  │   └── podman/   # Podman API 封装
  └── pkg/
      ├── apitypes/ # Server ↔ Agent 共享请求/响应结构体
      └── redisconf/ # 配置覆盖项校验规则（Server 与 Agent 共用同一份保留字清单）
```

## 配置文件
- `server.yaml`：Server 监听端口、CLI 鉴权 Token（空则不鉴权）、Redis/Kvrocks 版本白名单、`data_dir`（`pilot.db` 存放目录）
- `agent.yaml`：Agent 监听端口、鉴权 Token（空则不鉴权）、S3 备份存储配置（`backup.s3`：endpoint/bucket/access_key/secret_key）
- `proxy.yaml`：Proxy 的 Server 连接信息（`server_endpoint`/`server_token`）、长轮询参数（`poll_interval`/`poll_timeout`/`poll_wait`）、`metrics_listen`
- `~/.redis-pilot-cli/config.yaml`：CLI 配置，Server 地址默认 127.0.0.1:8080

> 服务器节点和实例的完整状态不再使用独立 YAML 文件，统一存储在 `data_dir/pilot.db`（SQLite）。

## 认证
- CLI -> Server：Bearer Token，Token 为空则跳过鉴权
- Server -> Agent：Bearer Token，Token 为空则跳过鉴权
- Proxy -> Server：Bearer Token（`server_token`），Proxy 拉取快照时携带；Token 为空则跳过鉴权

## 开发顺序
1. Server（状态存储读写 + HTTP API）
2. Agent（Podman 管理 + 定时任务）
3. CLI（子命令封装）
4. Proxy 部署
5. GAL Skills

## 参考文档
- 详细设计：docs/architecture/overview.md（另见同目录 components.md / flows.md / deployment-ops.md）

## 说明
如果有些文档描述不完整，或者不合理，修改以后需要更新文档 `docs/architecture/` 下对应的文档。
