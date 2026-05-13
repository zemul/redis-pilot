# Redis Pilot

基于 GAL 的 Redis/Kvrocks 多实例管理平台，支持单点/主从实例的全生命周期管理。

## 特性

- 单点 & 主从实例创建、配置、删除
- 服务器资源池化与智能调度（跨可用区分布）
- Envoy Redis Proxy 读写分离
- 自动备份与恢复
- 故障转移与状态自愈（定期 reconcile）
- 完整审计日志
- 支持 Redis 7 和 Apache Kvrocks 2.9

## 架构

```
用户 → GAL Skills → CLI (redis-tool) → Server → Agent → Podman
                                                          ↓
                                                     Envoy Proxy
```

| 组件 | 端口 | 职责 |
|------|------|------|
| Server | 8080 | 全局状态管理、API 网关、资源调度 |
| Agent | 8400 | 容器管理、健康检查、备份执行（每台服务器一个） |
| CLI | - | 命令行工具，调用 Server API |

## 快速开始

### 编译

```bash
make all
# 产出: bin/redis-server  bin/redis-agent  bin/redis-cli
```

### 启动 Server

```bash
mkdir -p /opt/redis-server/{state,audit,logs}
cp configs/server.yaml /opt/redis-server/
bin/redis-server --config /opt/redis-server/server.yaml
```

### 启动 Agent（每台数据节点）

```bash
mkdir -p /opt/redis-agent/logs /data/redis
cp configs/agent.yaml /opt/redis-agent/
bin/redis-agent --config /opt/redis-agent/agent.yaml
```

### 注册服务器 & 创建实例

```bash
# 注册服务器到资源池
redis-tool pool-add server-a 10.0.1.10 8400

# 创建单点缓存实例
redis-tool instance-create my-cache --engine redis --memory 2Gi

# 创建主从持久化实例
redis-tool instance-create my-order --engine kvrocks --memory 4Gi --type replication

# 查看状态
redis-tool instance-list
redis-tool instance-status my-cache
```

## 项目结构

```
├── cmd/
│   ├── server/          # Server 入口
│   ├── agent/           # Agent 入口
│   └── cli/             # CLI 入口
├── internal/
│   ├── server/          # Server: API handler、调度、Envoy 配置生成
│   ├── agent/           # Agent: 容器管理、健康检查、配置模板
│   ├── state/           # YAML 状态文件读写 + 文件锁
│   ├── audit/           # 审计日志（JSONL）
│   ├── podman/          # Podman API 封装
│   └── logger/          # 日志
├── pkg/apitypes/        # Server ↔ Agent 共享类型
├── configs/             # 配置文件示例
└── docs/                # 文档
```

## 配置

### Server (`server.yaml`)

```yaml
port: 8080
token: ""                    # Bearer Token，空则不鉴权
data_dir: /opt/redis-server/state
envoy_dir: ""                # Envoy 配置输出目录
envoy_reload_cmd: ""         # 配置变更后的重载命令
ports:
  redis:          { start: 6379,  end: 6499  }
  envoy_readwrite: { start: 16379, end: 16499 }
  envoy_writeonly: { start: 16500, end: 16619 }
  envoy_mgmt:     { start: 26379, end: 26499 }
log:
  dir: /opt/redis-server/logs
  stdout: true
```

### Agent (`agent.yaml`)

```yaml
port: 8400
token: ""
data_dir: /data/redis
log:
  dir: /opt/redis-agent/logs
  stdout: true
```

### CLI (`~/.redis-pilot-cli/config.yaml`)

```yaml
server: 127.0.0.1:8080
token: ""
```

## CLI 命令参考

```bash
# 资源池
redis-tool pool-query
redis-tool pool-add <name> <endpoint> <port>
redis-tool pool-remove <name>
redis-tool pool-update <name> --labels zone=az-1

# 实例
redis-tool instance-list
redis-tool instance-create <name> [--engine redis|kvrocks] [--memory 2Gi] [--type standalone|replication]
redis-tool instance-delete <name>
redis-tool instance-start/stop <name>
redis-tool instance-config <name> --maxmemory-policy allkeys-lru
redis-tool instance-promote <name>
redis-tool instance-replicate <replica> <master>

# 备份
redis-tool backup-exec <instance>
redis-tool backup-list <instance>
redis-tool backup-restore <instance> <backup_id>
```

## 文档

- [部署文档](docs/DEPLOYMENT.md) — 完整的生产部署指南
- [架构设计](ARCHITECTURE.md) — 详细设计文档
- [快速参考](docs/QUICK_REFERENCE.md)
- [实施清单](docs/IMPLEMENTATION_CHECKLIST.md)

## 技术栈

- Go 1.25 / Gin
- Podman（容器运行时）
- Envoy Redis Proxy（读写分离）
- YAML（状态存储）

## License

Internal use only.
