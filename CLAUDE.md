# Redis 多实例管理系统

## 项目概述
基于 GAL 的 Redis 多实例管理平台，支持单点/主从实例的创建、配置、备份、迁移、故障转移。

## 架构
```
GAL → CLI → Server → Agent（各服务器）
```
- **Server**：状态管理层，持有 pool-state.yaml + instances-state.yaml，暴露 HTTP API
- **Agent**：部署在每台服务器，管理本机 Redis/Kvrocks 容器（Podman），定时健康检查/指标采集
- **CLI**：原子操作命令行工具，调用 Server HTTP API
- **Envoy**：统一代理层，读写分离

## 技术栈
- 语言：Go
- HTTP 框架：Gin
- 容器：Podman
- Redis 引擎：Redis 5 / 6.2 / 7 / Apache Kvrocks 2.15.0

## 目录结构
```
redis-manager/
  ├── cmd/
  │   ├── server/
  │   ├── agent/
  │   └── cli/
  ├── internal/
  │   ├── state/       # pool-state / instances-state 读写 + 文件锁
  │   ├── server/      # Server HTTP handler
  │   ├── agent/       # Agent HTTP handler + 定时任务
  │   └── podman/      # Podman API 封装
  └── pkg/
      └── apitypes/    # Server ↔ Agent 共享请求/响应结构体
```

## 配置文件
- `server.yaml`：Server 监听端口、CLI 鉴权 Token（空则不鉴权）、Redis/Kvrocks 版本白名单
- `pool-state.yaml`：服务器资源池，含各 Agent endpoint 和 Token（空则不鉴权）
- `instances-state.yaml`：所有实例的完整状态
- `~/.redis-pilot-cli/config.yaml`：CLI 配置，Server 地址默认 127.0.0.1:8080

## 认证
- CLI → Server：Bearer Token，Token 为空则跳过鉴权
- Server → Agent：Bearer Token，Token 为空则跳过鉴权

## 开发顺序
1. Server（状态文件读写 + HTTP API）
2. Agent（Podman 管理 + 定时任务）
3. CLI（子命令封装）
4. Envoy 集成
5. GAL Skills

## 参考文档
- 详细设计：ARCHITECTURE.md

## 说明
如果有些文档描述不完整，或者不合理，修改以后需要更新文档`ARCHITECTURE.md`
