---
name: redis-pilot
description: Redis 多实例管理平台 CLI 工具总览，包含所有可用命令
---

# redis-pilot-cli

Redis/Kvrocks 多实例管理 CLI 工具。通过 HTTP API 与 Server 通信，管理实例生命周期。

## 连接配置

配置文件：`~/.redis-pilot-cli/config.yaml`

```yaml
server: 127.0.0.1:8080
token: ""
operator: ""
```

也可通过命令行参数覆盖：`--server`、`--token`、`--operator`

## 可用命令

### 节点管理
```
redis-pilot-cli node list                                      # 查看节点
redis-pilot-cli node add <name> --endpoint <ip> --cpu <n> --memory <mem> [--disk <disk>] [--agent-port <port>] [--agent-token <token>] [--zone <zone>] [--role <role>]
redis-pilot-cli node remove <name>                  # 移除服务器
redis-pilot-cli node update <name> --json ./server.json          # 更新服务器信息
```

### 实例管理
```
redis-pilot-cli instance list                       # 列出所有实例
redis-pilot-cli instance status <name>              # 查看实例状态
redis-pilot-cli instance create <name> --group <group> --memory <mem>
redis-pilot-cli instance create <name> --node <server> --group <group> --category <cache|persistent> --engine <redis|kvrocks> --memory <mem> [--access-mode <standard|rw-split>]
redis-pilot-cli instance create <replica> --replica-of <master> --node <server> --memory <mem>
redis-pilot-cli instance delete <name>              # 删除实例
redis-pilot-cli instance start <name>               # 启动实例
redis-pilot-cli instance stop <name>                # 停止实例（主库需加 --force）
redis-pilot-cli instance config <name> --set k=v    # 热更新配置（CONFIG SET + 写文件）
redis-pilot-cli instance config <name> --set k=v --restart  # 写文件后重启容器（角色不回退）
redis-pilot-cli instance resize <name> --memory <mem>       # 变配内存（cgroup + maxmemory，不停机）
redis-pilot-cli instance resize <name> --cpus <n>           # 变配 CPU（cgroup，不停机）
redis-pilot-cli instance resize <name> --disk <disk>        # 变配磁盘规划配额（仅元数据，不动容器）
redis-pilot-cli instance failover <name>            # 触发故障转移（参数仅用于定位 group，Sentinel 自行选举新主库）
redis-pilot-cli instance set-access-mode <name> <standard|rw-split>  # 设置代理访问模式
```

### 备份管理
```
redis-pilot-cli backup exec <name>                  # 执行备份
redis-pilot-cli backup list <name>                  # 查看备份列表
redis-pilot-cli backup restore <name> --backup-ts <ts> # 恢复指定时间戳备份
redis-pilot-cli backup get-schedule <name>          # 查看定时备份配置
redis-pilot-cli backup set-schedule <name> --cron "0 2 * * *" --retention 7
redis-pilot-cli backup set-schedule <name> --cron "" # 关闭定时备份
```

### Sentinel
```
redis-pilot-cli sentinel status                     # 查看 Sentinel 状态
redis-pilot-cli sentinel sync                       # 从 Sentinel 同步故障转移状态
```

### Proxy 代理

当前 CLI 不提供 `proxy` 子命令。实例创建、删除、主从切换会更新 Server 状态中的 Proxy 端口和后端拓扑；Proxy 为自建 Go 二进制（`cmd/proxy/`），部署为 systemd 服务，配置文件 `proxy.yaml`（含 `server_endpoint`、`poll_interval`、`metrics_listen`、`sentinel_listen` 等）。Proxy 通过 HTTP 长轮询 `/api/v1/proxy/snapshot` 自动发现拓扑变更（无 xDS、无 gRPC）。不要在技能里生成 Proxy 配置文件或执行 reload。

Proxy 内置 pseudo-sentinel 服务（默认 `:26380`），兼容 Redis Sentinel 协议，支持客户端通过 Sentinel 模式自动发现主库地址。access_mode 为 standard 时返回 master_port，为 rw-split 时返回 auto_port。

```
curl http://<server>/api/v1/proxy/snapshot          # 只读查看控制面快照
redis-cli -p 26380 SENTINEL get-master-addr-by-name <group>  # 通过 pseudo-sentinel 查询
```

### 审计日志
```
redis-pilot-cli audit                               # 最近 30 条审计日志
redis-pilot-cli audit --group <g>                   # 按实例组过滤
redis-pilot-cli audit --instance <name>             # 按实例过滤
redis-pilot-cli audit --level critical              # 按级别过滤
redis-pilot-cli audit --action instance.create      # 按操作过滤
redis-pilot-cli audit --from 20260501 --to 20260510 # 日期范围
```

### 指标与诊断
```
redis-pilot-cli instance metrics <name>             # 采集实时 INFO 指标
redis-pilot-cli instance diagnose <name>            # 聚合诊断（INFO + SLOWLOG + MEMORY DOCTOR）
```

### 其他
```
redis-pilot-cli version                             # 版本信息
```
