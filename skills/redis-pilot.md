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

### 资源池管理
```
redis-pilot-cli pool query                          # 查看资源池
redis-pilot-cli pool add <name> --endpoint <ip>     # 添加服务器
redis-pilot-cli pool remove <name>                  # 移除服务器
redis-pilot-cli pool update <name> --labels k=v     # 更新服务器信息
```

### 实例管理
```
redis-pilot-cli instance list                       # 列出所有实例
redis-pilot-cli instance status <name>              # 查看实例状态
redis-pilot-cli instance create <name> --node <server> --port <port> --engine <redis|kvrocks> --memory <mem>
redis-pilot-cli instance delete <name>              # 删除实例
redis-pilot-cli instance start <name>               # 启动实例
redis-pilot-cli instance stop <name>                # 停止实例
redis-pilot-cli instance config <name> --set k=v    # 修改配置
redis-pilot-cli instance promote <name>             # 提升为主库
redis-pilot-cli instance replicate <name> --replica-of <master>  # 设置复制
```

### 备份管理
```
redis-pilot-cli backup exec <name>                  # 执行备份
redis-pilot-cli backup restore <name> --file <f>    # 恢复备份
redis-pilot-cli backup list <name>                  # 查看备份列表
redis-pilot-cli backup schedule <name> --cron "..."  # 定时备份
```

### Envoy 代理
```
redis-pilot-cli envoy config                        # 查看 Envoy 配置
redis-pilot-cli envoy route-update --group <g> --master <addr> --replica <addr>
```

### 审计日志
```
redis-pilot-cli audit                               # 今天的审计日志
redis-pilot-cli audit --group <g>                   # 按实例组过滤
redis-pilot-cli audit --instance <name>             # 按实例过滤
redis-pilot-cli audit --level critical              # 按级别过滤
redis-pilot-cli audit --action instance.create      # 按操作过滤
redis-pilot-cli audit --from 20260501 --to 20260510 # 日期范围
```

### 资源清单
```
redis-pilot-cli inventory                           # 全部清单
redis-pilot-cli inventory --server <name>           # 按服务器
redis-pilot-cli inventory --engine redis            # 按引擎
```

### 其他
```
redis-pilot-cli version                             # 版本信息
```
