# Redis Pilot 部署文档

## 架构概览

```
用户 → GAL Skills → CLI (redis-tool) → Server (:8080) → Agent (:8400) → Podman 容器
                                                                         ↓
                                                                   Envoy 代理
```

| 组件 | 部署位置 | 端口 | 职责 |
|------|---------|------|------|
| Server | 控制节点 × 1 | 8080 | 全局状态管理、API 网关、资源调度、审计日志 |
| Agent | 每台数据节点 × N | 8400 | Podman 容器管理、健康检查、备份执行 |
| CLI | 运维机器 | - | 命令行工具，调用 Server API |
| Envoy | 控制节点 | 16379-16499 | Redis 代理，读写分离 |

---

## 前置条件

- Go 1.25+（编译用）
- Podman（每台数据节点）
- Envoy（可选，需要代理层时部署）
- Linux x86_64（生产环境）

---

## 1. 编译

```bash
cd /path/to/redis-disign
make all
```

产出三个二进制：

```
bin/redis-server   # Server
bin/redis-agent    # Agent
bin/redis-cli      # CLI (redis-tool)
```

交叉编译示例：

```bash
GOOS=linux GOARCH=amd64 make all
```

---

## 2. 部署 Server（控制节点）

### 2.1 创建目录

```bash
mkdir -p /opt/redis-server/{state,audit,logs,envoy}
```

### 2.2 放置二进制

```bash
cp bin/redis-server /opt/redis-server/
chmod +x /opt/redis-server/redis-server
```

### 2.3 编写配置

`/opt/redis-server/server.yaml`：

```yaml
port: 8080
token: "your-secret-token"       # 空字符串则不鉴权
data_dir: /opt/redis-server/state

# Envoy 属于预部署代理层；不需要代理层可留空
# 非空时 Server 只负责写入 envoy.yaml，不创建 Envoy 容器
envoy_dir: /opt/redis-server/envoy
# 仅在 Envoy 已提前部署且该命令可用时配置；否则留空
envoy_reload_cmd: "podman restart envoy"

# 端口分配范围
ports:
  redis:
    start: 6379
    end: 6499
  envoy_auto:
    start: 16379
    end: 16499
  envoy_master:
    start: 16500
    end: 16619

# 实例版本目录由 Server 统一维护，Agent 只执行 Server 下发的镜像
images:
  redis:
    default: "7"
    versions:
      "5": docker.io/redis:5
      "6.2": docker.io/redis:6.2
      "7": docker.io/redis:7
  kvrocks:
    default: "2.15.0"
    versions:
      "2.15.0": docker.io/apache/kvrocks:2.15.0

log:
  dir: /opt/redis-server/logs
  stdout: true
```

### 2.4 Systemd 服务

`/etc/systemd/system/redis-server.service`：

```ini
[Unit]
Description=Redis Pilot Server
After=network.target

[Service]
Type=simple
ExecStart=/opt/redis-server/redis-server --config /opt/redis-server/server.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now redis-server
```

### 2.5 验证

```bash
curl http://localhost:8080/pool/query
# 应返回 {"servers":{}} 或类似空资源池响应
```

---

## 3. 部署 Agent（每台数据节点）

### 3.1 创建目录

```bash
mkdir -p /opt/redis-agent/logs
mkdir -p /data/redis
```

### 3.2 放置二进制

```bash
cp bin/redis-agent /opt/redis-agent/
chmod +x /opt/redis-agent/redis-agent
```

### 3.3 编写配置

`/opt/redis-agent/agent.yaml`：

```yaml
port: 8400
token: "agent-secret-token"      # 空字符串则不鉴权
data_dir: /data/redis
log:
  dir: /opt/redis-agent/logs
  stdout: true
```

### 3.4 Systemd 服务

`/etc/systemd/system/redis-agent.service`：

```ini
[Unit]
Description=Redis Pilot Agent
After=network.target podman.service

[Service]
Type=simple
ExecStart=/opt/redis-agent/redis-agent --config /opt/redis-agent/agent.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now redis-agent
```

### 3.5 验证

```bash
curl http://localhost:8400/host/resources
curl http://localhost:8400/host/health
```

---

## 4. 配置 CLI

在运维机器上放置 CLI 二进制并创建配置：

```bash
cp bin/redis-cli /usr/local/bin/redis-tool
chmod +x /usr/local/bin/redis-tool

mkdir -p ~/.redis-pilot-cli
cat > ~/.redis-pilot-cli/config.yaml << 'EOF'
server: 10.0.1.1:8080
token: "your-secret-token"
EOF
```

也可通过环境变量覆盖：

```bash
export REDIS_SERVER_TOKEN="your-secret-token"
```

---

## 5. 注册服务器到资源池

部署完所有 Agent 后，将服务器注册到 Server 的资源池：

```bash
redis-tool pool-add server-a 10.0.1.10 8400
redis-tool pool-add server-b 10.0.1.11 8400
redis-tool pool-add server-c 10.0.1.12 8400

# 可选：设置标签用于调度
redis-tool pool-update server-a --labels zone=az-1,role=production
redis-tool pool-update server-b --labels zone=az-2,role=production
```

验证：

```bash
redis-tool pool-query
```

---

## 6. 创建测试实例

```bash
# 单点实例
redis-tool instance-create test-cache \
  --category cache \
  --engine redis \
  --memory 1Gi

# 主从实例
redis-tool instance-create test-order \
  --category persistent \
  --engine kvrocks \
  --memory 2Gi \
  --type replication

# 验证
redis-tool instance-list
redis-tool instance-status test-cache
```

---

## 7. 认证配置

系统有两层认证，均为 Bearer Token，Token 为空则跳过鉴权。

| 链路 | 配置位置 | 字段 |
|------|---------|------|
| CLI → Server | `~/.redis-pilot-cli/config.yaml` | `token` |
| Server → Agent | `pool-state.yaml` 中每台服务器的 `agent_token` | `agent_token` |

生产环境建议两层都启用 Token。

---

## 8. 端口与防火墙

### 控制节点

| 端口 | 用途 | 开放范围 |
|------|------|---------|
| 8080 | Server API | CLI 所在机器 |
| 16379-16499 | Envoy 读写代理 | 业务应用 |
| 16500-16619 | Envoy 只读代理 | 业务应用 |

### 数据节点

| 端口 | 用途 | 开放范围 |
|------|------|---------|
| 8400 | Agent API | 仅控制节点 |
| 6379-6499 | Redis/Kvrocks 实例 | 仅控制节点（通过 Envoy 暴露） |

---

## 9. 目录结构总览

### 控制节点

```
/opt/redis-server/
├── redis-server              # 二进制
├── server.yaml               # 配置
├── state/
│   ├── pool-state.yaml       # 服务器资源池
│   └── instances-state.yaml  # 实例状态
├── audit/
│   └── audit-YYYYMMDD.jsonl  # 审计日志
├── logs/
│   └── server.log
└── envoy/
    └── envoy.yaml            # 自动生成的 Envoy 配置
```

### 数据节点

```
/opt/redis-agent/
├── redis-agent               # 二进制
├── agent.yaml                # 配置
└── logs/
    └── agent.log

/data/redis/
└── {instance-name}/
    ├── conf/                  # Redis/Kvrocks 配置
    ├── data/                  # 数据文件
    └── backup/                # 备份文件
```

---

## 10. 运维操作

### 查看审计日志

```bash
tail -f /opt/redis-server/audit/audit-*.jsonl
cat /opt/redis-server/audit/audit-20260429.jsonl | jq .
```

### 手动触发状态校验

```bash
curl -X POST http://localhost:8080/reconcile
```

### 备份与恢复

```bash
redis-tool backup-exec order-master
redis-tool backup-list order-master
redis-tool backup-restore order-master 2026-04-29T12:00:00
```

### 故障转移

```bash
# 手动提升从库为主库
redis-tool instance-promote order-replica
```

---

## 11. 故障排查

### Server 无法启动

```bash
journalctl -u redis-server -f
cat /opt/redis-server/logs/server.log
lsof -i :8080
```

### Agent 无法连接

```bash
curl http://<agent-ip>:8400/host/health
ping <agent-ip>
iptables -L | grep 8400
```

### 实例异常

```bash
redis-tool instance-status <name>
# 登录对应数据节点
podman ps -a | grep redis
podman logs redis-<name>
```

### 状态不一致

```bash
# 触发 reconcile 自动修复
curl -X POST http://localhost:8080/reconcile
tail -f /opt/redis-server/logs/server.log | grep reconcile
```

---

## 12. 生产建议

1. **启用认证** — Server 和 Agent 都配置非空 Token
2. **网络隔离** — Agent 端口 (8400) 和 Redis 端口 (6379) 仅对控制节点/预部署 Envoy 开放，业务通过 Envoy 访问
3. **状态文件备份** — 定期备份 `/opt/redis-server/state/` 目录
4. **审计日志归档** — 配置日志轮转，关键操作保留 365 天
5. **资源预留** — 数据节点预留 10-20% 内存给系统和 Agent
6. **主从跨可用区** — 通过 labels 标记可用区，调度器自动将主从分布到不同 zone
