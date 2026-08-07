# Redis Pilot 部署文档

## 架构概览

```
                              ┌─────────────────────────────────────────────┐
                              │              VIP (keepalived)               │
                              │         10.0.0.100 (浮动)                    │
                              └──────────────┬──────────────────────────────┘
                                             │
                     ┌────────────────────────┴────────────────────────┐
                     │                                                  │
   控制节点 A (MASTER)                          控制节点 B (BACKUP)
   ┌─────────────────────────┐                 ┌─────────────────────────┐
   │ Server  (:8080)  ← 主    │                 │ Server  (:8080)  ← 备    │
   │ Proxy   (:16379-16619)  │                 │ Proxy   (:16379-16619)  │
   │ Proxy Metrics (:19100)  │                 │ Proxy Metrics (:19100)  │
   │ keepalived (MASTER)      │                 │ keepalived (BACKUP)      │
   └────────────┬────────────┘                 └────────────┬────────────┘
                │                                           │
                │  Server ← rsync state/ ──-> B (每 1min)     │
                │                                           │
                └───────────────┬───────────────────────────┘
                                │
           ┌────────────────────┼────────────────────┐
           ▼                    ▼                    ▼
   数据节点 A              数据节点 B              数据节点 C
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
   │ Agent  :8400  │      │ Agent  :8400  │      │ Agent  :8400  │
   │ Sentinel:26379│      │ Sentinel:26379│      │ Sentinel:26379│
   │ Redis  :6379+ │      │ Redis  :6379+ │      │ Redis  :6379+ │
   └──────────────┘      └──────────────┘      └──────────────┘

   用户 -> GAL Skills -> CLI (redis-tool) -> Server (VIP:8080) -> Agent (:8400) -> Podman 容器
   业务应用 -> Proxy (VIP:16379-16619) -> Redis/Kvrocks (:6379-6499)
   Proxy -- HTTP 长轮询 --> Server (VIP:8080) 拉取拓扑快照
```

| 组件 | 部署位置 | 端口 | 职责 |
|------|---------|------|------|
| Server | 控制节点 A（主）+ B（冷备） | 8080 | 全局状态管理、API 网关、资源调度、审计日志 |
| Agent | 每台数据节点 × N | 8400 | Podman 容器管理、健康检查、备份执行 |
| CLI | 运维机器 | - | 命令行工具，调用 Server API |
| Proxy | 控制节点 A + B | 16379-16619（代理）+ 19100（指标） | Redis 读写分离代理，通过 HTTP 长轮询从 Server 拉取拓扑快照 |
| keepalived | 控制节点 A + B | - | VIP 浮动，保障 Proxy 和 Server API 高可用 |
| Sentinel | 数据节点 × 3 | 26379 | Redis 主从自动故障转移 |

> **HA 模型**：控制节点 A 为主节点运行 Server + Proxy；控制节点 B 为备节点，Proxy 常驻运行（keepalived BACKUP），Server 以冷备方式部署（systemd enable 但不 start）。当 A 故障时 keepalived 将 VIP 浮动到 B，并触发 notify 脚本拉起 B 上的 Server。Proxy 无持久状态，自动通过长轮询发现拓扑变更，无需额外拉起。

---

## 前置条件

- Go 1.25+（编译用）
- Podman（每台数据节点）
- keepalived（控制节点 A + B）
- rsync（控制节点 A，用于状态同步）
- Linux x86_64（生产环境）

---

## 1. 编译

```bash
cd /path/to/redis-disign
make all
```

产出四个二进制：

```
bin/redis-server     # Server
bin/redis-agent      # Agent
bin/redis-cli        # CLI (redis-tool)
bin/redis-proxy      # Proxy（读写分离代理）
```

交叉编译示例：

```bash
GOOS=linux GOARCH=amd64 make all
```

---

## 2. 部署 Server（控制节点 A 主 + B 冷备）

> Server 以文件型状态管理（pool-state.yaml + instances-state.yaml）为基础，天然支持冷启动恢复。A 为主节点常驻运行，B 为冷备节点（状态文件通过 rsync 同步，故障时脚本拉起）。

### 2.1 控制节点 A - 主 Server

#### 2.1.1 创建目录

```bash
mkdir -p /opt/redis-server/{state,audit,logs}
```

#### 2.1.2 放置二进制

```bash
cp bin/redis-server /opt/redis-server/
chmod +x /opt/redis-server/redis-server
```

#### 2.1.3 编写配置

`/opt/redis-server/server.yaml`：

```yaml
port: 8080
token: "your-secret-token"       # 空字符串则不鉴权
data_dir: /opt/redis-server/state

# 端口分配范围
ports:
  redis:
    start: 6379
    end: 6499
  proxy_auto:
    start: 16379
    end: 16499
  proxy_master:
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

# Sentinel 高可用配置（见第 7 节）
sentinel:
  enabled: true
  nodes:
    - redis-a      # pool-state.yaml 中的节点名
    - redis-b
    - redis-c
  port: 26379
  quorum: 2
```

#### 2.1.4 Systemd 服务

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

#### 2.1.5 验证

```bash
curl http://localhost:8080/node/list
# 应返回 {"servers":{}} 或类似空节点响应
```

### 2.2 控制节点 B - 冷备 Server

#### 2.2.1 创建目录并放置二进制

```bash
mkdir -p /opt/redis-server/{state,audit,logs}
cp bin/redis-server /opt/redis-server/
chmod +x /opt/redis-server/redis-server
```

#### 2.2.2 复制配置

将 A 上的 `server.yaml` 复制到 B（配置完全相同）：

```bash
# 在 A 上执行
scp /opt/redis-server/server.yaml <B-IP>:/opt/redis-server/server.yaml
```

#### 2.2.3 Systemd 服务（enable 但不 start）

创建与 A 相同的 systemd unit，但**不启动**：

```bash
systemctl daemon-reload
systemctl enable redis-server     # 开机自启（但当前不运行）
# 注意：不执行 systemctl start，B 上的 Server 保持停止状态
```

> B 上的 Server 由 keepalived notify 脚本在 A 故障时自动拉起（见第 6 节）。

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
server: 10.0.0.100:8080          # 使用 VIP，A 故障时自动切到 B
token: "your-secret-token"
EOF
```

也可通过环境变量覆盖：

```bash
export REDIS_SERVER_TOKEN="your-secret-token"
```

> CLI 连接 VIP 而非固定 IP，确保 A 故障后 CLI 仍可操作 B 上的 Server。

---

## 5. 部署 Proxy（控制节点 A + B）

> Proxy 是自研 Go 读写分离代理（`redis-proxy` 二进制）。Proxy 无持久状态，通过 HTTP 长轮询从 Server 拉取拓扑快照（`/api/v1/proxy/snapshot`），动态开 listener、维护后端连接池。A/B 两台控制节点各运行一个 Proxy 实例，通过 keepalived VIP 实现主备——只有持有 VIP 的节点接收业务流量。

### 5.1 控制节点 A - 主 Proxy

#### 5.1.1 创建目录并放置二进制

```bash
mkdir -p /opt/redis-pilot
cp bin/redis-proxy /opt/redis-pilot/redis-pilot
chmod +x /opt/redis-pilot/redis-pilot
```

#### 5.1.2 编写配置

`/opt/redis-pilot/proxy.yaml`：

```yaml
# Discovery 模式：从 Server 拉取快照，动态驱动 listener 和连接池
server_endpoint: "http://127.0.0.1:8080"   # 本机 Server 地址（A 上 Server 和 Proxy 同机）
server_token: "your-secret-token"          # 与 server.yaml 中的 token 一致，空字符串则不鉴权

poll_interval: 2s          # 两次请求的最小间隔（长轮询下仅作下限）
poll_timeout: 5s           # 网络往返余量，不含服务端挂起时长
poll_wait: 20s             # 长轮询挂起上限，0 关闭长轮询退回定期轮询

metrics_listen: ":19100"   # Prometheus /metrics 地址（空 = 关闭）
```

> Proxy 快照中包含连接后端 Redis/Kvrocks 所需的明文密码，生产环境务必设置 `server_token`，
> 并在 `server.yaml` 中配置相同的 token，否则任何能访问 Server API 的网络主体都可拉取全部密码。

配置字段说明：

| 字段 | 说明 |
|------|------|
| `server_endpoint` | Server API 地址 |
| `server_token` | Bearer Token（空 = 不鉴权） |
| `poll_interval` | 两次轮询请求的最小间隔 |
| `poll_timeout` | 网络往返超时 |
| `poll_wait` | 长轮询最大挂起时长（0 = 关闭长轮询，退回定期轮询） |
| `metrics_listen` | Prometheus 指标监听地址（`:19100`，空 = 关闭） |

#### 5.1.3 Systemd 服务

`/etc/systemd/system/redis-pilot.service`：

```ini
[Unit]
Description=Redis Pilot Proxy (read/write splitting)
After=network.target

[Service]
ExecStart=/opt/redis-pilot/redis-pilot -config /opt/redis-pilot/proxy.yaml
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now redis-pilot
```

#### 5.1.4 验证

```bash
journalctl -u redis-pilot --no-pager -n 5
# 应看到 "waiting for first snapshot" 随后 "proxy listening" 日志

# 检查指标端点
curl http://localhost:19100/metrics | grep proxy_

# 测试 Redis 代理（通过 VIP）
redis-cli -h 10.0.0.100 -p 16500 PING
```

### 5.2 控制节点 B - 备 Proxy（常驻运行）

#### 5.2.1 创建目录并放置二进制

```bash
mkdir -p /opt/redis-pilot
cp bin/redis-proxy /opt/redis-pilot/redis-pilot
chmod +x /opt/redis-pilot/redis-pilot
```

#### 5.2.2 编写配置

`/opt/redis-pilot/proxy.yaml`：

```yaml
# Discovery 模式：从 Server 拉取快照
server_endpoint: "http://127.0.0.1:8080"   # 本机 Server 地址（B 故障切换后 Server 在 B 上运行）
server_token: "your-secret-token"          # 与 server.yaml 中的 token 一致

poll_interval: 2s
poll_timeout: 5s
poll_wait: 20s

metrics_listen: ":19100"
```

> B 上的 Proxy 配置与 A 完全相同。在 B 的 Server 尚未拉起时，Proxy 轮询本地 8080 会失败并记录日志，不影响 A 上的 Proxy 正常工作。当 B 的 Server 被 keepalived notify 脚本拉起后，B 的 Proxy 自动恢复拉取快照。

> 也可将 `server_endpoint` 指向 VIP（`http://10.0.0.100:8080`），这样无论 VIP 在哪个节点，Proxy 都能拉到活跃 Server 的快照。生产环境推荐此配置。

#### 5.2.3 Systemd 服务

创建与 A 相同的 systemd unit：

```bash
systemctl daemon-reload
systemctl enable --now redis-pilot    # B 上 Proxy 常驻运行
```

### 5.3 启动顺序

部署或重启时必须按以下顺序：

1. Server（提供 `/api/v1/proxy/snapshot` API）
2. redis-pilot proxy（长轮询 Server，拉取拓扑快照）
3. keepalived（VIP 浮动，最后启动确保 Proxy 已就绪）

---

## 6. 部署 keepalived（控制节点 A + B）

> keepalived 为 Proxy（16379-16619）和 Server API（8080）提供 VIP 浮动，实现控制面高可用。A 为 MASTER，B 为 BACKUP。A 故障时 VIP 浮动到 B，同时触发 notify 脚本拉起 B 上的 Server。

### 6.1 安装

```bash
# A 和 B 都执行
yum install -y keepalived     # RHEL/CentOS
# 或 apt install -y keepalived  # Debian/Ubuntu
```

### 6.2 控制节点 A - keepalived MASTER

`/etc/keepalived/keepalived.conf`：

```conf
global_defs {
    router_id redis_pilot_a
    enable_script_security
}

# 检测本机 Server 是否存活
vrrp_script chk_server {
    script "/usr/bin/curl -sf http://127.0.0.1:8080/node/list"
    interval 3
    fall 2
    rise 1
}

vrrp_instance VI_1 {
    state MASTER
    interface eth0                    # 实际网卡名
    virtual_router_id 51
    priority 100
    advert_int 1

    track_script {
        chk_server
    }

    virtual_ipaddress {
        10.0.0.100/24                 # VIP
    }

    # A 正常运行时确保 Server 已启动
    notify_master "/etc/keepalived/notify.sh master"
    # A 降级时停止 Server（避免脑裂），Proxy 常驻保留
    notify_backup "/etc/keepalived/notify.sh backup"
    # A 故障时停止 Server，由 B 接管
    notify_fault "/etc/keepalived/notify.sh fault"
}
```

### 6.3 控制节点 B - keepalived BACKUP

`/etc/keepalived/keepalived.conf`：

```conf
global_defs {
    router_id redis_pilot_b
    enable_script_security
}

vrrp_script chk_server {
    script "/usr/bin/curl -sf http://127.0.0.1:8080/node/list"
    interval 3
    fall 2
    rise 1
}

vrrp_instance VI_1 {
    state BACKUP
    interface eth0                    # 实际网卡名
    virtual_router_id 51
    priority 90
    advert_int 1

    track_script {
        chk_server
    }

    virtual_ipaddress {
        10.0.0.100/24                 # VIP
    }

    # B 升为 MASTER 时拉起 Server，Proxy 常驻运行
    notify_master "/etc/keepalived/notify.sh master"
    # B 降为 BACKUP 时停止 Server（避免双写冲突）
    notify_backup "/etc/keepalived/notify.sh backup"
    notify_fault "/etc/keepalived/notify.sh fault"
}
```

### 6.4 notify 脚本

`/etc/keepalived/notify.sh`（A 和 B 相同）：

```bash
#!/bin/bash
# keepalived notify 脚本：控制 Server 的启停
# 关键约束：同一时刻只有一个节点运行 Server，避免 YAML 文件双写

STATE=$1
case "$STATE" in
    master)
        # 本节点成为 MASTER，拉起 Server
        systemctl start redis-server 2>/dev/null
        # Proxy 在两节点均常驻运行，无需额外拉起
        logger "keepalived: became MASTER, started redis-server"
        ;;
    backup)
        # 本节点降为 BACKUP，停止 Server（避免双写）
        systemctl stop redis-server 2>/dev/null
        logger "keepalived: became BACKUP, stopped redis-server"
        ;;
    fault)
        # 本节点故障，停止 Server
        systemctl stop redis-server 2>/dev/null
        logger "keepalived: FAULT, stopped redis-server"
        ;;
esac
exit 0
```

```bash
chmod +x /etc/keepalived/notify.sh
```

### 6.5 启动 keepalived

```bash
# A 和 B 都执行
systemctl enable --now keepalived
```

### 6.6 验证

```bash
# 在 A 上检查 VIP
ip addr show eth0 | grep 10.0.0.100
# 应看到 VIP 在 A 上

# 在 A 上停止 Server 模拟故障
systemctl stop redis-server

# 等待 ~5 秒，检查 VIP 是否浮动到 B
# 在 B 上检查
ip addr show eth0 | grep 10.0.0.100

# 检查 B 上 Server 是否被拉起
systemctl status redis-server
# 应为 active (running)

# 恢复 A 上的 Server
systemctl start redis-server
# VIP 应回到 A（priority 100 > 90）
```

---

## 7. 部署 Sentinel（数据节点 × 3）

> Sentinel 用于 Redis/Kvrocks 主从实例组的自动故障转移。3 个 Sentinel 节点部署在 3 台数据节点上，quorum=2。Sentinel 由运维独立部署，Server 通过 Redis 协议直接下发监控配置。

### 7.1 创建目录（每台数据节点都执行）

```bash
mkdir -p /data/redis-sentinel/{conf,data}
```

### 7.2 初始 sentinel.conf

`/data/redis-sentinel/conf/sentinel.conf`：

```conf
port 26379
bind 0.0.0.0
dir /data

sentinel resolve-hostnames yes

# monitor 配置由 Server 自动下发（SENTINEL MONITOR / SET / REMOVE）
# 此处只需基础配置，无需手动添加 monitor 条目

# 安全配置
sentinel deny-scripts-reconfig yes
```

> **重要**：必须挂载目录而非单文件。Sentinel 保存配置时先写 `sentinel.conf.tmp` 再 `rename` 替换原文件。若将 `sentinel.conf` 以单文件 bind mount 挂入容器，`rename` 会因 inode 被宿主机锁定而报 `Device or resource busy`。

### 7.3 启动 Sentinel 容器（每台数据节点都执行）

```bash
podman run -d \
  --name redis-sentinel \
  --network host \
  --restart on-failure:5 \
  -v /data/redis-sentinel/conf:/etc/redis:Z \
  -v /data/redis-sentinel/data:/data:Z \
  docker.io/redis:7 \
  redis-sentinel /etc/redis/sentinel.conf
```

> 使用 host network，避免容器 NAT/端口映射导致 Sentinel 宣告错误地址、无法发现其他 Sentinel 或无法连接从库。

### 7.4 在 server.yaml 中声明 Sentinel 节点

确保控制节点 A 的 `/opt/redis-server/server.yaml` 中已配置（第 2.1.3 节已包含）：

```yaml
sentinel:
  enabled: true
  nodes:
    - redis-a      # pool-state.yaml 中的节点名，对应数据节点 A
    - redis-b      # 数据节点 B
    - redis-c      # 数据节点 C
  port: 26379
  quorum: 2
```

Server 启动后会自动连接各 Sentinel 节点下发监控配置、启动 watcher 订阅 `+switch-master` 事件。

### 7.5 验证

```bash
# 在任一数据节点检查 Sentinel 状态
redis-cli -p 26379 SENTINEL masters

# 通过 Server API 检查 Sentinel 连通性
curl http://localhost:8080/sentinel/status
```

### 7.6 运维注意事项

- **一次只下线一个数据节点**：3 个 Sentinel + quorum=2，下线一台后剩余 2 台仍可达成 quorum。但同时下线两台将导致自动故障转移不可用。
- **Sentinel 节点应跨 zone 部署**：通过 labels 标记可用区，确保数据节点 A/B/C 分布在不同故障域。
- **requirepass 配置**：如果 Redis/Kvrocks 实例启用了 `requirepass`，必须通过 Server 下发 `sentinel auth-pass`，否则 Sentinel 能检测端口存活但无法执行故障转移。

---

## 8. 状态文件同步（控制节点 A -> B）

> Server 状态文件（pool-state.yaml + instances-state.yaml + 审计日志）需要从 A 定期同步到 B，确保 B 的冷备 Server 启动时拥有最新状态。

### 8.1 配置 SSH 免密（A -> B）

```bash
# 在 A 上执行
ssh-keygen -t ed25519 -f ~/.ssh/rsync_redis -N ""
ssh-copy-id -i ~/.ssh/rsync_redis.pub root@<B-IP>
```

### 8.2 同步脚本

`/opt/redis-server/sync-state.sh`（放在 A 上）：

```bash
#!/bin/bash
# 从控制节点 A 同步 Server 状态文件到控制节点 B

B_IP="<B-IP>"
SSH_KEY=~/.ssh/rsync_redis
STATE_DIR=/opt/redis-server/state
AUDIT_DIR=/opt/redis-server/audit

# 同步状态文件（pool-state.yaml + instances-state.yaml）
rsync -azq -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
  "$STATE_DIR/" "root@$B_IP:$STATE_DIR/"

# 同步审计日志
rsync -azq -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
  "$AUDIT_DIR/" "root@$B_IP:$AUDIT_DIR/"
```

```bash
chmod +x /opt/redis-server/sync-state.sh
```

### 8.3 配置 Cron 定时同步

```bash
# 在 A 上执行，每分钟同步一次
echo "* * * * * root /opt/redis-server/sync-state.sh" | tee /etc/cron.d/redis-state-sync
```

> 同步频率建议 1 分钟。Server 状态变更不频繁（实例创建/删除/故障转移），1 分钟内的状态丢失可通过 reconcile 机制恢复。

### 8.4 手动触发同步（故障切换前）

```bash
# 正常情况下 cron 自动同步
# 如需手动同步（例如大批量操作后）
/opt/redis-server/sync-state.sh
```

---

## 9. 注册服务器到节点

部署完所有 Agent 后，将服务器注册到 Server 的节点：

```bash
redis-tool node add redis-a 10.0.1.10 8400
redis-tool node add redis-b 10.0.1.11 8400
redis-tool node add redis-c 10.0.1.12 8400

# 可选：设置标签用于调度
redis-tool node update redis-a --labels zone=az-1,role=production
redis-tool node update redis-b --labels zone=az-2,role=production
redis-tool node update redis-c --labels zone=az-3,role=production
```

验证：

```bash
redis-tool node list
```

---

## 10. 创建测试实例

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

## 11. 认证配置

系统有两层认证，均为 Bearer Token，Token 为空则跳过鉴权。

| 链路 | 配置位置 | 字段 |
|------|---------|------|
| CLI -> Server | `~/.redis-pilot-cli/config.yaml` | `token` |
| Server -> Agent | `pool-state.yaml` 中每台服务器的 `agent_token` | `agent_token` |

生产环境建议两层都启用 Token。

---

## 12. 端口与防火墙

### 控制节点（A + B）

| 端口 | 用途 | 开放范围 |
|------|------|---------|
| 8080 | Server API | CLI 所在机器、本机 Proxy、keepalived 健康检查 |
| 16379-16499 | Proxy 自动读写分离代理 | 业务应用（通过 VIP） |
| 16500-16619 | Proxy Master 代理 | 业务应用（通过 VIP） |
| 19100 | Proxy Metrics | 仅运维 |
| - | keepalived VRRP | A ↔ B（协议号 112） |

### VIP

| 端口 | 用途 | 开放范围 |
|------|------|---------|
| 8080 | Server API（浮动） | CLI 所在机器 |
| 16379-16499 | Proxy 代理（浮动） | 业务应用 |
| 16500-16619 | Proxy 代理（浮动） | 业务应用 |

### 数据节点

| 端口 | 用途 | 开放范围 |
|------|------|---------|
| 8400 | Agent API | 仅控制节点（A + B） |
| 26379 | Sentinel | 控制节点（Server 直连） |
| 6379-6499 | Redis/Kvrocks 实例 | 仅控制节点（通过 Proxy 暴露） |

---

## 13. 目录结构总览

### 控制节点 A（主）

```
/opt/redis-server/
├── redis-server                   # Server 二进制
├── server.yaml                    # 配置
├── sync-state.sh                  # 状态同步脚本（仅 A 有）
├── state/
│   ├── pool-state.yaml            # 服务器节点
│   └── instances-state.yaml       # 实例状态
├── audit/
│   └── audit-YYYYMMDD.jsonl       # 审计日志
└── logs/
    └── server.log

/opt/redis-pilot/
├── redis-pilot                    # Proxy 二进制
└── proxy.yaml                     # 配置

/etc/keepalived/
├── keepalived.conf                # keepalived 配置（MASTER）
└── notify.sh                      # 故障切换脚本
```

### 控制节点 B（备）

```
/opt/redis-server/
├── redis-server                   # Server 二进制（冷备，不运行）
├── server.yaml                    # 配置（与 A 相同）
├── state/                         # 由 A rsync 同步
│   ├── pool-state.yaml
│   └── instances-state.yaml
├── audit/                         # 由 A rsync 同步
│   └── audit-YYYYMMDD.jsonl
└── logs/

/opt/redis-pilot/
├── redis-pilot                    # Proxy 二进制（常驻运行）
└── proxy.yaml                     # 配置（与 A 相同）

/etc/keepalived/
├── keepalived.conf                # keepalived 配置（BACKUP）
└── notify.sh                      # 故障切换脚本（与 A 相同）
```

### 数据节点

```
/opt/redis-agent/
├── redis-agent                    # Agent 二进制
├── agent.yaml                     # 配置
└── logs/
    └── agent.log

/data/redis/
└── {instance-name}/
    ├── conf/                      # Redis/Kvrocks 配置
    ├── data/                      # 数据文件
    └── backup/                    # 备份文件

/data/redis-sentinel/
├── conf/
│   └── sentinel.conf              # Sentinel 配置（由 Server 动态更新）
└── data/                         # Sentinel 持久化数据
```

---

## 14. 运维操作

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

### Redis 故障转移

```bash
# 手动提升从库为主库
redis-tool instance-promote order-replica

# 查看 Sentinel 状态
redis-tool sentinel status

# 从 Sentinel 同步故障转移状态到 Server
redis-tool sentinel sync
```

### 控制面故障切换

```bash
# 查看当前 VIP 所在节点
ip addr show eth0 | grep 10.0.0.100

# 手动将 VIP 切到 B（维护 A 时使用）
# 在 A 上执行：
systemctl stop keepalived
# VIP 会自动浮动到 B，B 的 Server 会被 notify 脚本拉起

# 恢复 A 后
systemctl start keepalived
# VIP 回到 A（priority 100 > 90）
```

### 状态同步检查

```bash
# 在 A 上手动触发同步
/opt/redis-server/sync-state.sh

# 在 B 上验证状态文件已同步
diff /opt/redis-server/state/pool-state.yaml <(ssh root@<A-IP> cat /opt/redis-server/state/pool-state.yaml)
```

---

## 15. 故障排查

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

### VIP 未浮动

```bash
# 检查 keepalived 状态
systemctl status keepalived
journalctl -u keepalived -f

# 检查 VRRP 通信
tcpdump -i eth0 vrrp

# 检查健康检查脚本
curl -sf http://127.0.0.1:8080/node/list && echo "OK" || echo "FAIL"
```

### B 上 Server 未被拉起

```bash
# 检查 notify 脚本日志
journalctl -u keepalived | grep notify
grep keepalived /var/log/messages

# 手动拉起
systemctl start redis-server
```

### Sentinel 不可用

```bash
# 检查 Sentinel 容器
podman ps -a | grep sentinel
podman logs redis-sentinel

# 检查 Sentinel 连通性
redis-cli -p 26379 PING
redis-cli -p 26379 SENTINEL masters

# 通过 Server 检查
curl http://localhost:8080/sentinel/status
```

---

## 16. 生产建议

### 高可用

1. **控制面 HA** - Server（冷备）+ Proxy（双活，keepalived 主备），确保单节点故障不影响业务
2. **VIP 统一入口** - CLI 和业务应用都通过 VIP 连接，避免硬编码 IP
3. **状态文件同步** - rsync cron 每 1 分钟同步 A->B，确保冷备数据新鲜度
4. **故障切换演练** - 上线前模拟 A 宕机，验证 VIP 浮动、Server 拉起、Proxy 快照恢复

### 安全

5. **启用认证** - Server 和 Agent 都配置非空 Token
6. **网络隔离** - Agent 端口 (8400) 和 Redis 端口 (6379) 仅对控制节点开放，业务通过 Proxy VIP 访问
7. **VRRP 隔离** - keepalived VRRP 通信仅限 A ↔ B，防止外部节点干扰

### 运维

8. **状态文件备份** - 定期备份 `/opt/redis-server/state/` 目录（A 和 B 都备份）
9. **审计日志归档** - 配置日志轮转，关键操作保留 365 天
10. **资源预留** - 数据节点预留 10-20% 内存给系统和 Agent；控制节点预留资源给 Proxy（建议 4C8G）
11. **主从跨可用区** - 通过 labels 标记可用区，调度器自动将主从分布到不同 zone
12. **Sentinel 余量监控** - 3 节点 quorum=2 时，同时下线两台数据节点将导致故障转移不可用，运维操作时一次只下线一台

### 资源规划

| 节点类型 | 最低规格 | 推荐规格 | 说明 |
|---------|---------|---------|------|
| 控制节点 A | 2C4G | 4C8G | 跑 Server + Proxy，高 QPS 下 Proxy 需要更多 CPU |
| 控制节点 B | 2C4G | 4C8G | 跑 Proxy 常驻，故障时额外拉起 Server |
| 数据节点 | 4C32G | 8C64G | 跑 Agent + Sentinel + Redis/Kvrocks 实例 |
