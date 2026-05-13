#!/bin/bash
# 首次部署初始化：创建目录、安装 systemd 服务、推送配置文件
# 之后更新只需 make deploy
set -e

DEPLOY_SERVER=redis01
DEPLOY_AGENTS="redis01 redis02 redis03"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# 生成 systemd service 文件到临时目录
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

cat > "$TMP/redis-pilot-server.service" << 'EOF'
[Unit]
Description=Redis Pilot Server
After=network.target

[Service]
Type=simple
ExecStart=/opt/redis-pilot-server/redis-pilot-server --config /opt/redis-pilot-server/server.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
KillMode=process
Delegate=yes

[Install]
WantedBy=multi-user.target
EOF

cat > "$TMP/redis-pilot-agent.service" << 'EOF'
[Unit]
Description=Redis Pilot Agent
After=network.target podman.service

[Service]
Type=simple
ExecStart=/opt/redis-pilot-agent/redis-pilot-agent --config /opt/redis-pilot-agent/agent.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
KillMode=process
Delegate=yes

[Install]
WantedBy=multi-user.target
EOF

echo "=== Server ($DEPLOY_SERVER) ==="
ssh "$DEPLOY_SERVER" "mkdir -p /opt/redis-pilot-server/{state,audit,logs} /opt/envoy/conf"
scp "$PROJECT_DIR/configs/server.yaml" "$DEPLOY_SERVER:/opt/redis-pilot-server/server.yaml"
scp "$TMP/redis-pilot-server.service" "$DEPLOY_SERVER:/etc/systemd/system/redis-pilot-server.service"
ssh "$DEPLOY_SERVER" "systemctl daemon-reload && systemctl enable redis-pilot-server"

for h in $DEPLOY_AGENTS; do
    echo "=== Agent ($h) ==="
    ssh "$h" "mkdir -p /opt/redis-pilot-agent/logs /data/redis"
    scp "$PROJECT_DIR/configs/agent.yaml" "$h:/opt/redis-pilot-agent/agent.yaml"
    scp "$TMP/redis-pilot-agent.service" "$h:/etc/systemd/system/redis-pilot-agent.service"
    ssh "$h" "systemctl daemon-reload && systemctl enable redis-pilot-agent"
done

echo ""
echo "初始化完成，运行 make deploy 推送二进制并启动服务。"
