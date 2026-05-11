---
name: redis-pool
description: 管理服务器资源池
---

# redis-pool

维护 pool-state.yaml，注册/移除/更新服务器。

## 流程

1. 查看资源池
   ```
   redis-pilot-cli pool query
   ```

2. 添加服务器
   ```
   redis-pilot-cli pool add <name> --endpoint <ip> --agent-port <port>
   ```

3. 移除服务器
   ```
   redis-pilot-cli pool remove <name>
   ```

4. 更新服务器信息
   ```
   redis-pilot-cli pool update <name> --labels zone=az1,role=production
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 服务器名称 |
| endpoint | 添加时必填 | 服务器 IP |
| agent-port | 否 | Agent 端口，默认 8081 |
| labels | 否 | 标签，如 zone、role |
