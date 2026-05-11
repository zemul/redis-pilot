---
name: redis-create
description: 创建 Redis/Kvrocks 实例，支持单点和主从
---

# redis-create

创建 Redis 或 Kvrocks 实例。

## 流程

1. 查询资源池，选择目标服务器
   ```
   redis-pilot-cli pool query
   ```

2. 创建实例
   ```
   redis-pilot-cli instance create <name> \
     --node <server> \
     --port <port> \
     --engine <redis|kvrocks> \
     --memory <memory> \
     --group <group> \
     --password <password>
   ```

3. 如果需要主从，创建从库
   ```
   redis-pilot-cli instance create <replica-name> \
     --node <replica-server> \
     --port <port> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

4. 检查实例状态
   ```
   redis-pilot-cli instance status <name>
   ```

5. 如果启用 Envoy，更新路由
   ```
   redis-pilot-cli envoy route-update --group <group> --master <master-addr> --replica <replica-addr>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| node | 是 | 目标服务器 |
| port | 否 | 端口，不指定则自动分配 |
| engine | 否 | redis 或 kvrocks，默认 redis |
| memory | 否 | 内存限制，默认 256m |
| group | 否 | 实例组名（主从共用） |
| password | 否 | 访问密码 |
| type | 否 | standalone 或 replication，默认 standalone |

## 注意事项

- 创建主从时，先创建主库再创建从库
- group 名在 Sentinel/Envoy/审计中统一使用
- 端口冲突时自动分配下一个可用端口
