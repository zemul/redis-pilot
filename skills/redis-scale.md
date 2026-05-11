---
name: redis-scale
description: 扩容/缩容 Redis 实例（加从库或调整资源）
---

# redis-scale

加从库或调整实例资源。

## 加从库流程

1. 查询资源池选择从库服务器
   ```
   redis-pilot-cli pool query
   ```

2. 创建从库
   ```
   redis-pilot-cli instance create <replica-name> \
     --node <server> \
     --port <port> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

3. 等待同步完成
   ```
   redis-pilot-cli instance status <replica-name>
   ```

4. 更新 Envoy 路由（加入读负载均衡）
   ```
   redis-pilot-cli envoy route-update --group <group> --replica <new-replica-addr>
   ```

## 调整资源流程

1. 修改实例配置（内存/CPU）
   ```
   redis-pilot-cli instance config <name> --set maxmemory=512mb
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| master | 是 | 主库名称 |
| action | 是 | add-replica 或 resize |
| node | 加从库时必填 | 从库目标服务器 |
| memory | resize 时必填 | 新内存限制 |

## 注意事项

- 加从库时优先选择与主库不同的服务器（跨机容灾）
- 缩容（删从库）使用 redis-delete
