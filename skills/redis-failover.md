---
name: redis-failover
description: 手动或自动故障转移
---

# redis-failover

主库故障时提升从库为新主库。

## 自动故障转移

Sentinel 自动检测主库故障并执行切换，平台自动同步状态。无需人工干预。

## 手动故障转移

1. 确认主从状态
   ```
   redis-pilot-cli instance status <master-name>
   ```

2. 提升从库
   ```
   redis-pilot-cli instance promote <replica-name>
   ```

3. 更新其他从库复制目标
   ```
   redis-pilot-cli instance replicate <other-replica> --replica-of <new-master>
   ```

4. Envoy 路由自动更新（Sentinel 模式下自动完成）

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| group | 是 | 实例组名 |
| new-master | 否 | 指定提升的从库，不指定则自动选择 |

## 注意事项

- Sentinel 模式下故障转移全自动
- 手动 promote 后需要手动更新其他从库
- 审计级别为 critical
