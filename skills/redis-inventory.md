---
name: redis-inventory
description: 查询资源清单，按端口/服务器/引擎查询
---

# redis-inventory

从 instances-state + pool-state 派生端口/集群/用途映射表。

## 流程

```
redis-pilot-cli inventory
redis-pilot-cli inventory --server <name>
redis-pilot-cli inventory --engine redis
redis-pilot-cli inventory --port 6379
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| server | 否 | 按服务器过滤 |
| engine | 否 | 按引擎过滤（redis/kvrocks） |
| port | 否 | 按端口查询 |

## 输出

端口 → 实例名 → 服务器 → 引擎 → 角色 → 状态 的映射表。
