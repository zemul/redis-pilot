---
name: redis-config
description: 修改 Redis/Kvrocks 实例配置
---

# redis-config

修改实例运行时配置。

## 流程

1. 查看当前配置
   ```
   redis-pilot-cli instance status <name>
   ```

2. 修改配置
   ```
   redis-pilot-cli instance config <name> --set <key>=<value>
   ```

3. 验证配置生效
   ```
   redis-pilot-cli instance status <name>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| key=value | 是 | 配置项，如 maxmemory=512mb |

## 常用配置项

| 配置 | 说明 |
|------|------|
| maxmemory | 最大内存 |
| maxmemory-policy | 淘汰策略 |
| timeout | 客户端超时 |
| hz | 后台任务频率 |

## 注意事项

- 部分配置需要重启才能生效（如 bind、port）
- 主从组建议主从一起改
