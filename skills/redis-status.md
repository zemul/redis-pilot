---
name: redis-status
description: 查看 Redis/Kvrocks 实例状态
---

# redis-status

查看实例运行状态、主从同步、资源使用。

## 流程

1. 查看单个实例
   ```
   redis-pilot-cli instance status <name>
   ```

2. 查看所有实例
   ```
   redis-pilot-cli instance list
   ```

3. 查看资源池
   ```
   redis-pilot-cli pool query
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 否 | 实例名称，不指定则列出全部 |
| server | 否 | 按服务器过滤 |

## 输出信息

- 实例状态（running/stopped/failed）
- 角色（master/replica/standalone）
- 内存使用
- 连接数
- 主从同步状态
- 最近备份时间
