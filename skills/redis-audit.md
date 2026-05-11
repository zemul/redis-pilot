---
name: redis-audit
description: 查询操作审计日志
---

# redis-audit

查询审计日志，支持按时间/实例组/操作级别过滤。

## 流程

```
# 今天的日志
redis-pilot-cli audit

# 按实例组过滤
redis-pilot-cli audit --group order

# 按实例过滤
redis-pilot-cli audit --instance order-master

# 按级别过滤
redis-pilot-cli audit --level critical

# 按操作类型过滤
redis-pilot-cli audit --action instance.create

# 日期范围
redis-pilot-cli audit --from 20260501 --to 20260510
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| from | 否 | 开始日期 YYYYMMDD |
| to | 否 | 结束日期 YYYYMMDD |
| group | 否 | 实例组名 |
| instance | 否 | 实例名 |
| level | 否 | normal / important / critical |
| action | 否 | 操作类型 |

## 审计级别

| 级别 | 操作 |
|------|------|
| normal | 查询、配置变更 |
| important | 创建、删除、启停 |
| critical | 故障转移、迁移、恢复备份 |
