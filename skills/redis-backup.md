---
name: redis-backup
description: 备份和恢复 Redis/Kvrocks 实例
---

# redis-backup

执行备份或从备份恢复。

## 备份流程

1. 确定备份源（优先从库）
   ```
   redis-pilot-cli instance status <name>
   ```

2. 执行备份
   ```
   redis-pilot-cli backup exec <name>
   ```

3. 查看备份列表
   ```
   redis-pilot-cli backup list <name>
   ```

## 恢复流程

1. 查看可用备份
   ```
   redis-pilot-cli backup list <name>
   ```

2. 执行恢复
   ```
   redis-pilot-cli backup restore <name> --file <backup-file>
   ```

## 定时备份

```
redis-pilot-cli backup schedule <name> --cron "0 2 * * *"
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| file | 恢复时必填 | 备份文件名 |
| cron | 定时备份时必填 | cron 表达式 |

## 注意事项

- 恢复会停止实例、替换数据文件、重启
- 优先从从库备份，减少主库压力
- 审计级别：备份 normal，恢复 critical
