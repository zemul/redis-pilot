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

2. 执行恢复（命令内部自动完成 停止实例 → 替换数据 → 启动实例）
   ```
   redis-pilot-cli backup restore <name> --backup-ts <backup-timestamp>
   ```

3. 验证实例状态
   ```
   redis-pilot-cli instance status <name>
   ```

## 定时备份

```
redis-pilot-cli backup get-schedule <name>
redis-pilot-cli backup set-schedule <name> --cron "0 2 * * *" --retention 7
redis-pilot-cli backup set-schedule <name> --cron ""
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| backup-ts | 恢复时必填 | `backup list` 返回的备份时间戳 |
| cron | 设置定时备份时必填 | cron 表达式；空字符串表示关闭 |
| retention | 否 | 保留备份份数，0 表示保持当前值 |

## 注意事项

- 恢复会停止实例、替换数据文件、重启
- 优先从从库备份，减少主库压力
- 定时备份由 Server 内置 scheduler 驱动，配置保存在实例状态中，Agent 不独立维护调度
- 审计级别：备份 normal，恢复 critical
