---
name: redis-diagnose
description: 诊断 Redis/Kvrocks 实例问题，采集指标并分析
---

# redis-diagnose

采集实例运行指标，AI 辅助分析问题并给出建议。

## 流程

1. 获取实例状态
   ```
   redis-pilot-cli instance status <name>
   ```

2. 采集关键指标（通过 Server 转发到 Agent metrics 接口）：
   ```
   redis-pilot-cli metrics <name>
   ```

3. 重点查看：
   - INFO 全量信息（memory、clients、stats、replication、keyspace）
   - SLOWLOG GET 20
   - MEMORY DOCTOR（Redis）
   - 容器资源使用（CPU/内存/磁盘）

4. AI 分析以下维度：
   - 内存使用率和碎片率
   - 连接数是否异常
   - 慢查询模式
   - 主从同步延迟
   - 持久化状态
   - 淘汰策略是否合理

5. 输出诊断报告和优化建议

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |

## 诊断维度

| 指标 | 告警阈值 | 建议 |
|------|----------|------|
| used_memory / maxmemory | > 80% | 扩容或清理 key |
| mem_fragmentation_ratio | > 1.5 | 重启或 MEMORY PURGE |
| connected_clients | > 1000 | 检查连接池配置 |
| master_link_status | down | 检查网络/主库状态 |
| rdb_last_bgsave_status | err | 检查磁盘空间 |
