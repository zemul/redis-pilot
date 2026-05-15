---
name: redis-delete
description: 删除 Redis/Kvrocks 实例组
---

# redis-delete

停止并删除整个实例组（先删从库，再删主库）。

## 流程

1. 确认实例组存在，获取组内所有实例
   ```
   redis-pilot-cli instance list --group <group>
   ```

2. 提示用户确认删除（数据丢失风险，审计级别 critical）

3. 如果是主从实例组（type: replication），先删除所有从库
   从步骤 1 的 `instance list` 结果中筛选 role=replica 的实例，逐个删除：
   ```
   redis-pilot-cli instance delete <replica-instance-name> --clean-data
   ```
   逐个删除，等待每个从库停止并移除后再继续下一个。

4. 删除主库（从步骤 1 结果中筛选 role=master 的实例）
   ```
   redis-pilot-cli instance delete <master-instance-name> --clean-data
   ```

5. Sentinel 和 Envoy 路由无需手动操作：
   - Server 在删除主库后会自动调用 `removeSentinelMaster` + `syncSentinel`
   - `redis-pilot-xds` 会从 snapshot 自动下发新的 Envoy 配置，移除该组的端口映射

6. 确认清理完成
   ```
   redis-pilot-cli instance status <group>
   ```
   预期返回"实例组不存在"。

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| group | 是 | 实例组名称（如 order） |
| --clean-data | 否 | 同时清理数据目录，默认保留 |

## 注意事项

- 必须先删从库再删主库，避免主库删除后从库变为孤儿状态
- 默认保留数据目录；只有确认不再需要数据时才使用 `--clean-data`
- 删除操作不可逆，审计级别为 critical
- 如果只需删除某个从库而非整组，使用 `redis-pilot-cli instance delete <instance-name>` 单独操作
