---
name: redis-scale
description: 扩容 Redis 实例（加从库或提升配置）
---

# redis-scale

加从库或通过替换式升级提升实例配置。

## 加从库流程

1. 创建从库（Server 自动选择有资源的服务器）
   ```
   redis-pilot-cli instance create <replica-name> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

2. 等待同步完成
   ```
   redis-pilot-cli instance status <replica-name>
   ```

3. 确认控制面快照已包含新从库。Envoy 不通过 CLI 手动改路由；`redis-pilot-xds` 自动下发。
   ```
   redis-pilot-cli instance status <replica-name>
   redis-pilot-cli inventory --view port
   ```

## 提升配置流程（替换式扩容）

通过创建更高配置的从库，再 promote 为主库来实现扩容。

1. 创建高配从库
   ```
   redis-pilot-cli instance create <new-name> \
     --engine <engine> \
     --memory <new-memory> \
     --replica-of <master-name>
   ```

2. 等待复制追平
   ```
   redis-pilot-cli instance status <new-name>
   ```

3. 提升新实例为主库
   ```
   redis-pilot-cli instance promote <new-name>
   ```

4. 其他从库改复制目标
   ```
   redis-pilot-cli instance replicate <old-replica> --replica-of <new-name>
   ```

5. 删除旧主库
   ```
   redis-pilot-cli instance delete <old-master>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| master | 是 | 主库名称 |
| action | 是 | add-replica 或 upgrade |
| memory | upgrade 时必填 | 新内存规格 |

## 注意事项

- 加从库时优先选择与主库不同的服务器（跨机容灾）
- 缩容（删从库）使用 redis-delete
- Redis/Envoy 端口由 Server 自动分配和发布，不从 CLI 传 `--port`
- 提升配置本质是替换式升级：新建高配从库 → 同步 → promote → 清理旧实例
