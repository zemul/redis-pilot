---
name: redis-create
description: 创建 Redis/Kvrocks 实例，支持单点和主从
---

# redis-create

创建 Redis 或 Kvrocks 实例。

## 流程

1. 创建实例（Server 自动调度节点）
   ```
   redis-pilot-cli instance create <name> \
     --engine <redis|kvrocks> \
     --category <cache|persistent> \
     --memory <memory> \
     --group <group> \
     --password <password>
   ```

2. 如果需要主从，创建从库。`--replica-of` 指向主库实例名，`--group` 可省略并由 Server 继承主库实例组。
   ```
   redis-pilot-cli instance create <replica-name> \
     --engine <engine> \
     --memory <memory> \
     --replica-of <master-name>
   ```

3. 检查实例组状态
   ```
   redis-pilot-cli instance list --group <group>
   ```

4. 查看 Envoy 入口端口。实例创建成功后，Server 会分配并写入 `envoy.master_port` / `envoy.auto_port`；`redis-pilot-xds` 会从控制面 snapshot 自动下发到 Envoy。
   ```
   redis-pilot-cli inventory --port <envoy-port>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| name | 是 | 实例名称 |
| engine | 否 | redis 或 kvrocks，默认 redis |
| engine-version | 否 | Redis 5 / 6.2 / 7 或 Kvrocks 2.15.0 |
| category | 否 | cache 或 persistent，默认 cache |
| memory | 否 | 内存限制，默认 1Gi |
| disk | 否 | 磁盘规划；Kvrocks 未指定时默认 10Gi |
| cpus | 否 | CPU 核数，默认 1 |
| group | 主库/单点必填 | 实例组名（主从共用） |
| replica-of | 从库必填 | 主库实例名或地址 |
| password | 否 | 访问密码 |
| type | 否 | standalone 或 replication，默认 standalone。声明实例组的拓扑规划：standalone 为单点，replication 表示主从架构（会预分配 Envoy auto_port 用于读写分离）。添加从库时 Server 会自动将 standalone 升级为 replication |
| config | 否 | 配置覆盖，格式 `k=v,k=v` |

## 注意事项

- **实例命名不要包含角色或节点信息**（如 master、replica、node1、redis01）。主从角色会因 failover 切换，实例也可能迁移到不同服务器，名字应使用纯序号，例如 `order-1` / `order-2` 或 `order-a` / `order-b`。当前主库由 group 的 `current_master` 字段维护。
- 创建主从时，先创建主库再创建从库
- group 名在 Sentinel/Envoy/审计中统一使用
- Redis 实例端口和 Envoy 端口均由 Server 统一分配，不从 CLI 传 `--port`
- Envoy 通过 `/api/v1/proxy/snapshot` + xDS 控制面自动下发，不需要执行手动路由更新
