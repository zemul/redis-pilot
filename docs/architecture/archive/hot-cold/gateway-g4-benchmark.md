# G4 分负载复测（2026-09-07）

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 目的与边界

核实首次本地短测中的“Proxy → Redis 约 1.5 万 QPS”是否具有代表性；只测试基础传输，不修改 Proxy 或网关的数据路径。
该数字不是 Proxy 的容量上限：首次负载 SET/GET 交替，Proxy 会在命令读写分类变化时拆批并等待响应，无法获得纯 GET/SET 的同类组批效果。
代码依据：[session.go](../../../../internal/proxy/session/session.go) 的 pipeline 组批循环：`d2.Class != d.Class` 时停止当前批次。

## 持续负载方法

入口：[g4_sustained_test.go](../../../../internal/gateway/g4_sustained_test.go)。

- redis01，Linux/amd64，宿主机 2 个逻辑 CPU；已有 Redis 7 镜像实际版本 7.4.9。
- 独立、无外部网络的 Podman 容器，未发布端口、未挂载业务数据；临时 Redis 禁用 AOF/RDB。
- **整个测试容器共享 1 CPU 配额、768MiB 内存，所有 Go 进程 GOMAXPROCS=1。** 压测器、Redis、Proxy 和网关是独立进程，但共享这份资源，不是独立机器压测。
- 路径：Redis 直连、Proxy → Redis、网关 → Redis、Proxy → 网关 → Redis；均走主库。
- 负载：纯 GET、纯 SET、严格交替 SET/GET；value=128B，每客户端独立 key，预置数据保证 GET 命中。
- 客户端数 8/32，pipeline 固定为 8；每批完整读取响应后才发送下一批。
- 每个场景先建连、认证、预置数据和预编码请求，再在原连接上预热 2 秒、计时 10 秒。两轮共 48 个结果，第二轮轮换路径顺序。
- QPS 为测量阶段成功命令数 / 实际耗时；不含建连、认证和预热。排除跨越预热边界的批次，计入最后一批排空时间。
- 所有响应逐条验证；出错直接中止，不把错误计入成功吞吐。
- P99 为整批 8 条完成延迟，直方图分辨率 0.1ms；>=1s 进入溢出桶。不能除以 8 当单条 P99。
- 网关使用 64KiB 帧、256KiB 批次、max_pipeline=8、max_connections=256、1GiB 保守准入预算。准入预算不是实际 RSS，也不是容器内存硬限制。

### 环境约束

测量中观察到 CPU 配额限流：采样时 `cpu.stat` 的 `nr_throttled=1059`、`nr_periods=1116`，`cpu.max=100000 100000`。
因此延迟尾部包含 CPU 限流停顿，**这些 P99 不可视作无资源限制部署时的网络/网关延迟**。
同一采样时容器整体内存约 31MB、memory.peak=41132032 字节，只是中途样本，不是全程资源验收，也不是网关单进程内存。
吞吐用于比较这份固定资源下不同负载和路径的表现，不能外推开发机/生产集群容量；也不能与首次 Darwin 本地短测直接计算优化倍率。

## 独立压测器交叉检查

额外使用镜像自带的 `redis-benchmark`，单独测试 Redis 直连和 Proxy → Redis：

```text
redis-benchmark -h <isolated-host> -p <isolated-port> -a <test-password> \
  -c 32 -P 8 -d 128 -n 500000 -t set,get --csv
```

先执行每类 20000 请求的预跑，再执行每类 500000 请求。该工具的正式运行重新建连，统计口径与持续负载不同；它用于独立交叉检查纯读写，不替代混合读写测试，延迟字段也不与自制压测器的批次 P99 直接比较。

## 结果

持续测试通过，用时 577.90 秒；48 个场景累计成功校验 29,271,376 条测量阶段命令（不含预热）。
原始数据：[g4-sustained-20260907.jsonl](../../benchmarks/g4-sustained-20260907.jsonl)。以下为两轮范围，单位 **万条/秒**。

| 并发连接 | 路径 | 纯 GET | 纯 SET | SET/GET 交替 |
| --- | --- | --- | --- | --- |
| 8 | Redis 直连 | 16.53～16.97 | 16.36～16.45 | 16.44～16.52 |
| 8 | Proxy → Redis | 6.28～6.29 | 6.20～6.25 | 1.42～1.43 |
| 8 | 网关 → Redis | 1.59～1.61 | 1.60～1.64 | 1.61～1.61 |
| 8 | Proxy → 网关 → Redis | 0.83～0.83 | 0.81～0.83 | 0.62～0.63 |
| 32 | Redis 直连 | 16.97～17.22 | 17.64～17.74 | 17.45～17.56 |
| 32 | Proxy → Redis | 7.23～7.25 | 6.94～7.18 | 1.82～1.82 |
| 32 | 网关 → Redis | 1.53～1.58 | 1.57～1.58 | 1.53～1.56 |
| 32 | Proxy → 网关 → Redis | 0.82～0.84 | 0.79～0.84 | 0.70～0.71 |

四舍五入后同值的范围不代表两轮原始结果完全相同；精确数字及批次 P99 见 JSONL。

原生工具交叉检查通过，用时 22.88 秒；[完整输出](../../benchmarks/g4-native-20260907.log)。32 并发、pipeline=8、每类 500000 请求的结果（单位万条/秒）：

| 路径 | 纯 GET | 纯 SET |
| --- | --- | --- |
| Redis 直连 | 14.06 | 13.50 |
| Proxy → Redis | 6.99 | 6.70 |

两种压测器的 Proxy 纯读写结果量级一致，但不同客户端实现、key 使用和统计口径仍会影响数值。原生工具默认各客户端使用同一 key，持续负载为每客户端独立 key。

结论：

1. **不能把首次约 1.5 万 QPS 描述为 Proxy 的性能上限。** 同样 pipeline=8，本次纯读写达到约 6～7 万；交替读写则约 1.4～1.8 万。
2. 混合读写的分类切换使 Proxy 拆批，代码和本轮对照都支持这是差距来源之一；未做修改前后的消融测试，不能量化它占全部损耗的比例。
3. 新网关即使纯读写仍只有约 1.5～1.6 万，且 32 并发没有提升。这是另一项需要定位的瓶颈，不能全部归因于 Proxy 的分类拆批。
4. 当前双层实现还不适合用作高性能冷热网关的已验收版本。下一步应采集逐进程 CPU/heap profile、按场景采集批次分布，再评估有界组批与分配优化；本轮未修改生产数据路径。
5. 这些是 1 CPU 配额内共享资源的比较，不是独立服务器容量测试。跨机器网络、独立 CPU、更多并发、长时间稳态和大 value/大 keyspace 均未覆盖。

## 复现与安全约束

持续测试所用 Linux/amd64 二进制 SHA-256（工作区未提交版本）：

```text
gateway.test  974aa5085b18418e976ed7bffdabfb4207dbacbf5cec6ab75f1a564e1a755c89
redis-proxy   f033c69fcb512a61f5b289ca12ed4208c75be175eef1fa8d3e7e9daa33e8bc2c
redis-gateway 254a4ac82077eade6bede6744f487fd7cb0762bbbba10ef79abcb240816e813e
```

持续测试需显式设置 `GATEWAY_TEST_G4_SUSTAINED=1`、`GATEWAY_TEST_REDIS_SERVER`、`GATEWAY_TEST_RESULT_FILE`，运行 `go test ./internal/gateway -run '^TestG4Sustained$' -count=1 -v -timeout 15m`。
结果文件以独占新建方式打开，已有文件不会覆盖；每个场景写入 JSONL 并 Sync。
跨平台测试需另行提供 `GATEWAY_TEST_PROXY_BINARY` 与 `GATEWAY_TEST_GATEWAY_BINARY`，目标机无需 Go 工具链。
原生交叉测试入口 `TestG4NativeBenchmark`，另设 `GATEWAY_TEST_BENCHMARK_BINARY`；使用自己创建的临时 Redis，而非已有业务地址。

本轮没有改动已有 Server/Agent/Proxy 服务或实例，没有接入 Kvrocks，也没有实现冷热同步、回温或淘汰。
两次测试容器均已自动删除；测试二进制与原始 JSONL 保留在 redis01 的 `/tmp/redis-pilot-g4-sustained.San6i9`，没有安装新常驻服务。
