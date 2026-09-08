# G4 普通读写补测：pipeline=1（2026-09-07）

> 历史阶段归档：保留当时的方案与验收记录，不代表当前能力或恢复策略。现状以 [现行设计](../../hot-cold-gateway-design.md) 和 [实现进度](../../hot-cold-status.md) 为准。

## 测量口径

本报告补充[此前 pipeline=8 复测](gateway-g4-benchmark.md)，不替代它，也不混用两种模式的延迟。

- 每个连接一次只发送一条命令，收到完整响应后才发送下一条；连接之间可以并发，没有批量客户端 pipeline。
- 四条链路：Redis 直连、Proxy → Redis、网关 → Redis、Proxy → 网关 → Redis。
- 负载：纯 GET、纯 SET、SET/GET 严格交替；连接数 1/8/32；128B value，每连接独立 key，GET 全部命中。
- 每个场景先建连、认证和预置值；在同一批连接上预热 2 秒、正式计时 10 秒；两轮共 72 个场景，第二轮轮换路径顺序。
- 混合负载跨请求保持奇偶状态，不是把原 pipeline=8 模板截成单条 SET；已补 p=1/p=8 的自检。
- JSONL 同时记录 GET/SET 成功命令数，便于核验交替读写比例。每条响应均验证；不把错误作为成功吞吐。
- 数据字段 `batch_mean_ms`、`batch_p50_ms`、`batch_p95_ms`、`batch_p99_ms` 沿用公共测试结构名称；**本次批次只有一条，因此就是单条请求端到端延迟**，不包含建连、认证和预热。
- P50/P95/P99 直方图分辨率 0.01ms（数值为所在桶上界）；`latency_ge_100ms_batches` 记录 >=100ms 的溢出样本数。分位数若落入溢出桶，输出的 100.01ms 仅是溢出标记，不是实际延迟上界。平均值使用原始纳秒求和，不截断。
- 最后一条已发请求允许排空，QPS 的实际计时包括排空耗时；跨预热边界的请求不计入正式样本。

## 环境及限制

redis01、Linux/amd64、Redis 7.4.9；隔离 Podman 容器，network=none，不开放端口、不挂载业务数据，Redis 禁用持久化。
压测器、Redis、Proxy、网关为独立进程，但整个容器共享 **1 CPU 配额 / 768MiB**，Go 进程 `GOMAXPROCS=1`，与前一轮配置一致。
这不是独立 CPU/独立服务器压测；宿主机是共享开发环境，CPU 配额可能引入尾延迟，不能视为生产容量或 SLA。
该闭环负载在请求变慢时会降低发压速度，不能代表固定到达率、排队持续增长的过载场景。
中途采样确认限流实际发生：`cpu.max=100000 100000`，`nr_periods=2666`、`nr_throttled=2633`、`throttled_usec=56637001`。同次采样 `memory.peak=33804288` 字节（整个容器的截至当时峰值，不是网关独立内存，也不是全程验收）。

Proxy/网关二进制与 pipeline=8 复测 SHA-256 完全一致，没有在两轮之间优化转发路径：

```text
gateway.test  62e61f6a8b8a205ba7c27302a59f6987cd961faa9ef411d8576a875b5e22f51b
redis-proxy   f033c69fcb512a61f5b289ca12ed4208c75be175eef1fa8d3e7e9daa33e8bc2c
redis-gateway 254a4ac82077eade6bede6744f487fd7cb0762bbbba10ef79abcb240816e813e
```

## 结果

测量完成后补充原始数据与汇总。

## 复现

测试实现：[g4_sustained_test.go](../../../../internal/gateway/g4_sustained_test.go)。

```sh
GATEWAY_TEST_REDIS_SERVER=/absolute/path/to/redis-server \
GATEWAY_TEST_G4_SUSTAINED=1 GATEWAY_TEST_G4_PIPELINE=1 \
GATEWAY_TEST_RESULT_FILE=/new/isolated/path/results.jsonl \
go test ./internal/gateway -run '^TestG4Sustained$' -count=1 -v -timeout 20m
```

结果文件必须不存在，测试不会覆盖已有文件；每个场景写入并 Sync。
交叉编译时设置 `GATEWAY_TEST_PROXY_BINARY` 与 `GATEWAY_TEST_GATEWAY_BINARY`，在目标机运行对应平台的测试二进制。
原生工具交叉检查：`TestG4NativeBenchmark` 同时设置 `GATEWAY_TEST_BENCHMARK_BINARY` 和 `GATEWAY_TEST_G4_PIPELINE=1`；32 连接、128B、每类 20000 请求预跑后，每类 100000 请求正式运行。原生工具统计口径与持续负载不同，单独列结果。
