# 冷热存储：历史阶段归档

2026-09-08 整理。此目录保留旧方案、阶段实现细节和当时验收结果，**不代表当前产品能力**。旧文互相引用的是历史上下文，不能据此判断现在是否可部署或自动恢复。

现行入口：[设计](../../hot-cold-gateway-design.md)、[实现进度](../../hot-cold-status.md)、[部署运维](../../gateway-deployment.md)。原始测试日志仍在 [benchmarks](../../benchmarks/)，不重写其历史结果。

## 预研与旧总设计

- [hot-cold-gateway-design.md](hot-cold-gateway-design.md)
- [hot-cold-storage-research.md](hot-cold-storage-research.md)

## 网关基础、控制面与性能阶段

- [gateway-control-plane.md](gateway-control-plane.md)
- [gateway-g4-benchmark.md](gateway-g4-benchmark.md)
- [gateway-g4-no-pipeline.md](gateway-g4-no-pipeline.md)
- [gateway-heartbeat.md](gateway-heartbeat.md)

## 同步内核与独立服务阶段

- [gateway-async-sync.md](gateway-async-sync.md)
- [gateway-kvrocks-commit.md](gateway-kvrocks-commit.md)
- [gateway-pending-recovery.md](gateway-pending-recovery.md)
- [gateway-replay-validation.md](gateway-replay-validation.md)
- [gateway-segmented-journal.md](gateway-segmented-journal.md)
- [gateway-string-apply.md](gateway-string-apply.md)
- [gateway-sync-lifecycle.md](gateway-sync-lifecycle.md)
- [gateway-sync-resources.md](gateway-sync-resources.md)
- [gateway-sync-runner.md](gateway-sync-runner.md)
- [syncer-control-plane.md](syncer-control-plane.md)
- [syncer-multitask.md](syncer-multitask.md)
- [syncer-progress.md](syncer-progress.md)
- [syncer-service.md](syncer-service.md)

## 操作屏障、准备、确认与恢复阶段

- [gateway-admission-restart.md](gateway-admission-restart.md)
- [gateway-cold-confirmation.md](gateway-cold-confirmation.md)
- [gateway-confirmation-rpc.md](gateway-confirmation-rpc.md)
- [gateway-eviction-envelope.md](gateway-eviction-envelope.md)
- [gateway-eviction-intent.md](gateway-eviction-intent.md)
- [gateway-hot-preparation.md](gateway-hot-preparation.md)
- [gateway-key-barrier.md](gateway-key-barrier.md)
- [gateway-live-confirmation.md](gateway-live-confirmation.md)
- [gateway-preparation-integration.md](gateway-preparation-integration.md)
- [gateway-request-admission.md](gateway-request-admission.md)

后续仅更新现行文档和测试记录；不再按每次小改动新增阶段架构文档。归档文件正文仅增加历史提示和修正相对链接，保留原始论述用于追溯。
