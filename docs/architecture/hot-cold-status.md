# 冷热缓存实现状态

更新：2026-09-10。方向已改为 Kvrocks 权威存储 + Redis 缓存；本轮只清理旧实现，不交付新缓存链路。目标契约见 [设计](hot-cold-gateway-design.md)。

## 保留能力

- Gateway 独立二进制及 systemd 模板、配置校验。
- 固定单后端开发透传：RESP2、有限命令白名单、前后端独立认证、Pipeline 保序、超时、排空、内存预算、指标。
- Gateway 配置草稿、直接向 Server 上报心跳和运行观测。
- 原 Server/Agent/Proxy 的实例管理能力不变。

当前仍只有 skeleton/passthrough 模式，监听限 loopback；不支持 Cluster 冷热缓存。透传命令支持不等于新缓存语义已经实现。

## 本轮移除

- redis-syncer 可执行入口、构建目标、服务、配置示例、同步任务上报 API/类型/存储代码。
- RDB/PSYNC 捕获、复制身份和位点、分段日志、回放、pending/checkpoint、精确冷确认。
- Gateway 旧准备/驻留标记、旧请求屏障、手动降冷、主动水位淘汰及专属测试。
- 旧 hot_cold/state_dir 配置与 -init-state 入口；旧配置应明确失败，不能默默退回透传。
- 旧阶段归档和专属实验记录；保留通用透传基准记录，但它们不是新链路的性能验收。

已有 SQLite 中的历史同步器表不执行 DROP，现存服务、持久状态和 Redis/Kvrocks 业务数据未修改。既有未跟踪 deploy/、dump.rdb、session/ 原样保留。旧已提交源码与文档可通过 Git 提交 19d35ec 查回；未提交的旧源重启调查测试随废弃方案移除。

## 下一项完整功能

实现单 Gateway、固定端点的 GET/SET/DEL/INCR 缓存闭环：
写冷后失效热、读热未命中查冷、有界按需回填、同 key 读写协调、失效失败绕过及 Gateway 崩溃后的缓存恢复。完成完整功能和故障测试后再部署验证。

尚未实现：新缓存配置与模式、写后策略开关、Redis LRU 接入、Hash/ZSet 完整缓存、TTL、可选空值缓存、多 Gateway、Cluster 和扩容。全量布隆过滤器暂不做。

## 清理验证

- go test ./...、go vet ./... 通过。
- Gateway、Store、Server 的全套 race 测试（count=1）通过。
- 本地 Redis 7.2.5 的直接接入、Proxy → Gateway、未知写结果及慢读资源回收集成测试通过；不是新缓存链路或性能验收。
- 新增旧 hot_cold/state_dir 配置拒绝、旧同步器 API 不再注册的回归检查；文档本地链接与 git diff --check 通过。

## 开发环境边界

本轮没有连接或操作 redis01-03。redis03 上此前部署的旧 Gateway/同步器可能仍按原配置运行；源码删除不会停止远端进程。新二进制不能直接覆盖旧冷热部署。切换前必须核对数据与同步完成情况，保留备份，停止旧写入/同步路径并受控建立新缓存，具体迁移步骤待新链路实现后验证。
