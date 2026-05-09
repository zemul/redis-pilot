# Server 实现完整性检查清单

## 📋 API 端点实现状态

### 资源池管理 (4/4 ✅)

- [x] **GET /pool/query** - 查询资源池状态
  - 文件: `pool_handler.go:11-18`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **POST /pool/add** - 注册新服务器
  - 文件: `pool_handler.go:20-45`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **POST /pool/remove** - 移除服务器
  - 文件: `pool_handler.go:47-71`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **POST /pool/update** - 更新服务器信息
  - 文件: `pool_handler.go:73-98`
  - 状态: ✅ 完整
  - 测试: 需要

---

### 实例管理 (6/9 ⚠️)

- [x] **GET /instance/list** - 列出所有实例
  - 文件: `instance_handler.go:12-19`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **GET /instance/status** - 实例详细状态
  - 文件: `instance_handler.go:21-38`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **POST /instance/create** - 创建实例
  - 文件: `instance_handler.go:40-109`
  - 状态: ⚠️ 部分完整
  - 缺失:
    - [ ] 资源分配更新 (pool-state allocated)
    - [ ] Envoy 路由注册
    - [ ] 审计日志
    - [ ] 完整回滚机制
    - [ ] 健康检查验证
  - 测试: 需要

- [x] **POST /instance/delete** - 删除实例
  - 文件: `instance_handler.go:111-161`
  - 状态: ⚠️ 部分完整
  - 缺失:
    - [ ] 资源释放 (pool-state allocated)
    - [ ] Envoy 路由注销
    - [ ] 审计日志
  - 测试: 需要

- [x] **POST /instance/start** - 启动实例
  - 文件: `instance_handler.go:163-165`
  - 状态: ✅ 完整
  - 测试: 需要

- [x] **POST /instance/stop** - 停止实例
  - 文件: `instance_handler.go:167-169`
  - 状态: ✅ 完整
  - 测试: 需要

- [ ] **POST /instance/config** - 更新实例配置
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 接收配置参数
    - [ ] 验证配置有效性
    - [ ] 转发到 Agent
    - [ ] 更新 instances-state
    - [ ] 审计日志
  - 优先级: 🔴 P0

- [ ] **POST /instance/promote** - 从库提升为主库
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 验证实例是从库
    - [ ] 转发到 Agent 执行 REPLICAOF NO ONE
    - [ ] 更新 role 为 master
    - [ ] 更新 replicas 关系
    - [ ] 审计日志
  - 优先级: 🔴 P0

- [ ] **POST /instance/replicate** - 设置复制目标
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 验证目标主库存在
    - [ ] 转发到 Agent 执行 REPLICAOF
    - [ ] 更新 replica_of 字段
    - [ ] 更新主库的 replicas 列表
    - [ ] 审计日志
  - 优先级: 🔴 P0

---

### 备份管理 (0/3 ❌)

- [ ] **POST /backup/exec** - 执行备份
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 确定备份源（优先从库）
    - [ ] 转发到 Agent 执行 BGSAVE
    - [ ] 等待完成
    - [ ] 更新 last_backup 时间戳
    - [ ] 审计日志
  - 优先级: 🟠 P1

- [ ] **POST /backup/restore** - 从备份恢复
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 停止目标实例
    - [ ] 转发到 Agent 恢复备份
    - [ ] 启动实例
    - [ ] 验证数据完整性
    - [ ] 审计日志
  - 优先级: 🟠 P1

- [ ] **GET /backup/list** - 列出可用备份
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 转发到 Agent 查询备份列表
    - [ ] 返回备份文件信息
  - 优先级: 🟠 P1

---

### 代理管理 (0/2 ❌)

- [ ] **POST /envoy/route/update** - 更新 Envoy 路由
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 接收路由配置
    - [ ] 生成 Envoy 配置
    - [ ] 重载 Envoy
    - [ ] 更新 instances-state 中的 envoy 字段
    - [ ] 审计日志
  - 优先级: 🟠 P1

- [ ] **GET /envoy/config** - 查看当前 Envoy 配置
  - 文件: 未实现
  - 状态: ❌ 缺失
  - 需要实现:
    - [ ] 读取当前 Envoy 配置
    - [ ] 返回配置内容
  - 优先级: 🟠 P1

---

## 🔧 核心功能实现状态

### 状态管理 (state.go)

- [x] 读取 pool-state.yaml
- [x] 写入 pool-state.yaml
- [x] 读取 instances-state.yaml
- [x] 写入 instances-state.yaml
- [x] RWMutex 并发保护
- [ ] 实例级操作锁 (ARCHITECTURE.md §3.3.3)
- [ ] 文件锁 (防止进程间冲突)
- [ ] 状态一致性校验
- [ ] 审计日志记录

### Agent 通信 (agent_client.go)

- [x] POST 请求
- [x] GET 请求
- [x] Token 认证
- [x] 错误处理
- [ ] 超时控制
- [ ] 重试机制
- [ ] 连接池

### 资源管理

- [x] 资源池查询
- [x] 服务器注册/移除/更新
- [ ] 资源分配更新 (create/delete 时)
- [ ] 资源调度算法
- [ ] 容量规划

### 实例生命周期

- [x] 创建 (基础)
- [x] 删除 (基础)
- [x] 启动
- [x] 停止
- [ ] 配置更新
- [ ] 主从提升
- [ ] 复制设置
- [ ] 健康检查
- [ ] 故障恢复

### 备份与恢复

- [ ] 备份执行
- [ ] 备份列表
- [ ] 备份恢复
- [ ] 备份轮转
- [ ] 备份验证

### 代理管理

- [ ] Envoy 路由配置
- [ ] 读写分离
- [ ] 路由更新
- [ ] 配置查询

### 审计与安全

- [ ] 审计日志记录
- [ ] 日志格式 (JSONL)
- [ ] 日志轮转 (每日)
- [ ] 日志校验和
- [ ] 日志查询接口
- [ ] 操作锁机制
- [ ] 并发冲突检测

---

## 📊 代码质量检查

### 错误处理

- [x] HTTP 状态码正确
- [x] 错误消息清晰
- [ ] 错误恢复机制
- [ ] 部分失败处理
- [ ] 超时处理

### 并发安全

- [x] 基础 RWMutex
- [ ] 操作锁
- [ ] 文件锁
- [ ] 死锁检测
- [ ] 锁超时

### 性能

- [ ] 连接池
- [ ] 缓存机制
- [ ] 批量操作
- [ ] 异步处理
- [ ] 性能测试

### 可维护性

- [ ] 代码注释
- [ ] API 文档
- [ ] 错误日志
- [ ] 调试日志
- [ ] 单元测试
- [ ] 集成测试

---

## 🚀 实现优先级

### 🔴 P0 - 关键 (影响功能正确性)

1. [ ] 实现 /instance/config
2. [ ] 实现 /instance/promote
3. [ ] 实现 /instance/replicate
4. [ ] 修复资源分配更新 (create/delete)
5. [ ] 实现操作锁机制

### 🟠 P1 - 高 (影响运维体验)

6. [ ] 实现备份管理 API (3 个)
7. [ ] 实现 Envoy 路由管理 (2 个)
8. [ ] 实现审计日志
9. [ ] 添加健康检查

### 🟡 P2 - 中 (改进代码质量)

10. [ ] 添加超时控制
11. [ ] 实现重试机制
12. [ ] 完善错误处理
13. [ ] 添加单元测试

---

## 📝 文件清单

### 已实现

- [x] `cmd/server/main.go` (27 行)
- [x] `internal/server/server.go` (87 行)
- [x] `internal/server/config.go` (38 行)
- [x] `internal/server/pool_handler.go` (99 行)
- [x] `internal/server/instance_handler.go` (220 行)
- [x] `internal/server/agent_client.go` (69 行)
- [x] `internal/state/state.go` (94 行)
- [x] `pkg/apitypes/types.go` (101 行)

### 需要创建

- [ ] `internal/server/backup_handler.go` - 备份 API
- [ ] `internal/server/envoy_handler.go` - Envoy 路由 API
- [ ] `internal/server/audit.go` - 审计日志
- [ ] `internal/server/lock.go` - 操作锁管理
- [ ] `internal/server/health.go` - 健康检查

---

## ✅ 验收标准

### 功能完整性

- [ ] 所有 18 个 API 已实现
- [ ] 所有 API 都有单元测试
- [ ] 所有 API 都有集成测试
- [ ] 所有 API 都有文档

### 代码质量

- [ ] 代码覆盖率 > 80%
- [ ] 无 lint 错误
- [ ] 无 race condition
- [ ] 无内存泄漏

### 性能

- [ ] API 响应时间 < 100ms (不含 Agent 调用)
- [ ] 并发处理能力 > 100 req/s
- [ ] 内存占用 < 100MB

### 可靠性

- [ ] 所有操作都有审计日志
- [ ] 所有操作都支持回滚
- [ ] 所有操作都支持重试
- [ ] 所有操作都有超时控制

---

## 📅 实施计划

### Week 1 - P0 实现

- [ ] Day 1-2: 实现 /instance/config, promote, replicate
- [ ] Day 3-4: 修复资源分配更新
- [ ] Day 5: 实现操作锁机制

### Week 2 - P1 实现

- [ ] Day 1-2: 实现备份管理 API
- [ ] Day 3-4: 实现 Envoy 路由管理
- [ ] Day 5: 实现审计日志

### Week 3 - 测试与优化

- [ ] Day 1-2: 单元测试
- [ ] Day 3-4: 集成测试
- [ ] Day 5: 性能测试与优化

---

## 🔍 检查清单

在提交前检查:

- [ ] 所有 API 都已注册到路由
- [ ] 所有 API 都有错误处理
- [ ] 所有 API 都有审计日志
- [ ] 所有 API 都有单元测试
- [ ] 所有 API 都有文档
- [ ] 代码通过 lint 检查
- [ ] 代码通过 race detector
- [ ] 代码通过集成测试
- [ ] 性能测试通过
- [ ] 文档已更新

