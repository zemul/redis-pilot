# 冷热组件部署与运维

现行运行入口，更新：2026-09-08。架构与支持边界见 [现行设计](hot-cold-gateway-design.md)，能力状态见 [实现进度](hot-cold-status.md)。以下安装步骤、模板与历史部署记录不等于当前线上版本；本轮仅整理文档，没有部署或核实远端状态。

Gateway 是独立数据面进程，与 Proxy 一样由运维部署二进制、由 systemd 管理生命周期。
Agent 仅管理本机 Redis/Kvrocks 容器，不安装、启停或采集 Gateway 专属状态。
Server 保留配置草稿；Gateway 登记及健康上报的最小闭环已有代码，实际部署版本须另查，配置拉取和已应用版本确认尚未实现。
当前服务仍为单后端透传原型，没有冷热协调能力。

## 构建与安装

本机 `make gateway`，Linux amd64 开发机使用 `make gateway-linux`，产物为 `bin/redis-gateway`。
独立目标不属于 `make all`、`make deploy`；默认构建目标仍为 `all`。
当前只提供手动部署步骤，不自动选择主机或覆盖现有服务。

首次部署前，操作人员需要：

1. 确认使用隔离测试 Redis，数据完整，不能是只有部分 key 的热层。
2. 检查监听端口空闲、主机资源充足。当前只能回环监听，与测试 Proxy 同机；跨机入口支持待定。
3. 创建无登录权限的专用系统账号 `redis-gateway`，准备 `/opt/redis-gateway` 和 `/etc/redis-gateway`。二进制由 root 持有，服务账号仅可读取/执行。
4. 将构建产物安装到 `/opt/redis-gateway/redis-gateway`，权限 0755。将 [配置示例](../examples/gateway-passthrough.yaml) 复制为 `/etc/redis-gateway/gateway.yaml` 并调整后端、端口和容量预算。
5. 设置 `allow_unauthenticated: false`，配置独立的 `frontend_password_file`；后端有密码时设置 `backend_password_file`。凭据文件使用绝对路径，服务账号持有、权限 0600；配置目录 root:redis-gateway、0750，配置文件 root:redis-gateway、0640。不要把真实凭据加入仓库或命令行。
6. 以服务账号运行 `redis-gateway -config /etc/redis-gateway/gateway.yaml -check-config`，确认本地配置和凭据可读。此检查不代表后端连通、角色正确或业务就绪。
7. 安装 [systemd 模板](../examples/redis-gateway.service) 到 `/etc/systemd/system/redis-gateway.service`，执行 `systemctl daemon-reload`、`systemctl start redis-gateway`。确认保留的部署验收后执行 `systemctl enable redis-gateway`，满足开机启动要求；一次性测试实例不设置自启动。

示例 buffer_budget 是应用层保守预留预算，不是实际 RSS 上限。应按测试连接数、最大命令和机器余量调整；本模板不施加压测用的 CPU 配额。生产资源预算与 systemd 内存限制待容量测试后确定。

## 检查、升级与停止

- 使用 `systemctl status redis-gateway`、`journalctl -u redis-gateway` 检查进程与日志；通过配置中的管理地址检查 `/readyz` 和 `/metrics`。进程 active 不等于后端 ready。
- 用独立测试客户端验证认证、SET/GET，以及明确的错误返回；不修改现有 Proxy 快照。
- 停止使用 `systemctl stop redis-gateway`：SIGTERM 触发网关排空，systemd 超时必须大于配置中的 `shutdown_timeout`。
- 升级前保留旧二进制与配置，检查新配置，停止服务，再替换二进制并启动、验收；失败恢复旧版本。当前不提供滚动升级、自动切流或无损重试保证，客户端遇到不确定写入不能盲目重试。
- Server 草稿创建、修改、删除均不会启停该服务，草稿 revision 也不是进程已应用版本。

G5c 由 Gateway 直接与 Server 通信，配置与边界见 [登记与心跳](archive/hot-cold/gateway-heartbeat.md)；不通过 Agent 转发，配置应用确认仍待后续实现。
多网关归属、分片 fencing、高可用与切流仍待实现，不能把复制 systemd 服务理解为完成高可用。

## 独立同步器：当前可运行范围

- 通过 `make syncer` / `make syncer-linux` 构建 `bin/redis-syncer`；独立二进制，不由 Gateway 或 Agent 启动，不加入普通部署目标。
- 普通隔离任务使用 `redis-syncer -config /绝对路径/syncer.yaml`，先用同一参数加 `-check-config` 检查配置与凭据。参考 [单任务示例](../examples/syncer-isolated.yaml) 与 [多任务示例](../examples/syncer-multitask.yaml)；检查成功不证明后端/持久状态可用。
- 源/冷目标需符合当前 loopback 隔离限制，每任务独占冷 DB，日志目录与身份固定。不得通过清空目录、删除冷元数据或更换 task_id 绕过恢复检查。
- 使用独立账号与 [systemd 模板](../examples/redis-syncer.service)，安装前核对路径、权限、日志磁盘预算与停止超时；保留部署验收后开启自启动。模板当前 `Restart=no`，仅开机启动不等于故障自愈；自动恢复策略仍待完成，不能直接宣称自动接管。
- `/status`、`/livez`、`/metrics` 用于观测；可选 Server 心跳不下发任务、不授权写入，也不改变任务归属。增减静态任务目前需要受控重启。
- `-confirmation-config` 是另一个显式实验入口，与普通配置互斥，固定单任务/单未完成操作；live 实验可持续捕获，但确认边界尚无自动收尾。不能把普通同步器部署步骤当作完整冷热服务启动方法。

内部准备接口没有公开 Gateway YAML 开关。当前不要接入只有部分数据的热层，不进行业务切流，不启动自动淘汰。远端部署位置、自动拉起策略、资源预算与生产配置验收需在功能闭环后确认。

配置字段和早期实验细节保留在 [同步器阶段归档](archive/hot-cold/syncer-service.md) 与 [静态多任务归档](archive/hot-cold/syncer-multitask.md)，现状优先以实现进度为准。

## 开发机部署记录（2026-09-07）

代码版本 `efbf187` 已在 redis01 独立部署，未部署 Server/Agent 的新版本，未修改 Proxy 路由。

| 项目 | 实际值 |
| --- | --- |
| 服务 | `redis-gateway.service`，专用无登录账号 `redis-gateway` |
| 二进制 | `/opt/redis-gateway/redis-gateway` |
| 配置 | `/etc/redis-gateway/gateway.yaml`，来源为 [小预算部署示例](../examples/gateway-systemd.yaml) |
| 入口 / 管理 | `127.0.0.1:17379` / `127.0.0.1:19200` |
| 凭据 | 随机生成，保存在 `/etc/redis-gateway/frontend.secret`，0600；不入库、不入仓库 |
| 隔离后端 | `redis-gateway-dev-backend`，Redis 7.4.9，仅映射 `127.0.0.1:16389` |
| 后端资源 | 容器内存 128MiB，Redis maxmemory 64MiB，noeviction；禁用 AOF/RDB，不存业务数据 |
| 自启动 | Gateway 和 `redis-gateway-dev-backend.service` 均已 enable、active；通过开发专用 drop-in 声明后端启动顺序 |

二进制 SHA256：`3ca12802520cfb49f5c3ef9ee67d10d5e8ec3fe4b1534e736e6f8feb311ae8f8`，本地与远端一致。
已验证 `/readyz`、前端认证、SET/INCR/GET/DEL（41→42），未认证 GET 返回 NOAUTH；测试 key 已删除。
另已验证 Gateway 停止后重新启动、`/metrics` 可访问、仅回环监听和凭据权限；后端 DBSIZE 为 0。现有 Server/Agent/Proxy 保持 active，未重启。
systemd 单元校验通过；系统同时提示既有 cloudmonitor 单元的告警，该服务未改动。
首次 start 返回后立即请求健康端口曾遇到连接拒绝，随后服务完成启动并返回 ok；不能把 systemctl start 成功等同于就绪。

该部署只用于功能验证，不是生产发布，也没有开启冷热同步、回温、淘汰或 Server 状态上报。

### 开机自启动补充

开发后端使用 [独立 systemd 单元](../examples/redis-gateway-dev-backend.service) 管理已有容器，不重建、不删除容器。
[开发依赖 drop-in](../examples/redis-gateway-dev-backend.conf) 安装至 `/etc/systemd/system/redis-gateway.service.d/dev-backend.conf`，仅适用于此隔离部署，不属于通用 Gateway 后端绑定。
两服务已执行 `systemctl enable --now`；停止后端/网关后重新启动验证通过，健康检查在后端就绪前短暂返回 503，随后返回 ok。
启动顺序不等于 Redis 就绪承诺，调用方仍需检查 `/readyz`。未实际重启开发机验证，未影响现有 Server/Agent/Proxy。
测试 Redis 仍禁用持久化：开机自动启动不代表重启保留测试数据。
