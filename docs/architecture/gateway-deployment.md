# Gateway 独立二进制部署

Gateway 是独立数据面进程，与 Proxy 一样由运维部署二进制、由 systemd 管理生命周期。
Agent 仅管理本机 Redis/Kvrocks 容器，不安装、启停或采集 Gateway 专属状态。
Server 保留配置草稿；Gateway 注册、配置拉取、已应用版本和健康上报尚未实现。
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
7. 安装 [systemd 模板](../examples/redis-gateway.service) 到 `/etc/systemd/system/redis-gateway.service`，执行 `systemctl daemon-reload`、`systemctl start redis-gateway`。验收后再决定是否 `systemctl enable redis-gateway`。

示例 buffer_budget 是应用层保守预留预算，不是实际 RSS 上限。应按测试连接数、最大命令和机器余量调整；本模板不施加压测用的 CPU 配额。生产资源预算与 systemd 内存限制待容量测试后确定。

## 检查、升级与停止

- 使用 `systemctl status redis-gateway`、`journalctl -u redis-gateway` 检查进程与日志；通过配置中的管理地址检查 `/readyz` 和 `/metrics`。进程 active 不等于后端 ready。
- 用独立测试客户端验证认证、SET/GET，以及明确的错误返回；不修改现有 Proxy 快照。
- 停止使用 `systemctl stop redis-gateway`：SIGTERM 触发网关排空，systemd 超时必须大于配置中的 `shutdown_timeout`。
- 升级前保留旧二进制与配置，检查新配置，停止服务，再替换二进制并启动、验收；失败恢复旧版本。当前不提供滚动升级、自动切流或无损重试保证，客户端遇到不确定写入不能盲目重试。
- Server 草稿创建、修改、删除均不会启停该服务，草稿 revision 也不是进程已应用版本。

后续 G5c 由 Gateway 直接与 Server 建立控制面通信，具体注册、认证、心跳、配置应用确认协议待定；不通过 Agent 转发。
多网关归属、分片 fencing、高可用与切流仍待实现，不能把复制 systemd 服务理解为完成高可用。

本次仅添加模板和构建入口，没有在 redis01～03 安装或启动服务。
