---
name: redis-envoy
description: 管理 Envoy 代理路由
---

# redis-envoy

查看和更新 Envoy 路由配置。

## 流程

1. 查看当前配置
   ```
   redis-pilot-cli envoy config
   ```

2. 更新路由
   ```
   redis-pilot-cli envoy route-update --group <group> --master <addr> --replica <addr>
   ```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| group | 是 | 实例组名 |
| master | 否 | 主库地址 |
| replica | 否 | 从库地址（读负载均衡） |

## 注意事项

- 路由更新后 Envoy 热重载，无需重启
- 故障转移时路由自动更新
