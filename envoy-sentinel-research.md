# Envoy redis_proxy with Sentinel-Managed Master-Slave Deployments

## Executive Summary

**Envoy does NOT natively support Redis Sentinel.** There is no built-in mechanism for Envoy to query Sentinel, track master role changes, or automatically follow failover events in a Sentinel-managed deployment. This is a fundamental architectural gap. However, there are practical workarounds using Envoy's health checking and TCP health check capabilities.

---

## Answer to Question 1: Do you configure just the master, or all nodes?

**In non-Cluster (standalone/Sentinel) mode, you should configure ALL nodes (master + replicas) in the upstream cluster.** Here's why:

- In non-Cluster mode, Envoy's redis_proxy uses **ring hash load balancing** against the upstream cluster. It treats all endpoints in the cluster as equivalent and routes commands based on key hashing.
- If you configure only the master, you lose all load balancing and health checking benefits — and you still have the failover problem (see Q2).
- The recommended approach from the Envoy architecture docs is: *"The corresponding cluster definition should be configured with ring hash load balancing"* — this implies multiple hosts in the cluster.
- **However**, there's a critical issue: in a master-slave setup without Envoy's Redis Cluster mode, Envoy does NOT distinguish between master and replica roles. It will route write commands (SET, etc.) to ANY healthy host in the cluster, including replicas — which will fail because replicas are read-only.

**The practical solution** is to use separate clusters for reads and writes (see Q4 workaround below).

---

## Answer to Question 2: How does Envoy detect which node is the current master after Sentinel failover?

**Envoy does NOT detect master role changes in non-Cluster mode.** This is the core problem.

- **Redis Cluster mode**: Envoy tracks topology by periodically sending `CLUSTER SLOTS` commands to a random node. It learns which nodes are primaries and which are replicas. When a failover happens, the topology update eventually propagates to Envoy.

- **Non-Cluster/Sentinel mode**: There is NO equivalent mechanism. Envoy has no way to:
  - Query Sentinel for the current master
  - Send `INFO replication` to detect role changes
  - Automatically update its routing after a Sentinel-initiated failover

This was explicitly raised in [GitHub issue #35727](https://github.com/envoyproxy/envoy/issues/35727): *"Does Envoy Redis proxy support Sentinel deployment?"* — The answer from maintainers was effectively "contributions welcome," and the issue was closed as stale with no implementation.

---

## Answer to Question 3: Can Envoy's health check detect Redis node role via INFO replication?

**No, the built-in Redis health checker cannot detect node role.** The Redis health checker (proto: `envoy.extensions.health_checkers.redis.v3.Redis`) only supports:

1. **PING** — sends a Redis PING command and checks for PONG response
2. **EXISTS key** — optionally checks if a specific key exists (for maintenance mode)

That's it. There is no mechanism to:
- Parse `INFO replication` output
- Check `ROLE` command response
- Distinguish master from replica

The proto definition only has two fields:
```protobuf
message Redis {
  string key = 1;       // Optional: EXISTS <key> instead of PING
  AwsIam aws_iam = 2;   // AWS IAM auth for health check
}
```

**However**, there IS a creative workaround using **TCP health checks** with custom send/receive payloads, as demonstrated by a community member in [GitHub issue #39630](https://github.com/envoyproxy/envoy/issues/39630):

```yaml
health_checks:
  - timeout: 1s
    interval: 1s
    unhealthy_threshold: 2
    healthy_threshold: 1
    reuse_connection: false
    tcp_health_check:
      send:
        # Hex-encoded "ROLE\r\n" command
        text: '0d0a524f4c450d0a'
      receive:
        # Hex-encoded "*3" — only the master returns a 3-element array
        # (master returns: *3\r\n$6\r\nmaster\r\n...)
        # Replica returns: *5\r\n$6\r\nslave\r\n... 
        - text: '2a33'
```

This cleverly uses the Redis ROLE command via raw TCP health check:
- **Master** responds with `*3` (3-element array: role, offset, list of replicas)
- **Replica** responds with `*5` (5-element array: role, state, master info, offset, delay)

By matching `*3` in the receive field, only the master passes the health check. Replicas are marked unhealthy and excluded from the load balancing pool for write operations.

---

## Answer to Question 4: Is there any built-in mechanism for handling master role changes in non-Cluster mode?

**No.** There is no built-in mechanism. The planned features list in the architecture docs explicitly lists "Replication" and "Built-in retry" as future enhancements — neither has been implemented.

Specific gaps:
- No Sentinel protocol support (the `SENTINEL` command is rejected as "unsupported command")
- No automatic topology refresh in non-Cluster mode
- No role-aware routing in non-Cluster mode
- No automatic failover detection
- The `ReadPolicy` settings (MASTER, PREFER_MASTER, REPLICA, etc.) only work with **Redis Cluster** mode

---

## Community Workarounds and Solutions

### Solution 1: Separate Read/Write Clusters with TCP ROLE Health Check (Recommended)

From [GitHub issue #39630](https://github.com/envoyproxy/envoy/issues/39630), the most practical approach:

```yaml
static_resources:
  listeners:
  - name: redis
    address:
      socket_address: { address: 0.0.0.0, port_value: 6379 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.redis_proxy
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
          stat_prefix: redis
          settings:
            op_timeout: 10s
          prefix_routes:
            catch_all_route:
              cluster: rw          # Write commands go here
              read_command_policy:
                cluster: ro        # Read commands go here

  clusters:
  # Read-only cluster: all replicas (round-robin)
  - name: ro
    type: STRICT_DNS
    lb_policy: ROUND_ROBIN
    health_checks:
    - timeout: 1s
      interval: 1s
      unhealthy_threshold: 2
      healthy_threshold: 1
      custom_health_check:
        name: envoy.health_checkers.redis
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.health_checkers.redis.v3.Redis
    load_assignment:
      cluster_name: ro
      endpoints:
      - priority: 0
        lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: replica-1, port_value: 6379 }
        - endpoint:
            address:
              socket_address: { address: replica-2, port_value: 6379 }
      - priority: 1
        lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: master, port_value: 6379 }

  # Write cluster: only master passes health check
  - name: rw
    type: STRICT_DNS
    lb_policy: ROUND_ROBIN
    common_lb_config:
      healthy_panic_threshold:
        value: 0    # Allow routing to 0 healthy hosts rather than panic
    health_checks:
    - timeout: 1s
      interval: 1s
      no_traffic_interval: 1s
      no_traffic_healthy_interval: 1s
      unhealthy_interval: 1s
      unhealthy_threshold: 2
      healthy_threshold: 1
      reuse_connection: false
      tcp_health_check:
        send:
          text: '0d0a524f4c450d0a'  # ROLE\r\n
        receive:
        - text: '2a33'              # *3 (master only)
    load_assignment:
      cluster_name: rw
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: master, port_value: 6379 }
        - endpoint:
            address:
              socket_address: { address: replica-1, port_value: 6379 }
        - endpoint:
            address:
              socket_address: { address: replica-2, port_value: 6379 }
```

**How this works:**
1. The `rw` cluster includes ALL nodes but uses a TCP health check that sends `ROLE\r\n` and expects `*3` (master response). Only the current master passes → all writes go to master.
2. The `ro` cluster uses standard Redis PING health checks. Replicas are healthy → reads go to replicas. The master is in priority 1 as fallback.
3. When Sentinel performs a failover: the old master fails the ROLE health check (it now returns `*5` as a replica), and the new master passes it. Writes automatically redirect to the new master within the health check interval.
4. The `read_command_policy` in `catch_all_route` splits read commands to the `ro` cluster.

**Limitations:**
- There's a brief window of write failures during failover (between the actual failover and the next health check cycle)
- The `reuse_connection: false` is important — without it, Envoy may reuse a connection that was established when the node was a master
- The `healthy_panic_threshold: 0` is critical — without it, Envoy would panic and route to all hosts when no host passes the health check during transition

### Solution 2: Use Redis Cluster Instead of Sentinel

If you can choose your deployment model, **Redis Cluster** is much better supported by Envoy:

```yaml
clusters:
- name: redis_cluster
  connect_timeout: 0.25s
  lb_policy: CLUSTER_PROVIDED
  cluster_type:
    name: envoy.clusters.redis
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.clusters.redis.v3.RedisClusterConfig
      cluster_refresh_rate: 5s
      cluster_refresh_timeout: 3s
      redirect_refresh_interval: 5s
      redirect_refresh_threshold: 10
  load_assignment:
    cluster_name: redis_cluster
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address: { address: redis-node-1, port_value: 6379 }
      - endpoint:
          address:
            socket_address: { address: redis-node-2, port_value: 6379 }
      - endpoint:
          address:
            socket_address: { address: redis-node-3, port_value: 6379 }
```

With Redis Cluster, Envoy:
- Periodically sends `CLUSTER SLOTS` to discover topology
- Knows which nodes are primaries and which are replicas
- Supports `ReadPolicy` for read/write splitting (MASTER, PREFER_MASTER, REPLICA, etc.)
- Handles MOVED/ASK redirections
- Automatically detects failovers

### Solution 3: External Sentinel Watcher + xDS Dynamic Updates

For production Sentinel deployments where Redis Cluster is not an option:

1. Run a sidecar process that monitors Sentinel (using `SENTINEL GET-MASTER-ADDR-BY-NAME`)
2. When a failover is detected, update Envoy's cluster configuration via xDS (control plane)
3. Update the `rw` cluster endpoints to point to the new master IP

This is the most robust approach but requires a custom control plane or integration with an existing one (like Istio/Gloo).

---

## Summary Table

| Capability | Non-Cluster (Standalone) | Non-Cluster (Sentinel) | Redis Cluster |
|---|---|---|---|
| Configure all nodes | ✅ Yes | ✅ Yes | ✅ Yes |
| Auto-detect master role | ❌ No | ❌ No | ✅ Via CLUSTER SLOTS |
| Follow Sentinel failover | ❌ N/A | ❌ No | ❌ N/A (uses own failover) |
| Redis HC detects role | ❌ PING only | ❌ PING only | ✅ Via topology |
| TCP HC ROLE workaround | ✅ Possible | ✅ Possible | N/A (not needed) |
| ReadPolicy support | ❌ No | ❌ No | ✅ Yes |
| read_command_policy | ✅ Yes (prefix routes) | ✅ Yes (prefix routes) | ✅ Yes |
| SENTINEL command passthrough | ❌ Unsupported | ❌ Unsupported | ❌ N/A |

---

## Key GitHub References

1. **[#35727](https://github.com/envoyproxy/envoy/issues/35727)** — "Does Envoy Redis proxy support Sentinel deployment?" — Confirmed: SENTINEL command returns "unsupported command", issue closed as stale with no implementation.
2. **[#39630](https://github.com/envoyproxy/envoy/issues/39630)** — "Support ROLE command in redis proxy" — Contains the TCP ROLE health check workaround with full config example.
3. **[#21352](https://github.com/envoyproxy/envoy/issues/21352)** — "Envoy requests are failing when Redis replica fails" — Discusses health checking and the importance of `lb_policy: CLUSTER_PROVIDED` for Redis Cluster mode.
4. **[#36962](https://github.com/envoyproxy/envoy/issues/36962)** — "A 2-step Plan for Improving Envoy Redis Proxy Reliability" — Proposal for async failover and cross-cluster failover, still open/unimplemented.
