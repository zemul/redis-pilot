# Envoy + Sentinel + Redis Architecture: Pitfalls & Edge Cases Analysis

## Executive Summary

This architecture uses an **indirect coordination** pattern: Envoy detects the master via ROLE TCP health checks, while Sentinel independently manages failover. The gap between these two systems creates multiple failure modes. Below is a detailed analysis of each area with concrete evidence from source code, GitHub issues, and documentation.

---

## 1. Race Conditions During Failover

### The Core Problem
There is an **unavoidable timing gap** between Sentinel starting a failover and Envoy detecting the new master. The sequence is:

1. Old master fails → Sentinel detects SDOWN → promotes to ODOWN → leader election
2. Sentinel sends `SLAVEOF NO ONE` to the promoted slave
3. Promoted slave transitions: `server.masterhost` is set to NULL
4. Envoy's next ROLE health check sees `*3` (master) on the new node
5. Envoy marks old master unhealthy (ROLE returns `*5` or connection fails)
6. Envoy routes writes to the new master

**During steps 1-4, writes can be routed to the old master (if it's still accepting connections) or fail entirely.**

### Evidence from Sentinel Source Code
From `sentinel.c` (Redis 7.2), the failover state machine has these states:
```
SENTINEL_FAILOVER_STATE_WAIT_START     1  // Leader election
SENTINEL_FAILOVER_STATE_SELECT_SLAVE   2  // Select best slave
SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3  // Send SLAVEOF NO ONE
SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4  // Wait for slave to become master
SENTINEL_FAILOVER_STATE_RECONF_SLAVES  5  // Reconfigure other slaves
```

The default `failover_timeout` is **180 seconds** (3 minutes). The `WAIT_PROMOTION` state waits for the promoted slave's INFO command to show it's a master — this is an **indirect check**, not atomic.

### Concrete Risk: Writes to Demoted Master
- If the old master is still reachable (network partition, not a crash), it still accepts writes
- Envoy's ROLE health check interval is 1s, but `unhealthy_threshold: 2` means it takes **2 failed checks** to mark unhealthy = **~2-3 seconds minimum**
- During this window, writes go to the old master which is now a slave → **writes are accepted but will be overwritten by replication from the new master** (data loss)
- `healthy_panic_threshold: 0` helps here — it means Envoy will route to *no* hosts rather than all hosts when all are unhealthy, but it doesn't prevent routing to a node that still passes ROLE=*3

### Mitigation
- Set `unhealthy_threshold: 1` (not 2) to reduce the detection window
- Use `min_health_check_interval` to ensure faster re-checks during state changes
- Consider `reuse_connection: false` on the rw cluster (already configured) to avoid stale connection state
- **Critical**: The old master must be configured with `min-replicas-to-write 1` so it rejects writes when it loses its replicas

---

## 2. Split-Brain Scenarios

### Scenario A: Network Partition, Sentinel Elects New Master, Envoy Sees Old Master as Healthy

This is the **most dangerous scenario** for this architecture.

**Timeline:**
1. Network partition isolates the old master from Sentinels but not from Envoy
2. Sentinels on the majority side elect a new master
3. Envoy's ROLE health check to the old master **still returns `*3`** (it's still a master from its own perspective)
4. Envoy continues routing writes to the old master
5. **Two masters accepting writes simultaneously** → irreconcilable data divergence

**Evidence:** Redis issue #926 (redis/redis) documents that Sentinel does not always properly detect when a master becomes a slave. The issue shows that when a master receives `SLAVEOF` between Sentinel pings, timing-dependent behavior can cause inconsistent views.

**Why `healthy_panic_threshold: 0` doesn't help:** This setting only affects behavior when *all* hosts are unhealthy. If the old master is still reachable and returning ROLE=*3, it's still "healthy" from Envoy's perspective.

### Scenario B: Two Sentinels on Different Sides Elect Different Masters

Sentinel requires a **quorum** for failover. If you have 3 Sentinels and a network partition splits them 2:1:
- The 2-Sentinel side can achieve quorum and elect a new master
- The 1-Sentinel side cannot achieve quorum → no split-brain from Sentinel itself
- **But**: If you have only 2 Sentinels (quorum=1), both sides can elect different masters

**Mitigation:**
- Always use **≥3 Sentinels** with `quorum ≥ 2`
- Configure `min-replicas-to-write 1` on all Redis nodes (this is the **most important** mitigation — it forces the old master to stop accepting writes when it loses slave connections)
- Use `sentinel down-after-milliseconds` aggressively (e.g., 5000ms) to reduce the split-brain window
- Monitor for `+tilt` mode on Sentinels (indicates clock issues that can cause incorrect failovers)

---

## 3. ROLE Health Check Reliability

### ROLE Command Response Format

From Redis source code (`replication.c`, Redis 7.x):

**Master response:** Array of 3 elements (`*3`):
```
*3
$6
master
:<repl_offset>
*<num_slaves>
  [<slave_ip>, <slave_port>, <slave_repl_offset>...]
```

**Slave response:** Array of 5 elements (`*5`):
```
*5
$5
slave
$<master_host_len>
<master_host>
:<master_port>
$<state_len>
<state>
:<repl_offset>
```

**Sentinel response:** Array of 2 elements (`*2`):
```
*2
$8
sentinel
*<num_masters>
  [<master_name>...]
```

### Critical Edge Cases

#### 3a. ROLE Returns `*2` for Sentinel Nodes
If a Sentinel process is accidentally running on the same host/port as a Redis data node (misconfiguration), ROLE returns `*2` (sentinel), not `*3` or `*5`. The TCP health check matching `*3` would correctly reject this, but it's worth monitoring for.

#### 3b. The Transition Window — Is There a Brief Unexpected Format?
**This is the most important finding for this area.** Looking at the Redis source code:

```c
void roleCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelRoleCommand(c);
        return;
    }
    if (server.masterhost == NULL) {
        addReplyArrayLen(c,3);  // Master: *3
        ...
    } else {
        addReplyArrayLen(c,5);  // Slave: *5
        ...
    }
}
```

The ROLE command checks `server.masterhost` — this is set to NULL **atomically** when `SLAVEOF NO ONE` is processed. There is **no intermediate state** where ROLE returns something unexpected. The transition from `*5` to `*3` is instantaneous from the ROLE command's perspective.

**However**, there IS a subtle issue: during the `REPL_STATE_TRANSFER` state (full resync), the node reports itself as a slave with state "sync". After `SLAVEOF NO ONE`, the node immediately reports as master, **even before it has finished loading data from the RDB transfer if one was in progress**. This means:
- ROLE returns `*3` (master) immediately
- But the node might still be loading data → `LOADING` errors on commands
- Envoy would route writes to a "master" that returns LOADING errors

#### 3c. Version Stability
ROLE has been available since Redis 2.8.12. The response format (`*3` for master, `*5` for slave) has been **stable across all versions**. The `*2` sentinel format was added in Redis 2.8.12 as well. No version-related format changes are known.

#### 3d. The `*3` Match Is Too Loose
The TCP health check matches `*3` as a hex string (`2a33`). This matches **any** RESP array of length 3. While ROLE for a master always returns `*3`, other Redis commands can also return arrays of length 3. If the health check connection gets out of sync (e.g., partial response from a previous check), the `*3` could match a different response. With `reuse_connection: false`, this risk is minimized.

---

## 4. Connection Handling After Failover

### reuse_connection=false Analysis

The `reuse_connection: false` setting on the rw cluster means Envoy creates a **new TCP connection for each health check** and closes it after the response. This is correct for the health check itself.

**But this does NOT affect data connections.** Envoy's redis_proxy filter maintains its own connection pool (`conn_pool_impl.cc`) that is separate from health check connections.

### The Real Danger: Stale Data Connections

After failover:
1. Envoy has an existing data connection to the old master
2. The old master is now a slave (or disconnected)
3. Envoy's health check detects the change (ROLE returns `*5` or connection fails)
4. Envoy marks the old host as unhealthy
5. **Existing connections in the connection pool are NOT immediately closed**

From the Envoy source code (`conn_pool_impl.cc`), when a host is marked unhealthy:
- The connection is moved to `clients_to_drain_`
- A drain timer fires every **1 second** to check if the client is still active
- The connection is only closed when `!redis_client_->active()` (no pending requests)

**This means:** If there are in-flight requests on the old connection, they complete on the old (now-slave) node. For read commands, this returns stale data. For write commands, the write goes to the wrong node.

### Pipelining Concerns

Envoy's redis_proxy supports pipelining (multiple commands on the same connection without waiting for responses). If a pipeline of commands is in-flight when failover occurs:
- All commands in the pipeline complete against the old node
- The client sees successful responses, but writes may be lost (overwritten by replication from new master)

### Mitigation
- `reuse_connection: false` on health checks is necessary but not sufficient
- Set `op_timeout` to a low value (e.g., 1s) to fail fast on stale connections
- Consider implementing client-side retry logic with idempotency keys
- Monitor `upstream_cx_drained` metric to track connection drain events

---

## 5. Envoy's redis_proxy Filter Limitations

### 5a. ROLE Command as Custom Command — Broken Behavior
**GitHub Issue #39630** (envoyproxy/envoy): When `role` is added to `custom_commands`, it's treated as a "simple command" and routed to the write cluster (rw). But the response is `invalid request` because:

From the source code, `ROLE` is already in `ClusterScopeCommands` (sent to all shards in cluster mode). When added as a `custom_command`, it's registered as a `simple_command_handler` which expects a single key to hash on. ROLE has no key argument, causing the "invalid request" error.

**Impact:** Clients that send ROLE to check they're connected to the master will get errors. This is a known, unfixed issue.

### 5b. RESP3 Not Supported
**GitHub Issue #44256** (envoyproxy/envoy): The redis_proxy filter only speaks RESP2. If a client sends `HELLO 3`, Envoy responds with `NOPROTO`. This means:
- Modern Redis clients defaulting to RESP3 will fail
- Client-side caching (RESP3 feature) is unavailable
- Pub/sub via Push frames (RESP3) is unavailable

### 5c. Transaction Connection Churn
**GitHub Issue #44338** (envoyproxy/envoy): Every MULTI/EXEC transaction creates and destroys a dedicated TCP connection. At high transaction QPS, this causes:
- 291x increase in new connections
- Redis CPU spike from 1% to 99% purely from TCP overhead
- Effectively makes the proxy unusable for transaction-heavy workloads

### 5d. SELECT/Database Routing Bug
**GitHub Issue #41659** (envoyproxy/envoy): The redis_proxy does not track the currently selected database per downstream connection. Connection pooling means a new client can be routed to a connection left on db 3, causing reads/writes to the wrong database.

### 5e. MULTI/EXEC Crash Bugs
**GitHub Issues #26342, #23651, #37825** (envoyproxy/envoy):
- MULTI/EXEC with mirroring causes segfault (#26342)
- MULTI/EXEC with Redis Cluster causes segfault (#23651)
- UNWATCH causes unexpected behavior on release builds due to memory ordering (#37825)

### 5f. SCAN Returns Incomplete Results
**GitHub Issue #38516** (envoyproxy/envoy): SCAN through the proxy returns fewer keys than expected because the proxy doesn't properly aggregate SCAN cursors across shards.

### 5g. Latency Snowball Effect
**GitHub Issue #36962** (envoyproxy/envoy): When one Redis node fails, all requests (even to healthy nodes) are blocked because Envoy must return responses in order. A single failed shard can snowball into high latency for all requests.

---

## 6. Sentinel + Envoy Timing Issues: Data Not Fully Synced

### The "Premature Master" Problem

When Sentinel sends `SLAVEOF NO ONE` to a slave:
1. The slave immediately sets `server.masterhost = NULL`
2. ROLE command now returns `*3` (master)
3. Envoy's health check sees `*3` → marks the node as healthy master
4. **But the node may not have fully synced data from the old master**

The replication states before promotion:
- `REPL_STATE_CONNECTED` — fully synced, ready
- `REPL_STATE_TRANSFER` — still receiving RDB from master

Sentinel's `sentinelFailoverWaitPromotion` only checks for timeout (default 180s). It waits for the INFO command to show the slave has become a master, but **does not verify data completeness**.

**Concrete risk:** If a slave was partially synced when promoted, Envoy will route writes to it immediately after ROLE returns `*3`, but some data from the old master may be missing.

### Mitigation
- Configure `min-replicas-to-write` and `min-replicas-max-lag` on Redis nodes
- Use Sentinel's `failover_timeout` to allow sufficient time for sync
- Monitor replication lag (`INFO replication`) and alert on high lag before failover

---

## 7. Read-After-Write Consistency

### How read_command_policy Works

From the Envoy source code (`router_impl.cc`):
```cpp
ConnPool::InstanceSharedPtr Prefix::upstream(const std::string& command) const {
  if (read_upstream_) {
    std::string to_lower_string = absl::AsciiStrToLower(command);
    if (Common::Redis::SupportedCommands::isReadCommand(to_lower_string)) {
      return read_upstream_;
    }
  }
  return upstream_;
}
```

`isReadCommand` returns `true` for **any command not in the writeCommands set**. This means ROLE, PING, INFO, CONFIG, etc. are all classified as "read commands" and routed to the `ro` cluster.

### The Stale Read Problem

With the ro cluster configured as:
- Replicas at priority 0 (preferred)
- Master at priority 1 (fallback)
- PING health check

**After a write to the master, a read from a replica can return stale data** because:
1. Redis replication is **asynchronous** — the master doesn't wait for replicas to acknowledge writes
2. The replication lag can be milliseconds to seconds depending on network and load
3. Envoy has **no mechanism** to wait for replication before routing reads

**This is explicitly documented** in the Envoy proto:
> "All ReadPolicy settings except MASTER may return stale data because replication is asynchronous and requires some delay. You need to ensure that your application can tolerate stale data."

### What read_command_policy Does NOT Do
- It does NOT wait for replication
- It does NOT check replica lag
- It does NOT provide causal consistency
- It does NOT route reads to the master for recently-written keys

### Mitigation
- For read-after-write consistency, route reads through the rw cluster (master only)
- Use `WAIT` command after critical writes to ensure replication (but this is not supported through Envoy's proxy — see limitation 5a)
- Consider application-level versioning/timestamps to detect stale reads

---

## 8. Multiple Master-Slave Groups Under One Sentinel

### Architecture Concern

If one Sentinel deployment manages multiple independent master-slave clusters:

**Resource contention:**
- Each Sentinel process monitors all masters in a single event loop
- During a failover for one master, Sentinel's CPU and network are consumed by the failover process
- If multiple masters fail simultaneously (e.g., host failure), failovers are processed **sequentially**, not in parallel
- Each failover takes 10-30 seconds minimum, so 5 simultaneous failures could take 50-150 seconds

**Interference scenarios:**
- A failover for master A could delay detection of master B's failure
- Sentinel's tilt mode (triggered by clock issues or excessive latency) affects ALL monitored masters — if one master's monitoring causes tilt, all masters lose failover protection
- Sentinel's configuration rewrite (after failover) is a blocking operation that can delay other failovers

**Best practice:** Use separate Sentinel deployments for each master-slave group, or at minimum ensure no more than 3-5 masters per Sentinel group.

---

## 9. Podman-Specific Networking Issues

### DNS Resolution
Podman uses `netavark` or `CNI` for container networking, which has different DNS behavior than Docker:
- Container name resolution may have **longer propagation delays** after container restart
- If using `STRICT_DNS` cluster type in Envoy, DNS resolution failures can cause all hosts to be marked unhealthy
- Podman's DNS resolver (`aardvark-dns`) has known issues with high query rates

### Health Check Connectivity
- Podman's network namespace isolation means health check connections from Envoy to Redis may traverse different network paths than data connections
- In rootless Podman, `slirp4netns` adds significant network overhead and latency, which can cause health check timeouts
- Podman's container restart behavior does not guarantee IP address stability — if a Redis container restarts and gets a new IP, Envoy's STRICT_DNS resolution must detect this

### Sentinel Communication
- Sentinel processes need to communicate with each other and with Redis nodes
- In Podman, if containers are on different networks, Sentinel's `SENTINEL resolve-ip` may not work correctly
- Podman's `pasta` network mode (default in rootless) has different port forwarding behavior that can affect Sentinel's ability to connect to Redis nodes

### Mitigation
- Use `EDS` (Endpoint Discovery Service) instead of `STRICT_DNS` for dynamic endpoint updates
- Set appropriate `dns_refresh_rate` and `dns_failure_refresh_rate` for STRICT_DNS clusters
- Use fixed IP addresses or a DNS service that supports fast updates
- Test health check behavior under `pasta` and `slirp4netns` network modes
- Consider using host networking for Sentinel processes to avoid container networking overhead

---

## 10. Production War Stories

### GitHub Issue #39630 — The Exact Architecture
This issue (filed May 2025) describes **the exact same architecture** as described in the question. The reporter uses:
- ROLE TCP health check with `*3` matching for the rw cluster
- Redis custom health checker (PING) for the ro cluster
- `read_command_policy` to split read/write
- `healthy_panic_threshold: 0`
- `reuse_connection: false`

**Problem encountered:** Clients sending ROLE command get `ERR unknown command 'ROLE'` or `invalid request` when added as a custom command. This is because ROLE is already classified as a ClusterScopeCommand in Envoy's internal command table, and adding it as a custom_command creates a conflict.

### GitHub Issue #21352 — Writes Fail When Replica Goes Down
When any replica fails, SET requests through Envoy return "upstream failure" for a fraction of a second. This happens because Envoy's connection pool has connections to all nodes, and when one fails, in-flight requests on that connection fail even if they should have gone to a different node.

### GitHub Issue #44338 — Transaction Connection Churn
Production evidence from a company that couldn't put their Redis behind Envoy because MULTI/EXEC transactions caused 291x increase in TCP connections, spiking Redis CPU from 1% to 99%.

### GitHub Issue #36962 — Latency Snowball
A production user reports that when one Redis shard fails, **all** requests experience high latency because Envoy must return responses in order. A single failed node can cause cascading latency for the entire proxy.

### GitHub Issue #41659 — Database Routing Bug
Production issue where clients connecting to db 0 were routed to connections left on db 3, causing reads and writes to the wrong database. This is a fundamental design issue with Envoy's connection pooling.

### Redis Issue #926 — Sentinel Doesn't Detect Master→Slave Transition
When a master receives SLAVEOF between Sentinel pings, Sentinel may not detect the state change, leading to inconsistent views of the cluster topology.

### Redis Issue #14313 — Sentinel Promotes Wrong Slave
Sentinel can promote a slave with wrong cluster data during failover due to missing replication ID validation. This means the promoted master may have stale or incomplete data.

---

## Summary: Top 10 Gotchas Ranked by Severity

| # | Gotcha | Severity | Likelihood | Mitigation |
|---|--------|----------|------------|------------|
| 1 | **Split-brain: Envoy routes writes to old master during network partition** | 🔴 Critical | Medium | `min-replicas-to-write 1`; ≥3 Sentinels with quorum≥2 |
| 2 | **Writes lost during failover gap (2-3s window)** | 🔴 Critical | High | `unhealthy_threshold: 1`; client retries; `min-replicas-max-lag` |
| 3 | **ROLE custom command broken in Envoy proxy** | 🟠 High | Certain | Don't add ROLE as custom_command; use separate direct connection for ROLE checks |
| 4 | **Stale reads from replicas (read-after-write inconsistency)** | 🟠 High | Certain | Accept eventual consistency; route critical reads through rw cluster |
| 5 | **SELECT/database routing bug in connection pool** | 🟠 High | High | Always explicitly SELECT; avoid multi-db usage through proxy |
| 6 | **Transaction connection churn (MULTI/EXEC)** | 🟠 High | High (if using transactions) | Avoid transactions through proxy; use Lua scripts instead |
| 7 | **Latency snowball from single failed node** | 🟡 Medium | Medium | Monitor per-shard latency; implement circuit breaking |
| 8 | **RESP3 not supported** | 🟡 Medium | Increasing | Force clients to use RESP2; monitor for HELLO 3 attempts |
| 9 | **Premature master promotion (data not fully synced)** | 🟡 Medium | Low-Medium | Monitor replication lag; use `WAIT` before failover |
| 10 | **Podman DNS/networking delays affecting health checks** | 🟡 Medium | Medium | Use EDS instead of STRICT_DNS; test under pasta/slirp4netns |

---

## Recommended Configuration Changes

```yaml
# Redis nodes - CRITICAL for preventing split-brain
min-replicas-to-write 1
min-replicas-max-lag 10

# Envoy rw cluster - faster failover detection
health_checks:
  unhealthy_threshold: 1    # Was 2, reduce to 1
  healthy_threshold: 2      # Require 2 checks before promoting new master
  interval: 1s
  reuse_connection: false   # Already set, keep it

# Sentinel - faster detection
down-after-milliseconds 5000
failover-timeout 30000     # 30s, not default 180s

# Envoy - op_timeout for fail-fast
settings:
  op_timeout: 1s           # Fail fast on stale connections
```
