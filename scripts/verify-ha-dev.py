#!/usr/bin/env python3
"""Real Server/Agent/3 Sentinel/Proxy/Gateway HA acceptance on dedicated ha-* DBs.

Run on redis01 with private stage, action and cluster. Never flush cold data.
Fault actions always restore stopped containers/services in finally blocks.
Unknown writes are counted, not retried. No zero-loss claim for async failover.
"""
import concurrent.futures
import importlib.util
import json
import pathlib
import secrets
import socket
import sqlite3
import subprocess
import sys
import threading
import time

ROOT = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('installer', ROOT/'deploy-spec-dev.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)
api, run = installer.api, installer.run
CLUSTER = sys.argv[3]
assert CLUSTER in ('ha-redis-dev', 'ha-update-dev', 'ha-invalidate-dev')
BASE = '/api/v1/custom-clusters/'+CLUSTER
PATH = ROOT/(CLUSTER+'-acceptance.json')
IPS = {'redis01': '10.70.100.67', 'redis02': '10.70.100.63', 'redis03': '10.70.100.68'}


class Redis:
    def __init__(self, host, port, password=None):
        self.sock = socket.create_connection((host, port), timeout=3)
        self.file = self.sock.makefile('rb')
        if password: assert self.call('AUTH', password) == b'OK'

    def read(self):
        line = self.file.readline()
        if not line.endswith(b'\r\n'): raise OSError('truncated reply')
        kind, value = line[:1], line[1:-2]
        if kind == b'-': raise RuntimeError(value.decode())
        if kind == b'+': return value
        if kind == b':': return int(value)
        if kind == b'$':
            n = int(value)
            if n == -1: return None
            data = self.file.read(n+2)
            if len(data) != n+2: raise OSError('short bulk')
            return data[:-2]
        if kind == b'*':
            n = int(value)
            return None if n == -1 else [self.read() for _ in range(n)]
        raise AssertionError('RESP framing')

    def call(self, *args):
        args = [v if isinstance(v, bytes) else str(v).encode() for v in args]
        self.sock.sendall(b'*%d\r\n'%len(args)+b''.join(b'$%d\r\n'%len(v)+v+b'\r\n' for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()

    def __enter__(self): return self
    def __exit__(self, *args): self.close()


def connect():
    ingress = json.loads((ROOT/(CLUSTER+'-ingress.json')).read_text())
    return Redis('127.0.0.1', api(BASE)['ingress']['port'], ingress['password'])


def persist(state):
    import os
    tmp = PATH.with_suffix('.next')
    with open(tmp, 'w') as f:
        os.chmod(tmp, 0o600)
        json.dump(state, f)
    tmp.replace(PATH)


def await_ok(fn, seconds=90):
    deadline = time.monotonic()+seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result: return result
        except (OSError, RuntimeError, AssertionError) as e: last = str(e)
        time.sleep(.3)
    raise AssertionError(('timed out', last))


def primaries():
    p = pathlib.Path('/var/lib/redis-gateway-'+CLUSTER+'/sharding/runtime.db')
    with sqlite3.connect('file:'+str(p)+'?mode=ro', uri=True) as db:
        return {x['current']['shard_id']: x for (body,) in db.execute('SELECT body FROM shard_primaries')
                for x in [json.loads(body)]}


def backend(p):
    host, port = p['address'].rsplit(':', 1)
    return Redis(host, int(port))


def resource(name): return api('/instance/list')['instances'][name]


def podman(action, inst):
    assert inst['container'].startswith(('redis-ha-', 'kvrocks-ha-'))
    assert action in ('stop', 'start', 'pause', 'unpause')
    args = ['podman', action]
    if action == 'stop': args += ['--time', '2']
    args.append(inst['container'])
    if inst['server'] != 'redis01': args = ['ssh', '-o', 'BatchMode=yes', inst['server']]+args
    run(*args)


def replication_ready(ack=True):
    state = json.loads(PATH.read_text())
    for v in primaries().values():
        with backend(v['current']) as r:
            info = r.call('INFO', 'replication').decode()
            assert 'role:master' in info and 'connected_slaves:1' in info, info
            if not ack: continue
            # WAIT on a fresh connection alone isn't proof of earlier writes.
            # Re-write this run's marker first, then confirm its replication.
            # Establish the marker through the real router first, so every
            # marker exists on its canonical shard (not duplicated on nodes).
            with connect() as client:
                for i in range(100):
                    key, value = state['prefix']+'repl-'+str(i), 'marker'
                    client.call('SET', key, value)
                    if r.call('GET', key) == b'marker': break
                else: raise AssertionError('cannot locate shard marker')
            r.call('SET', key, value)
            assert r.call('WAIT', 1, 2000) == 1
    return True


def seed():
    assert not PATH.exists(), 'manifest exists; do not overwrite'
    state = {'prefix': 'ha-accept-'+secrets.token_hex(8)+':', 'records': 240,
             'expires': int(time.time()*1000)+14400000}
    persist(state)
    c = api(BASE)
    assert len(c['shards']) == 2
    state['original_shards'] = [{'id': s['id'], 'instance': s['instance']} for s in c['shards']]
    p = state['prefix']
    with connect() as r:
        for i in range(state['records']):
            k = p+str(i)+':'
            r.call('SET', k+'s', i, 'PXAT', state['expires'])
            r.call('HSET', k+'h', 'v', i, 'keep', 'intact')
            r.call('RPUSH', k+'l', 'a', 'b', i)
            r.call('SADD', k+'set', 'a', i)
            r.call('ZADD', k+'z', i, 'a')
        r.call('MSET', p+'a', 'A', p+'b', 'B')
        tag = '{'+p+'tx}'
        assert r.call('MULTI') == b'OK'
        assert r.call('SET', tag+':a', 'T') == b'QUEUED'
        assert r.call('GET', tag+':a') == b'QUEUED'
        assert r.call('EXEC') == [b'OK', b'T']
        assert r.call('EVAL', "return redis.call('INCR',KEYS[1])", 1, tag+':n') == 1
        args = [v for i in range(2000) for v in ('f'+str(i), 'v'+str(i))]
        r.call('HSET', p+'large', *args)
        assert r.call('HGET', p+'large', 'f1999') == b'v1999'
        r.call('HSET', p+'large', 'counter', 1)
        if c.get('hot_address'):
            host, port = c['hot_address'].rsplit(':', 1)
            with Redis(host, int(port)) as hot:
                if CLUSTER == 'ha-update-dev': assert hot.call('HLEN', p+'large') == 2001
                else: assert hot.call('EXISTS', p+'large') == 0
        assert r.call('HGET', p+'large', 'counter') == b'1'
        r.call('SET', p+'expiry', 'x', 'PX', 200)
        r.call('GET', p+'expiry')
        time.sleep(.3)
        assert r.call('GET', p+'expiry') is None
    state['seeded'] = True
    persist(state)
    await_ok(replication_ready)
    verify()
    print(CLUSTER, 'seed PASS: 1200 objects, five types, TTL, transaction/Lua, 2000-field cache policy', flush=True)


def verify():
    state = json.loads(PATH.read_text())
    assert state['seeded']
    p = state['prefix']
    with connect() as r:
        for i in range(state['records']):
            k = p+str(i)+':'
            assert r.call('GET', k+'s') == str(i).encode(), k
            ttl = r.call('PTTL', k+'s')
            assert ttl > 0 and abs(time.time()*1000+ttl-state['expires']) < 2000, ('ttl', k)
            assert r.call('HGET', k+'h', 'v') == str(i).encode(), k
            assert r.call('HGET', k+'h', 'keep') == b'intact'
            assert r.call('LRANGE', k+'l', 0, -1) == [b'a', b'b', str(i).encode()], k
            assert set(r.call('SMEMBERS', k+'set')) == {b'a', str(i).encode()}, k
            assert float(r.call('ZSCORE', k+'z', 'a')) == i, k
        assert r.call('MGET', p+'a', p+'b') == [b'A', b'B']
        assert r.call('HGET', p+'large', 'f1999') == b'v1999'
        assert r.call('HGET', p+'large', 'counter') == b'1'
        assert r.call('GET', '{'+p+'tx}:a') == b'T'
        assert r.call('GET', '{'+p+'tx}:n') == b'1'
    print(CLUSTER, 'full data/absolute TTL/cache verify PASS', flush=True)
    return True


def load(stop, result, worker):
    prefix = json.loads(PATH.read_text())['prefix']
    r = None
    # Each worker owns its key; no false stale-read report from competing writes.
    key = prefix+'live-'+str(worker)
    i = 0
    while not stop.is_set():
        try:
            if r is None: r = connect()
            i += 1
            r.call('SET', key, i)
            got = r.call('GET', key)
            if got != str(i).encode():
                result['stale'].append((worker, i, repr(got)))
                stop.set()
            else: result['checks'] += 1
        except (RuntimeError, OSError) as e:
            result['errors'] += 1
            result.setdefault('error_types', {})[str(e).split(' ')[0]] = result.get('error_types', {}).get(str(e).split(' ')[0], 0)+1
            if r:
                r.close()
                r = None
        stop.wait(.02)
    if r: r.close()


def workload(action):
    stop = threading.Event()
    result = {'checks': 0, 'errors': 0, 'stale': []}
    threads = [threading.Thread(target=load, args=(stop, result, n)) for n in range(3)]
    for t in threads: t.start()
    try: action()
    finally:
        stop.set()
        for t in threads: t.join()
        print(CLUSTER, 'workload', json.dumps(result), flush=True)
    assert not result['stale'] and result['checks'] > 0, result


def failover(shard='s1', migrating=False):
    await_ok(lambda: replication_ready(not migrating))
    old = primaries()[shard]['current']
    inst = resource(old['instance'])
    print(CLUSTER, 'stopping primary', old['instance'], 'generation', old['generation'], flush=True)
    started = time.monotonic()
    podman('stop', inst)
    try:
        def switched():
            new = primaries()[shard]
            return (not new.get('pending') and new['current']['generation'] > old['generation']
                    and new['current']['instance'] != old['instance'])
        await_ok(switched, 100)
        print(CLUSTER, 'automatic Sentinel→Server→Gateway switch seconds', round(time.monotonic()-started, 2),
              'current', primaries()[shard]['current'], flush=True)
    finally:
        podman('start', inst)
    await_ok(lambda: replication_ready(not migrating), 100)
    await_ok(verify)
    print(CLUSTER, 'old primary rejoined as replica', flush=True)


def restart():
    unit = 'redis-gateway-'+CLUSTER
    before = run('systemctl', 'show', unit, '-p', 'MainPID', '--value')
    run('systemctl', 'kill', '--kill-who=main', '--signal=SIGKILL', unit)
    await_ok(lambda: run('systemctl', 'show', unit, '-p', 'MainPID', '--value') not in ('0', before))
    await_ok(verify)
    print(CLUSTER, 'Gateway SIGKILL recovery PASS', flush=True)


def hot_failure():
    assert CLUSTER != 'ha-redis-dev'
    inst = resource(CLUSTER+'-hot')
    p = json.loads(PATH.read_text())['prefix']
    with connect() as r:
        r.call('SET', p+'hot-fault', 'old')
        assert r.call('GET', p+'hot-fault') == b'old'
        podman('stop', inst)
        try:
            r.call('SET', p+'hot-fault', 'new')
            assert r.call('GET', p+'hot-fault') == b'new'
        finally: podman('start', inst)
        for _ in range(100):
            assert r.call('GET', p+'hot-fault') == b'new'
            time.sleep(.03)
    print(CLUSTER, 'hot outage write-cold/recovery no stale cache PASS', flush=True)


def server_outage():
    p = json.loads(PATH.read_text())['prefix']
    with connect() as r:
        run('systemctl', 'stop', 'redis-pilot-server')
        try:
            for i in range(400):
                assert r.call('SET', p+'control-outage', i) == b'OK'
                assert r.call('GET', p+'control-outage') == str(i).encode()
                time.sleep(.025)
        finally: run('systemctl', 'start', 'redis-pilot-server')
    await_ok(lambda: api(BASE))
    verify()
    print(CLUSTER, 'Server unavailable: 400 Proxy write/read checks PASS', flush=True)


def replica_outage():
    old = primaries()['s1']['current']
    replicas = [v for v in api('/instance/list')['instances'].values()
                if v['group'] == old['group'] and v['role'] == 'replica']
    assert len(replicas) == 1
    replica = replicas[0]
    podman('stop', replica)
    try:
        p = json.loads(PATH.read_text())['prefix']
        with connect() as r:
            for i in range(400):
                r.call('SET', p+'replica-outage', i)
                assert r.call('GET', p+'replica-outage') == str(i).encode()
                time.sleep(.02)
        assert primaries()['s1']['current'] == old
    finally: podman('start', replica)
    await_ok(replication_ready)
    print(CLUSTER, 'replica unavailable: 400 write/read checks, no election PASS', flush=True)


def offline_election():
    await_ok(replication_ready)
    old = primaries()['s1']['current']
    inst = resource(old['instance'])
    unit = 'redis-gateway-'+CLUSTER
    run('systemctl', 'stop', unit)
    try:
        podman('stop', inst)
        try:
            await_ok(lambda: api('/instance/list')['groups'][old['group']]['current_master'] != old['instance'])
            run('systemctl', 'start', unit)
            await_ok(lambda: primaries()['s1']['current']['generation'] > old['generation'])
        finally: podman('start', inst)
    finally: run('systemctl', 'start', unit)
    await_ok(replication_ready)
    verify()
    print(CLUSTER, 'election while Gateway offline; restart discovers current primary PASS', flush=True)


def audit():
    clusters = {c['id']: c for c in api('/api/v1/custom-clusters')}
    for old in json.loads((ROOT/'baseline-clusters.json').read_text()):
        for field in ('revision', 'shards', 'ingress'):
            assert old.get(field) == clusters[old['id']].get(field), (old['id'], field)
    c = clusters[CLUSTER]
    assert c.get('task', {}).get('phase', 'complete') == 'complete'
    for shard, v in primaries().items():
        assert not v.get('pending')
        p = v['current']
        st = api('/instance/list')
        assert st['groups'][p['group']]['current_master'] == p['instance']
        for host in IPS.values():
            with Redis(host, 26379) as sentinel:
                found = sentinel.call('SENTINEL', 'GET-MASTER-ADDR-BY-NAME', p['group'])
                assert b':'.join(found).decode() == p['address'], (shard, found, p)
    await_ok(replication_ready)
    assert run('systemctl', 'is-enabled', 'redis-gateway-'+CLUSTER) == 'enabled'
    verify()
    print(CLUSTER, '3 Sentinel / Server / Gateway agree; old clusters unchanged; systemd enabled PASS', flush=True)


def cleanup():
    state = json.loads(PATH.read_text())
    p = state['prefix']
    keys, cursor = set(), '0'
    with connect() as r:
        while True:
            cursor, page = r.call('SCAN', cursor, 'MATCH', '*'+p+'*', 'COUNT', 200)
            keys.update(page)
            if cursor == b'0': break
        assert all(p.encode() in k for k in keys)
        for k in keys: r.call('DEL', k)
    state['cleaned'] = True
    persist(state)
    print(CLUSTER, 'removed only this manifest prefix', len(keys), 'keys; no FLUSH', flush=True)


def migrate(fault=False):
    state, c = json.loads(PATH.read_text()), api(BASE)
    # Remove the old harness's non-routed acknowledgement marker only. The
    # random prefix is owned by this run; never remove arbitrary backend keys.
    for v in primaries().values():
        with backend(v['current']) as r:
            r.call('DEL', state['prefix']+'replication-check')
            assert r.call('WAIT', 1, 2000) == 1
    shrinking = len(c['shards']) == 2
    targets = [state['original_shards'][0]] if shrinking else state['original_shards']
    req = {'id': ('ha-shrink-' if shrinking else 'ha-expand-')+secrets.token_hex(4),
           'expected_revision': c['revision'], 'shards': targets}
    api(BASE+'/tasks', req)
    api(BASE+'/tasks', req)
    if fault:
        # Kill current s1 while the accepted migration is active, then recover
        # its replica so WAIT can progress. Verify active phase first.
        await_ok(lambda: (api(BASE).get('task') or {}).get('phase') == 'active')
        phase = api(BASE)['task']['phase']
        assert phase != 'complete'
        print(CLUSTER, 'injecting election during migration', phase, flush=True)
        failover('s1', True)
        restart()
    last = [None]
    def done():
        t = api(BASE).get('task') or {}
        summary = (t.get('phase'), t.get('error_code'))
        if summary != last[0]:
            print(CLUSTER, 'migration', req['id'], summary, 'pages', t.get('pages'), flush=True)
            last[0] = summary
        return t.get('id') == req['id'] and t.get('phase') == 'complete'
    await_ok(done, 900)
    assert api(BASE)['revision'] == c['revision']+1
    verify()


def continue_fault():
    assert api(BASE)['task']['phase'] == 'active'
    failover('s1', True)
    restart()
    await_ok(lambda: api(BASE)['task']['phase'] == 'complete', 900)
    verify()
    print(CLUSTER, 'active migration + election + Gateway crash recovery PASS', flush=True)


if __name__ == '__main__':
    actions = {'seed': seed, 'verify': verify, 'failover': lambda: workload(failover),
               'restart': restart, 'hot-failure': hot_failure,
               'migrate': lambda: workload(migrate), 'migrate-fault': lambda: workload(lambda: migrate(True)),
               'continue-fault': lambda: workload(continue_fault),
               'server-outage': server_outage, 'replica-outage': replica_outage,
               'offline-election': offline_election,
               'audit': audit, 'cleanup': cleanup,
               'load': lambda: workload(lambda: time.sleep(20)), 'replication': lambda: await_ok(replication_ready)}
    actions[sys.argv[2]]()
