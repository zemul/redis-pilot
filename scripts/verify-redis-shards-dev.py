#!/usr/bin/env python3
"""Real API/Proxy acceptance for redis-shards-dev only. No FLUSH, no foreign keys.

Usage on redis01: python3 verify-redis-shards-dev.py PRIVATE_STAGE ACTION
Seed persists a random-prefix manifest; subsequent actions verify the same data.
Only successful writes update expectations. Unknown writes are never replayed.
"""
import json
import pathlib
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import yaml

assert socket.gethostname() == 'redis01'
STAGE = pathlib.Path(sys.argv[1])
CLUSTER = 'redis-shards-dev'
BASE = '/api/v1/custom-clusters/'+CLUSTER
TOKEN = yaml.safe_load(pathlib.Path('/root/.redis-pilot-cli/config.yaml').read_text())['token']
INGRESS = json.loads((STAGE/(CLUSTER+'-ingress.json')).read_text())
assert INGRESS['gateway_address'] == '127.0.0.1:17682'
MANIFEST = STAGE/'acceptance.json'


def api(path, body=None):
    req = urllib.request.Request('http://127.0.0.1:8080'+path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': 'Bearer '+TOKEN, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as response: data = json.load(response)
    assert data['success'], data.get('error')
    return data['data']


class Redis:
    def __init__(self, host, port, password=None):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.file = self.sock.makefile('rb')
        if password: assert self.call('AUTH', password) == b'OK'

    def read(self):
        line = self.file.readline()
        assert line.endswith(b'\r\n'), 'truncated reply'
        kind, val = line[:1], line[1:-2]
        if kind == b'-': raise RuntimeError(val.decode())
        if kind == b'+': return val
        if kind == b':': return int(val)
        if kind == b'$':
            size = int(val)
            if size == -1: return None
            data = self.file.read(size+2)
            assert len(data) == size+2 and data[-2:] == b'\r\n'
            return data[:-2]
        if kind == b'*':
            size = int(val)
            return None if size == -1 else [self.read() for _ in range(size)]
        raise AssertionError('invalid response')

    def call(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        self.sock.sendall(b'*%d\r\n'%len(args)+b''.join(b'$%d\r\n'%len(a)+a+b'\r\n' for a in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def connect():
    return Redis('127.0.0.1', api(BASE)['ingress']['port'], INGRESS['password'])


def inventory():
    current = {c['id']: c for c in api('/api/v1/custom-clusters')}
    for before in json.loads((STAGE/'baseline-clusters.json').read_text()):
        after = current[before['id']]
        for field in ('revision', 'shards', 'ingress'):
            assert after.get(field) == before.get(field), (before['id'], field)
    c = current[CLUSTER]
    print('existing cluster topology/ingress unchanged; test cluster',
          c['id'], 'revision', c['revision'], 'shards', len(c['shards']),
          'Proxy port', c['ingress']['port'], flush=True)


def persist(state):
    tmp = MANIFEST.with_suffix('.next')
    with open(tmp, 'w') as f:
        import os
        os.chmod(tmp, 0o600)
        json.dump(state, f)
    tmp.replace(MANIFEST)


def seed():
    if MANIFEST.exists():
        prior = json.loads(MANIFEST.read_text())
        assert not prior.get('seeded'), 'use existing manifest; do not overwrite expectations'
        cleanup()
        MANIFEST.rename(STAGE/('failed-seed-'+secrets.token_hex(4)+'.json'))
    c = api(BASE)
    assert c['engine'] == 'redis' and c['cache_enabled'] is False and len(c['shards']) == 2
    state = {'prefix': 'redis-shard-accept-'+secrets.token_hex(8)+':', 'records': 120,
             'workers': {'0': 0, '1': 0}, 'expires': int(time.time()*1000)+3600000}
    persist(state)  # preserves cleanup scope even if seeding is interrupted
    r = connect()
    try:
        for i in range(state['records']):
            k = state['prefix']+str(i)+':'
            assert r.call('SET', k+'s', 'value-'+str(i), 'PXAT', state['expires']) == b'OK'
            assert r.call('HSET', k+'h', 'field', 'value-'+str(i), 'keep', 'unchanged') == 2
            assert r.call('RPUSH', k+'l', 'a', 'b', str(i)) == 3
            assert r.call('SADD', k+'set', 'member', str(i)) == 2
            assert r.call('ZADD', k+'z', i, 'member') == 1
        # Cross-node batch operations and same-tag transaction/Lua.
        p = state['prefix']
        assert r.call('MSET', p+'batch-a', 'a', p+'batch-b', 'b') == b'OK'
        assert r.call('MGET', p+'batch-a', p+'batch-b') == [b'a', b'b']
        tx = '{'+p+'tx}'
        assert r.call('MULTI') == b'OK'
        assert r.call('SET', tx+':a', 'a') == b'QUEUED'
        assert r.call('SET', tx+':b', 'b') == b'QUEUED'
        assert r.call('EXEC') == [b'OK', b'OK']
        assert r.call('EVAL', "return redis.call('INCR',KEYS[1])", 1, tx+':counter') == 1
        assert r.call('EVAL', "return _G.redis.call('GET',KEYS[1])", 1, tx+':a') == b'a'
        for source in [
            "return redis.call('GET','__pilot_cache_v1__:owner')",
            "return getfenv(0).redis.call('GET',KEYS[1])",
            "return loadstring('return redis')()",
            "return rawget(_G,'loadstring')('return redis')()",
            "return debug.getupvalue(redis.call,1)",
            "return redis.call('FLUSHDB')",
            "end; redis.call('SET','escaped','bad'); return function()",
        ]:
            try: r.call('EVAL', source, 1, tx+':a')
            except RuntimeError: pass
            else: raise AssertionError('Lua sandbox escape accepted')
        for worker in state['workers']: assert r.call('SET', p+'live-'+worker, 0) == b'OK'
        state['seeded'] = True
        persist(state)
    finally: r.close()
    print('seeded 600 five-type objects + batch/transaction/Lua/live keys', flush=True)


def verify():
    state = json.loads(MANIFEST.read_text())
    assert state.get('seeded')
    r = connect()
    try:
        for i in range(state['records']):
            k = state['prefix']+str(i)+':'
            assert r.call('GET', k+'s') == ('value-'+str(i)).encode(), k+'s'
            ttl = r.call('PTTL', k+'s')
            assert ttl > 0 and abs(time.time()*1000+ttl-state['expires']) < 2000, ('ttl', k, ttl)
            assert r.call('HGET', k+'h', 'field') == ('value-'+str(i)).encode(), k+'h'
            assert r.call('HGET', k+'h', 'keep') == b'unchanged'
            assert r.call('LRANGE', k+'l', 0, -1) == [b'a', b'b', str(i).encode()], k+'l'
            assert set(r.call('SMEMBERS', k+'set')) == {b'member', str(i).encode()}, k+'set'
            assert float(r.call('ZSCORE', k+'z', 'member')) == i, k+'z'
        p = state['prefix']
        assert r.call('MGET', p+'batch-a', p+'batch-b') == [b'a', b'b']
        tx = '{'+p+'tx}'
        assert r.call('MGET', tx+':a', tx+':b', tx+':counter') == [b'a', b'b', b'1']
        for worker, value in state['workers'].items():
            assert r.call('GET', p+'live-'+worker) == str(value).encode()
    finally: r.close()
    print('verified five types, absolute TTL, batch, transaction/Lua, live final values', flush=True)


def migrate(expand):
    state = json.loads(MANIFEST.read_text())
    c = api(BASE)
    targets = [{'id': s['id'], 'instance': s['instance']} for s in c['shards'] if s['id'] != 's3']
    assert len(targets) == 2
    if expand:
        assert len(c['shards']) == 2
        targets.append({'id': 's3', 'instance': CLUSTER+'-extra'})
    else: assert len(c['shards']) == 3
    stop = threading.Event()
    errors, counts, retries = [], [0, 0], [0, 0]
    def writer(n):
        r = connect()
        try:
            while not stop.is_set():
                value = state['workers'][str(n)]+1
                try:
                    assert r.call('SET', state['prefix']+'live-'+str(n), value) == b'OK'
                    state['workers'][str(n)] = value
                    assert r.call('GET', state['prefix']+'live-'+str(n)) == str(value).encode(), 'stale read'
                    counts[n] += 1
                except RuntimeError as e:
                    if not str(e).startswith('TRYAGAIN'): raise
                    retries[n] += 1
                stop.wait(0.02)
        except Exception as e:
            errors.append(repr(e))
            stop.set()
        finally: r.close()
    workers = [threading.Thread(target=writer, args=(n,)) for n in range(2)]
    for w in workers: w.start()
    task = ('expand-' if expand else 'shrink-')+secrets.token_hex(4)
    try:
        req = {'id': task, 'expected_revision': c['revision'], 'shards': targets}
        api(BASE+'/tasks', req)
        api(BASE+'/tasks', req)  # exact replay must not create another migration
        deadline, report_at = time.monotonic()+180, 0
        while True:
            current = api(BASE)
            t = current.get('task') or {}
            assert t.get('id') == task
            if time.monotonic() >= report_at:
                print(task, t.get('phase'), 'pages', t.get('pages'), 'writes', sum(counts),
                      'retryable', sum(retries), 'error', t.get('error_code'), flush=True)
                report_at = time.monotonic()+10
            assert not errors, errors
            if t.get('phase') == 'complete':
                assert current['revision'] == c['revision']+1
                break
            assert time.monotonic() < deadline, t
            time.sleep(0.5)
    finally:
        stop.set()
        for w in workers: w.join()
        persist(state)
    assert not errors and sum(counts) > 0, (errors, counts)
    print(task, 'complete; successful write/read checks', sum(counts), 'retryable', sum(retries), flush=True)
    verify()


def gateway_restart():
    subprocess.run(['systemctl', 'kill', '--kill-who=main', '--signal=SIGKILL', 'redis-gateway-'+CLUSTER], check=True)
    deadline = time.monotonic()+45
    time.sleep(1)
    while True:
        try:
            with urllib.request.urlopen('http://127.0.0.1:19603/readyz', timeout=2) as r: assert r.status == 200
            break
        except Exception:
            if time.monotonic() >= deadline: raise
            time.sleep(0.5)
    print('Gateway automatically restarted and ready', flush=True)
    verify()


def cleanup():
    state = json.loads(MANIFEST.read_text())
    p, r = state['prefix'], connect()
    try:
        keys = [p+str(i)+':'+kind for i in range(state['records']) for kind in ['s','h','l','set','z']]
        keys += [p+'batch-a', p+'batch-b', p+'live-0', p+'live-1']
        keys += ['{'+p+'tx}:'+suffix for suffix in ['a','b','counter']]
        for key in keys: r.call('DEL', key)
    finally: r.close()
    print('deleted only', len(keys), 'keys from manifest prefix', flush=True)


{'seed': seed, 'verify': verify, 'expand': lambda: migrate(True), 'shrink': lambda: migrate(False),
 'gateway-restart': gateway_restart, 'cleanup': cleanup, 'inventory': inventory}[sys.argv[2]]()
