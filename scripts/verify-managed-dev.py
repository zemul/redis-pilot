#!/usr/bin/env python3
"""Run on redis01 against the dedicated managed-dev stack only.

Uses the real Server API, CLI task commands and business Proxy. Restarts Server
once and Gateway between policies, but never stops ordinary database instances.
Requires the root-only ingress JSON created during this stack's installation.
"""
import json
import pathlib
import secrets
import socket
import subprocess as sp
import sys
import threading
import time
import urllib.request
import yaml

assert socket.gethostname() == 'redis01', 'development host only'
ingress = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert ingress['gateway_address'] == '127.0.0.1:17379'
cli = yaml.safe_load(pathlib.Path('/root/.redis-pilot-cli/config.yaml').read_text())
config_path = pathlib.Path('/etc/redis-gateway/gateway.yaml')
original = yaml.safe_load(config_path.read_text())
assert original['service_id'] == 'managed-dev'
prefix = 'managed-accept-' + secrets.token_hex(6) + ':'
keys = []


def api(path):
    req = urllib.request.Request('http://127.0.0.1:8080' + path,
                                 headers={'Authorization': 'Bearer ' + cli['token']})
    with urllib.request.urlopen(req, timeout=10) as response:
        body = json.load(response)
    assert body['success'], body.get('error')
    return body['data']


def cluster():
    return api('/api/v1/cold-clusters/managed-dev')


class Redis:
    def __init__(self, address, password=None):
        self.sock = socket.create_connection(address, timeout=10)
        self.file = self.sock.makefile('rb')
        if password:
            assert self.call('AUTH', password) == b'OK'

    def read(self):
        line = self.file.readline()
        if not line.endswith(b'\r\n'):
            raise RuntimeError('truncated Redis reply')
        tag, value = line[:1], line[1:-2]
        if tag == b'-':
            raise RuntimeError(value.decode())
        if tag == b'+':
            return value
        if tag == b':':
            return int(value)
        if tag == b'$':
            size = int(value)
            if size == -1:
                return None
            data = self.file.read(size + 2)
            assert len(data) == size + 2 and data[-2:] == b'\r\n'
            return data[:-2]
        if tag == b'*':
            size = int(value)
            return None if size == -1 else [self.read() for _ in range(size)]
        raise RuntimeError('unknown reply')

    def call(self, *args):
        data = [str(a).encode() if not isinstance(a, bytes) else a for a in args]
        frame = b'*%d\r\n' % len(data)
        frame += b''.join(b'$%d\r\n' % len(a) + a + b'\r\n' for a in data)
        self.sock.sendall(frame)
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def connect():
    return Redis(('127.0.0.1', cluster()['ingress']['port']), ingress['password'])


def gateway_restart(policy):
    cfg = yaml.safe_load(config_path.read_text())
    cfg['cache']['write_policy'] = policy
    config_path.write_text(yaml.safe_dump(cfg))
    sp.run(['systemctl', 'restart', 'redis-gateway'], check=True)
    deadline = time.monotonic() + 30
    while True:
        c = None
        try:
            with urllib.request.urlopen('http://127.0.0.1:19200/readyz', timeout=2) as response:
                assert response.status == 200
            c = connect()
            # Proxy handles PING locally; only a business command proves the
            # freshly restarted Gateway/backend path is actually available.
            assert c.call('EXISTS', prefix + 'readiness') == 0
            c.close()
            return
        except Exception:
            if c:
                c.close()
            if time.monotonic() > deadline:
                raise
            time.sleep(.2)


def migrate(ids, label, restart_server=False):
    state = cluster()
    task = dict(id=label + '-' + secrets.token_hex(4), expected_revision=state['revision'],
                shards=[dict(id=i, instance='pmdev-cold-' + i) for i in ids])
    path = pathlib.Path('/run') / (task['id'] + '.json')
    path.write_text(json.dumps(task))
    path.chmod(0o600)
    try:
        result = sp.run(['redis-pilot-cli', 'cold-cluster', 'migrate', 'managed-dev', '--file', str(path)],
                        capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        path.unlink()
    if restart_server:
        sp.run(['systemctl', 'restart', 'redis-pilot-server'], check=True)
    deadline = time.monotonic() + 90
    while True:
        try:
            state = cluster()
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.2)
            continue
        if state['revision'] == task['expected_revision'] + 1 and state['task']['phase'] == 'complete':
            print('migration complete', task['id'], state['revision'], flush=True)
            return
        if time.monotonic() > deadline:
            raise RuntimeError('migration timeout: ' + json.dumps(state['task']))
        time.sleep(.2)


def writer(key):
    stop = threading.Event()
    outcome = {}

    def work():
        c = None
        try:
            c = connect()
            i = 0
            while not stop.is_set():
                i += 1
                c.call('HSET', key, 'counter', i)
                assert c.call('HGET', key, 'counter') == str(i).encode(), 'stale counter'
                assert c.call('HGET', key, 'f999') == b'v999', 'untouched field changed'
                outcome['last'] = i
                time.sleep(.01)
        except Exception as e:
            outcome['error'] = repr(e)
        finally:
            if c:
                c.close()

    thread = threading.Thread(target=work)
    thread.start()

    def finish():
        stop.set()
        thread.join(15)
        assert not thread.is_alive(), 'writer stuck'
        assert 'error' not in outcome, outcome
        assert outcome.get('last', 0) > 0, outcome
        return outcome['last']
    return finish


try:
    state = cluster()
    assert {n['id'] for n in state['shards']} <= {'a', 'b'}, 'unexpected dedicated topology'
    assert not state.get('task') or state['task']['phase'] == 'complete', 'another migration is active'
    if [n['id'] for n in state['shards']] != ['a']:
        migrate(['a'], 'prepare-single')
    for policy, remaining in [('invalidate', 'b'), ('update_existing', 'a')]:
        gateway_restart(policy)
        c = connect()
        h = prefix + policy + ':hash'
        keys.append(h)
        pairs = [x for i in range(1000) for x in ('f' + str(i), 'v' + str(i))]
        assert c.call('HSET', h, *pairs) == 1000
        assert c.call('HGET', h, 'f999') == b'v999'
        c.call('HSET', h, 'counter', 0)
        if policy == 'update_existing':
            hot = Redis(('10.70.100.63', 6383))
            assert hot.call('HLEN', h) == 1001, 'complete Hash cache was lost'
            hot.close()
        for i in range(40):
            k = prefix + policy + ':s' + str(i)
            keys.append(k)
            assert c.call('SET', k, 'value', 'PX', 120000) == b'OK'
        for kind in ['list', 'set', 'zset']:
            k = prefix + policy + ':' + kind
            keys.append(k)
            if kind == 'list': c.call('RPUSH', k, 'a', 'b')
            if kind == 'set': c.call('SADD', k, 'a', 'b')
            if kind == 'zset': c.call('ZADD', k, 1, 'a', 2, 'b')
        c.close()
        finish = writer(h)
        try:
            migrate(['a', 'b'], policy + '-expand', restart_server=policy == 'invalidate')
            migrate([remaining], policy + '-shrink')
        finally:
            last = finish()
        gateway_restart(policy)
        c = connect()
        assert c.call('HGET', h, 'counter') == str(last).encode()
        assert c.call('HGET', h, 'f999') == b'v999'
        assert c.call('LRANGE', prefix + policy + ':list', 0, -1) == [b'a', b'b']
        assert sorted(c.call('SMEMBERS', prefix + policy + ':set')) == [b'a', b'b']
        assert c.call('ZRANGE', prefix + policy + ':zset', 0, -1) == [b'a', b'b']
        for i in range(40):
            assert c.call('GET', prefix + policy + ':s' + str(i)) == b'value'
        c.close()
        print('policy passed', policy, 'writer iterations', last, flush=True)
    migrate(['a', 'b'], 'final-expand')
finally:
    gateway_restart(original['cache']['write_policy'])
    c = connect()
    try:
        for key in keys:
            c.call('DEL', key)
    finally:
        c.close()
print('managed-dev acceptance passed; own test keys removed; final two-shard topology retained')
