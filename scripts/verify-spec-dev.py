#!/usr/bin/env python3
"""Real Proxy acceptance for the three dedicated spec-* development clusters.

Run on redis01 with the private deployment staging directory as argument.
Restarts only these Gateways plus the control Server, not database containers.
Never FLUSHDB; deletes only this run's random-prefix business keys.
"""
import concurrent.futures
import json
import pathlib
import secrets
import socket
import subprocess as sp
import sys
import time
import urllib.request
import yaml

assert socket.gethostname() == 'redis01'
stage = pathlib.Path(sys.argv[1])
token = yaml.safe_load(pathlib.Path('/root/.redis-pilot-cli/config.yaml').read_text())['token']
prefix = 'spec-accept-'+secrets.token_hex(8)+':'


def api(path, body=None):
    req = urllib.request.Request('http://127.0.0.1:8080'+path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as r: result = json.load(r)
    assert result['success']
    return result['data']


class Redis:
    def __init__(self, host, port, password=None):
        self.sock = socket.create_connection((host, port), timeout=8)
        self.reader = self.sock.makefile('rb')
        if password: assert self.call('AUTH', password) == b'OK'

    def read(self):
        line = self.reader.readline()
        assert line.endswith(b'\r\n'), 'truncated reply'
        tag, value = line[:1], line[1:-2]
        if tag == b'-': raise RuntimeError(value.decode())
        if tag == b'+': return value
        if tag == b':': return int(value)
        if tag == b'$':
            n = int(value)
            if n == -1: return None
            data = self.reader.read(n+2)
            assert len(data) == n+2 and data[-2:] == b'\r\n'
            return data[:-2]
        if tag == b'*':
            n = int(value)
            return None if n == -1 else [self.read() for _ in range(n)]
        raise RuntimeError('unknown frame')

    def call(self, *args):
        data = [str(a).encode() if not isinstance(a, bytes) else a for a in args]
        self.sock.sendall(b'*%d\r\n'%len(data)+b''.join(b'$%d\r\n'%len(a)+a+b'\r\n' for a in data))
        return self.read()

    def close(self):
        self.reader.close()
        self.sock.close()


def verify(cluster, idx):
    ingress = json.loads((stage/(cluster+'-ingress.json')).read_text())
    config = yaml.safe_load(pathlib.Path('/etc/redis-gateway-'+cluster+'/gateway.yaml').read_text())
    state = api('/api/v1/custom-clusters/'+cluster)
    port = state['ingress']['port']
    def connect(): return Redis('127.0.0.1', port, ingress['password'])
    def ready():
        deadline = time.monotonic()+45
        while True:
            c = None
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{19600+idx}/readyz', timeout=2) as r: assert r.status == 200
                c = connect()
                c.call('EXISTS', prefix+'ready')
                return
            except Exception:
                if time.monotonic() >= deadline: raise
                time.sleep(.2)
            finally:
                if c: c.close()
    ready()
    c = connect()
    hot = None
    if state.get('hot_address'):
        host, hotport = state['hot_address'].rsplit(':', 1)
        hot = Redis(host, int(hotport))
    keys = [prefix+cluster+':'+k for k in ['s','h','l','set','z','a','b','ttl','persist']]
    s,h,l,st,z,a,b,ttl,persist = keys
    try:
        assert c.call('SET', s, 1) == b'OK'
        assert c.call('GET', s) == b'1'
        assert c.call('INCR', s) == 2
        assert c.call('GET', s) == b'2'
        pairs = [x for i in range(1000) for x in ('f'+str(i), 'v'+str(i))]
        assert c.call('HSET', h, *pairs) == 1000
        assert c.call('HGET', h, 'f999') == b'v999'
        c.call('HSET', h, 'counter', 0)
        if hot:
            if cluster == 'spec-update': assert hot.call('HLEN', h) == 1001, 'update lost full cache'
            else: assert hot.call('EXISTS', h) == 0, 'invalidate retained cache'
        assert c.call('HGET', h, 'counter') == b'0'
        assert c.call('RPUSH', l, 'a','b') == 2
        assert c.call('LRANGE', l, 0,-1) == [b'a',b'b']
        assert c.call('LSET', l, 0,'c') == b'OK'
        assert c.call('LRANGE', l, 0,-1) == [b'c',b'b']
        assert c.call('SADD', st, 'a','b') == 2
        assert set(c.call('SMEMBERS', st)) == {b'a',b'b'}
        c.call('SREM', st,'a')
        assert c.call('SMEMBERS', st) == [b'b']
        assert c.call('ZADD', z, 1,'a',2,'b') == 2
        assert c.call('ZRANGE', z,0,-1) == [b'a',b'b']
        assert c.call('ZINCRBY', z, 3,'a') == b'4'
        assert c.call('ZRANGE', z,0,-1) == [b'b',b'a']
        assert c.call('MSET', a,'A',b,'B') == b'OK'
        assert c.call('MGET', a,b) == [b'A',b'B']
        assert c.call('SET', ttl,'expires','PX',500) == b'OK'
        assert c.call('GET', ttl) == b'expires'
        time.sleep(.7)
        assert c.call('GET', ttl) is None
        # Two clients share one Hash: check each client's completed write and
        # unrelated fields, without incorrectly demanding cross-client order.
        def writer(n):
            conn = connect()
            try:
                for i in range(100):
                    conn.call('HSET', h, 'writer'+str(n), i)
                    assert conn.call('HGET', h, 'writer'+str(n)) == str(i).encode(), 'stale write'
                    assert conn.call('HGET', h,'f999') == b'v999'
            finally: conn.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(writer, range(2)))
        assert c.call('SET', persist,'survives') == b'OK'
        c.close()
        sp.run(['systemctl','restart','redis-gateway-'+cluster], check=True)
        ready()
        c = connect()
        assert c.call('GET', persist) == b'survives'
        assert c.call('HGET', h,'f999') == b'v999'
        assert c.call('HGET', h,'writer0') == b'99'
        assert c.call('HGET', h,'writer1') == b'99'
        # Control-plane restart and replay must not create or probe populated DBs.
        if idx == 0:
            sp.run(['systemctl','restart','redis-pilot-server'], check=True)
            for _ in range(40):
                try:
                    spec = json.loads((stage/(cluster+'-spec.json')).read_text())
                    replay = api('/api/v1/custom-clusters/create',spec)
                    break
                except Exception: time.sleep(.25)
            else: raise AssertionError('Server did not recover')
            tasks = json.loads((stage/'tasks.json').read_text())
            assert replay['task_id'] == tasks[cluster] and replay['status'] == 'success'
            assert c.call('GET',persist) == b'survives'
        print(cluster, 'PASS five-types, multi-key, TTL, 200 concurrent write/read rounds, Gateway restart, cache semantics; Proxy port',port, flush=True)
    finally:
        if hot: hot.close()
        c.close()
        cleanup = connect()
        try:
            for k in keys: cleanup.call('DEL',k)
        finally: cleanup.close()


for idx, cluster in enumerate(['spec-update','spec-invalidate','spec-nocache']): verify(cluster, idx)
print('PASS; only own random-prefix business keys removed; clusters retained', flush=True)
