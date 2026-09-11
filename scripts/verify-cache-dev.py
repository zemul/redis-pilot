#!/usr/bin/env python3
"""Destructive fault tests ONLY for the isolated stack from deploy-cache-dev.py.

Run as root on redis03. Restarts its Gateway/hot container, switches both write
policies and generates cache eviction pressure. Gateway recovery flushes ONLY
the dedicated hot DB; this script never flushes cold storage.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import subprocess as sp
import time
import uuid
import yaml

base = Path('/etc/redis-gateway-cache')
passwords = {n: (base/(n+'.secret')).read_text().strip() for n in ['frontend','hot','cold']}
prefix = 'acceptance:' + uuid.uuid4().hex + ':'
keys = set()

class Redis:
    def __init__(self, port=17480, password=None):
        self.s=socket.create_connection(('127.0.0.1',port),timeout=5)
        self.s.settimeout(10)
        self.f=self.s.makefile('rb')
        self.call('AUTH', password or passwords['frontend'])
    def close(self):
        self.f.close(); self.s.close()
    def send(self,*args):
        args=[str(x).encode() for x in args]
        self.s.sendall(b'*%d\r\n'%len(args)+b''.join(b'$%d\r\n'%len(x)+x+b'\r\n' for x in args))
    def read(self):
        line=self.f.readline()
        if not line: raise EOFError('backend closed')
        kind, value=line[:1],line[1:-2]
        if kind==b'-': raise RuntimeError(value.decode())
        if kind==b'+': return value.decode()
        if kind==b':': return int(value)
        if kind==b'$':
            n=int(value)
            if n<0: return None
            data=self.f.read(n); assert self.f.read(2)==b'\r\n'; return data.decode()
        if kind==b'*': return None if int(value)<0 else [self.read() for _ in range(int(value))]
        raise RuntimeError('invalid RESP')
    def call(self,*args): self.send(*args); return self.read()

def key(name):
    k=prefix+name; keys.add(k); return k

def service(action,name): sp.run(['systemctl',action,name],check=True)

def ready():
    last=None
    for _ in range(60):
        r=None
        try:
            r=Redis(); assert r.call('SET',key('ready'),'1')=='OK'; r.close(); return
        except (OSError,RuntimeError,EOFError) as e: last=e; time.sleep(.2)
        finally:
            if r is not None: r.close()
    raise RuntimeError('stack failed to become ready') from last

def run(policy):
    path=base/'gateway.yaml'
    cfg=yaml.safe_load(path.read_text()); cfg['cache']['write_policy']=policy
    path.write_text(yaml.safe_dump(cfg)); service('restart','redis-gateway-cache'); ready()
    r=Redis()
    try:
        k=key(policy+'-pipeline')
        for a in [('SET',k,'0'),('INCR',k),('GET',k)]: r.send(*a)
        assert [r.read(),r.read(),r.read()]==['OK',1,'1']
        def increment(_):
            c=Redis()
            try:
                for _ in range(20): c.call('INCR',k)
            finally: c.close()
        with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(increment,range(4)))
        assert r.call('GET',k)=='81'
        queue='queue:'+key(policy+'-blocking'); keys.add(queue)
        def pop():
            c=Redis()
            try: return c.call('BLPOP',queue,2)
            finally: c.close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiting=pool.submit(pop)
            time.sleep(.1)
            assert r.call('RPUSH',queue,'work')==1
            assert waiting.result()==[queue,'work']
        large=key(policy+'-large'); payload='L'*70000
        assert r.call('SET',large,payload)=='OK'
        assert r.call('GET',large)==payload
        cases=[('hash',('HSET','f','old'),('HSET','g','new'),('HGETALL',),['f','old','g','new']),
               ('list',('RPUSH','old'),('RPUSH','new'),('LRANGE',0,-1),['old','new']),
               ('set',('SADD','old'),('SADD','new'),('SMEMBERS',),['new','old']),
               ('zset',('ZADD',1,'old'),('ZADD',2,'new'),('ZRANGE',0,-1),['old','new'])]
        for name,initial,update,read,want in cases:
            obj=key(policy+name)
            r.call(initial[0],obj,*initial[1:]); r.call(read[0],obj,*read[1:])
            r.call(update[0],obj,*update[1:]); got=r.call(read[0],obj,*read[1:])
            assert sorted(got)==sorted(want),(name,got)
        # Larger than the removed 64KiB/256-element admission limits.
        big=key(policy+'-big-hash')
        for start in range(0,5000,100):
            fields=[]
            for i in range(start,start+100): fields += ['f'+str(i),'v'*100]
            assert r.call('HSET',big,*fields)==100
        assert r.call('HLEN',big)==5000
        inspect=Redis(19490,passwords['hot'])
        try:
            assert inspect.call('HLEN',big)==5000
            assert r.call('HSET',big,'f0','new')==0
            if policy=='update_existing':
                assert inspect.call('HLEN',big)==5000
                assert inspect.call('HGET',big,'f0')=='new'
                assert inspect.call('HGET',big,'f4999')=='v'*100
            else:
                assert inspect.call('EXISTS',big)==0
        finally: inspect.close()
        assert r.call('HGET',big,'f0')=='new'
        # NX no-op, KEEPTTL, expire and rewarm.
        ttl=key(policy+'ttl'); r.call('SET',ttl,'v','PX',300)
        assert r.call('GET',ttl)=='v'
        assert r.call('SET',ttl,'bad','NX') is None
        r.call('SET',ttl,'v2','KEEPTTL'); time.sleep(.35)
        assert r.call('GET',ttl) is None
        # Hot failure must not fail a successful cold write or expose old data.
        r.call('SET',k,'old'); r.call('GET',k)
        service('stop','redis-cache-dev-hot')
        try:
            assert r.call('SET',k,'new')=='OK'
            assert r.call('GET',k)=='new'
        finally: service('start','redis-cache-dev-hot')
        ready(); assert r.call('GET',k)=='new'
        r.close()
        # systemd must automatically recover from process death.
        sp.run(['systemctl','kill','--kill-whom=main','--signal=KILL','redis-gateway-cache'],check=True)
        ready(); r=Redis(); assert r.call('GET',k)=='new'
        # Cold outage: only cached reads may proceed, no hot-only writes.
        r.call('GET',k); service('stop','redis-cache-dev-cold')
        try:
            assert r.call('GET',k)=='new'
            try: r.call('SET',k,'forbidden')
            except RuntimeError: pass
            else: raise AssertionError('cold outage accepted write')
        finally: service('start','redis-cache-dev-cold')
        ready(); assert r.call('GET',k)=='new'
        # Pressure Redis directly, without changing the authoritative database.
        hot=Redis(19490,passwords['hot'])
        try:
            before=int(dict(x.split(':',1) for x in hot.call('INFO','stats').splitlines() if ':' in x)['evicted_keys'])
            for i in range(10000): hot.call('SET',prefix+'pressure:'+str(i),'x'*4096,'PX',1000)
            after=int(dict(x.split(':',1) for x in hot.call('INFO','stats').splitlines() if ':' in x)['evicted_keys'])
            assert after>before,'LRU pressure did not cause eviction'
        finally: hot.close()
        assert r.call('GET',k)=='new'
        print(policy+': pipeline, concurrency, blocking, large object, complete collections, TTL, hot/cold outage, process restart, LRU passed',flush=True)
    finally: r.close()

try:
    for policy in ['invalidate','update_existing']: run(policy)
    # Start while cold is down for longer than the old 5-attempt restart cap.
    service('stop','redis-cache-dev-cold')
    service('restart','redis-gateway-cache')
    time.sleep(18)
    service('start','redis-cache-dev-cold')
    ready()
    print('prolonged cold-unavailable startup recovered automatically',flush=True)
finally:
    service('start','redis-cache-dev-hot'); service('start','redis-cache-dev-cold')
    cfg=yaml.safe_load((base/'gateway.yaml').read_text()); cfg['cache']['write_policy']='invalidate'
    (base/'gateway.yaml').write_text(yaml.safe_dump(cfg))
    service('restart','redis-gateway-cache'); ready()
    r=Redis()
    try:
        for k in keys: r.call('DEL',k)
    finally: r.close()
print('PASS; default invalidate restored, test business keys removed; cold DB never flushed.')
