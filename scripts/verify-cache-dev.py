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
import hashlib
import sys
import yaml

base = Path('/etc/redis-gateway-cache')
passwords = {n: (base/(n+'.secret')).read_text().strip() for n in ['frontend','hot','cold']}
prefix = 'acceptance:' + uuid.uuid4().hex + ':'
keys = set()
commands_only = '--commands-only' in sys.argv[1:]

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

def compatibility(r, policy):
    """Exercise expanded commands through the actual Proxy, in both policies."""
    a,b=key(policy+'-multi-a'),key(policy+'-multi-b')
    assert r.call('MSET',a,'old',b,'old')=='OK'
    assert r.call('MGET',a,b,a)==['old','old','old']
    assert r.call('MGET',a,b)==['old','old']
    assert r.call('MSETNX',a,'ignored',b,'ignored')==0
    assert r.call('MSET',b,'new',a,'first',a,'new')=='OK'
    if policy=='update_existing':
        h=Redis(19490,passwords['hot'])
        try: assert h.call('MGET',a,b)==['new','new']
        finally: h.close()
    assert r.call('MGET',a,b)==['new','new']
    assert r.call('EXISTS',a,b,a)==3
    assert r.call('UNLINK',a,b,a)==2
    assert r.call('MSETNX',a,'1',b,'2')==1
    r.send('MGET',b,a,b); r.send('DEL',a,b)
    assert r.read()==['2','1','2'] and r.read()==2
    obj=key(policy+'-compat-string')
    assert r.call('SET',obj,'OK')=='OK'
    assert r.call('GET',obj)=='OK'
    assert r.call('SET',obj,'new','GET')=='OK'  # old bulk value, not a status
    assert r.call('SET',obj,'ignored','NX','GET')=='new'
    assert r.call('GET',obj)=='new'
    assert r.call('GETDEL',obj)=='new'
    assert r.call('SET',obj,'1','GET') is None
    assert r.call('INCRBYFLOAT',obj,'.5')=='1.5'
    assert r.call('GET',obj)=='1.5'
    assert r.call('EXPIRE',obj,60,'NX')==1
    assert r.call('EXPIRE',obj,30,'GT')==0
    assert r.call('GETEX',obj,'PERSIST')=='1.5'
    assert r.call('PTTL',obj)==-1
    obj=key(policy+'-compat-hash')
    assert r.call('HMSET',obj,'f','1.25','keep','untouched')=='OK'
    assert r.call('HLEN',obj)==2
    assert r.call('HSETNX',obj,'f','ignored')==0
    assert r.call('HINCRBYFLOAT',obj,'f','.5')=='1.75'
    if policy=='update_existing':
        h=Redis(19490,passwords['hot'])
        try: assert h.call('HMGET',obj,'f','keep')==['1.75','untouched']
        finally: h.close()
    assert r.call('HMGET',obj,'f','keep')==['1.75','untouched']
    assert r.call('HSTRLEN',obj,'f')==4
    assert r.call('HSCAN',obj,0,'MATCH','f*','COUNT',100)[1]==['f','1.75']
    obj=key(policy+'-compat-list')
    assert r.call('RPUSH',obj,'a','b','c','a')==4
    assert r.call('LRANGE',obj,0,-1)==['a','b','c','a']
    assert r.call('LINSERT',obj,'BEFORE','b','x')==5
    assert r.call('LPOS',obj,'a','COUNT',0)==[0,4]
    assert r.call('LPOP',obj,2)==['a','x']
    assert r.call('RPOP',obj,2)==['a','c']
    assert r.call('LRANGE',obj,0,-1)==['b']
    obj=key(policy+'-compat-set')
    assert r.call('SADD',obj,'a','b','c')==3
    assert sorted(r.call('SMEMBERS',obj))==['a','b','c']
    popped=r.call('SPOP',obj,2)
    assert len(set(popped))==2
    remaining=sorted(set(['a','b','c'])-set(popped))
    assert sorted(r.call('SMEMBERS',obj))==remaining
    assert r.call('SRANDMEMBER',obj)==remaining[0]
    obj=key(policy+'-compat-zset')
    assert r.call('ZADD',obj,1,'a',2,'b',3,'c')==3
    assert r.call('ZRANGE',obj,0,-1)==['a','b','c']
    assert r.call('ZADD',obj,'NX',9,'a',4,'d')==1
    assert r.call('ZADD',obj,'XX','INCR',.5,'a')=='1.5'
    assert r.call('ZMSCORE',obj,'a','b','missing')==['1.5','2',None]
    assert r.call('ZPOPMAX',obj,2)==['d','4','c','3']
    assert r.call('ZRANGEBYSCORE',obj,'-inf','+inf','WITHSCORES')==['a','1.5','b','2']
    assert r.call('ZREMRANGEBYSCORE',obj,'(1.5','+inf')==1
    assert r.call('ZRANDMEMBER',obj)=='a'

def advanced(r, policy):
    """Cross-key operations and connection-affine transactions via real Proxy."""
    h,d=key(policy+'-advanced-hash'),key(policy+'-advanced-renamed')
    assert r.call('HSET',h,'f','1','keep','untouched')==2
    assert r.call('HGET',h,'keep')=='untouched'
    assert r.call('MULTI')=='OK'
    for command in [('HSET',h,'f','2'),('PING',),('ECHO','queued'),('UNWATCH',)]:
        assert r.call(*command)=='QUEUED'
    assert r.call('EXEC')==[0,'PONG','queued','OK']
    script="redis.call('HINCRBY',KEYS[1],'f',ARGV[1]);return redis.call('HGET',KEYS[1],'f')"
    sha=hashlib.sha1(script.encode()).hexdigest()
    assert r.call('SCRIPT','LOAD',script)==sha
    assert r.call('SCRIPT','EXISTS',sha)==[1]
    assert r.call('EVALSHA',sha,1,h,3)=='5'
    if policy=='update_existing':
        hot=Redis(19490,passwords['hot'])
        try: assert hot.call('HMGET',h,'f','keep')==['5','untouched']
        finally: hot.close()
    assert r.call('RENAME',h,d)=='OK'
    assert r.call('HGET',d,'keep')=='untouched'
    assert r.call('WATCH',d)=='OK'
    other=Redis()
    try: assert other.call('HSET',d,'f','new')==0
    finally: other.close()
    assert r.call('MULTI')=='OK'
    assert r.call('HSET',d,'f','wrong')=='QUEUED'
    assert r.call('EXEC') is None
    assert r.call('HGET',d,'f')=='new'
    try:
        r.call('EVAL',"redis.call('HSET',KEYS[1],'f','after-error');error('intentional')",1,d)
        raise AssertionError('expected Lua runtime error')
    except RuntimeError as e: assert 'intentional' in str(e)
    assert r.call('HGET',d,'f')=='after-error'
    # A Proxy-local rejection must also poison the cold transaction queue.
    for rejected in [('KEYS','*'),('SUBSCRIBE','not-a-real-subscription')]:
        assert r.call('MULTI')=='OK'
        assert r.call('HSET',d,'f','must-not-commit')=='QUEUED'
        try: r.call(*rejected); raise AssertionError('expected Proxy rejection')
        except RuntimeError: pass
        try: r.call('EXEC'); raise AssertionError('expected EXECABORT')
        except RuntimeError as e: assert 'EXECABORT' in str(e)
        assert r.call('HGET',d,'f')=='after-error'
    a,b,c=[key(policy+'-advanced-'+n) for n in ['a','b','c']]
    assert r.call('SADD',a,'a','b')==2
    assert r.call('SADD',b,'b','c')==2
    assert sorted(r.call('SUNION',a,b))==['a','b','c']
    assert r.call('SINTER',a,b)==['b']
    assert r.call('SDIFF',a,b)==['a']
    assert r.call('SUNIONSTORE',c,a,b)==3
    assert r.call('SMOVE',a,b,'a')==1
    assert r.call('SCARD',b)==3
    assert r.call('DEL',a,b,c)==3
    assert r.call('RPUSH',a,'a','b','c')==3
    assert r.call('RPUSH',b,'x')==1
    assert r.call('LRANGE',a,0,-1)==['a','b','c']
    assert r.call('LRANGE',b,0,-1)==['x']
    assert r.call('LMOVE',a,b,'LEFT','RIGHT')=='a'
    assert r.call('RPOPLPUSH',a,b)=='c'
    assert r.call('LRANGE',b,0,-1)==['c','x','a']
    cursor='0'; found=set()
    for _ in range(1000):
        cursor,page=r.call('SCAN',cursor,'MATCH',prefix+'*','COUNT',20)
        assert all(not k.startswith('__pilot_cache_v1__:') for k in page)
        found.update(page)
        if cursor=='0': break
    else: raise AssertionError('SCAN did not finish')
    assert d in found

def run(policy):
    path=base/'gateway.yaml'
    cfg=yaml.safe_load(path.read_text()); cfg['cache']['write_policy']=policy
    path.write_text(yaml.safe_dump(cfg)); service('restart','redis-gateway-cache'); ready()
    r=Redis()
    try:
        compatibility(r,policy)
        advanced(r,policy)

        if commands_only:
            print(policy+': command matrix, transactions, scripts, cross-key operations passed',flush=True)
            return
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
    if not commands_only:
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
