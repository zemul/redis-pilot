#!/usr/bin/env python3
"""Run as root on redis03 with gateway/proxy binaries and examples in staging.

Creates a NEW isolated stack only; refuses existing installation/container names.
Does not manage Agent, discovery Proxy, or any old database volumes.
"""
import os
from pathlib import Path
import pwd
import secrets
import socket
import subprocess as sp
import sys
import time
import urllib.request
import yaml

stage = Path(sys.argv[1]).resolve()
base = Path('/etc/redis-gateway-cache')
binary = Path('/opt/redis-gateway-cache')
assert os.geteuid() == 0
assert not base.exists() and not binary.exists(), 'installation already exists'
for name in ['pilot-cache-dev-hot', 'pilot-cache-dev-cold']:
    assert sp.run(['podman', 'container', 'exists', name]).returncode != 0
for port in [17479, 17480, 19400, 19401, 19490, 19491]:
    with socket.socket() as s:
        s.bind(('127.0.0.1', port))
owner = pwd.getpwnam('redis-gateway')
os.umask(0o077)
base.mkdir(mode=0o750)
base.chmod(0o750)
os.chown(base, 0, owner.pw_gid)
binary.mkdir(mode=0o755)
binary.chmod(0o755)

def config(name, content, shared=False):
    p = base / name
    p.write_text(content)
    p.chmod(0o640 if shared else 0o600)
    os.chown(p, 0, owner.pw_gid)

passwords = {n: secrets.token_hex(32) for n in ['frontend', 'hot', 'cold']}
for n, value in passwords.items():
    config(n + '.secret', value + '\n', True)
g = yaml.safe_load((stage / 'gateway-cache.yaml').read_text())
config('gateway.yaml', yaml.safe_dump(g), True)
config('proxy.yaml', yaml.safe_dump(dict(listen='127.0.0.1:17480', master='127.0.0.1:17479', password=passwords['frontend'], pool_size=4, idle_timeout='30s', metrics_listen='127.0.0.1:19401', max_request_bytes=262144)), True)
config('hot.conf', 'bind 127.0.0.1\nport 19490\nprotected-mode yes\nrequirepass '+passwords['hot']+'\nmaxmemory 32mb\nmaxmemory-policy allkeys-lru\nappendonly no\nsave ""\n', True)
# Configuration is mounted read-only; cold data lives in its own named volume.
config('cold.conf', 'bind 127.0.0.1\nport 19491\ndir /data\nrequirepass '+passwords['cold']+'\nworkers 2\nrocksdb.block_cache_size 32\nrocksdb.write_buffer_size 8\n', True)
for name in ['redis-gateway', 'redis-proxy']:
    sp.run(['install', '-m', '0755', str(stage/name), str(binary/name)], check=True)
for name, image, command, conf, memory in [
    ('hot','docker.io/library/redis:7.2.5',['redis-server','/etc/pilot.conf'],'hot.conf','128m'),
    ('cold','docker.io/apache/kvrocks:2.15.0',['kvrocks','-c','/etc/pilot.conf'],'cold.conf','512m')]:
    args=['podman','create','--name','pilot-cache-dev-'+name,'--network','host','--user','0:0','--memory',memory,'-v',str(base/conf)+':/etc/pilot.conf:ro']
    if name=='cold': args += ['-v','pilot-cache-dev-cold-data:/data']
    sp.run(args+['--entrypoint',command[0],image]+command[1:],check=True)
    unit='''[Unit]
Description=Redis Pilot isolated cache development BACKEND
After=network.target
[Service]
ExecStart=/usr/bin/podman start --attach pilot-cache-dev-BACKEND
ExecStop=/usr/bin/podman stop --time 10 pilot-cache-dev-BACKEND
Restart=on-failure
RestartSec=3
TimeoutStopSec=20
[Install]
WantedBy=multi-user.target
'''.replace('BACKEND',name)
    Path('/etc/systemd/system/redis-cache-dev-'+name+'.service').write_text(unit)
unit=(stage/'redis-gateway-cache.service').read_text().replace('After=network.target','Wants=redis-cache-dev-hot.service redis-cache-dev-cold.service\nAfter=network.target redis-cache-dev-hot.service redis-cache-dev-cold.service')
Path('/etc/systemd/system/redis-gateway-cache.service').write_text(unit)
Path('/etc/systemd/system/redis-proxy-cache.service').write_text('''[Unit]
Description=Redis Pilot isolated cache development Proxy
Wants=redis-gateway-cache.service
After=network.target redis-gateway-cache.service
[Service]
User=redis-gateway
Group=redis-gateway
ExecStart=/opt/redis-gateway-cache/redis-proxy -config /etc/redis-gateway-cache/proxy.yaml
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
[Install]
WantedBy=multi-user.target
''')
sp.run(['systemctl','daemon-reload'],check=True)
sp.run(['systemctl','enable','--now','redis-cache-dev-hot','redis-cache-dev-cold','redis-gateway-cache','redis-proxy-cache'],check=True)
for attempt in range(60):
    try:
        with urllib.request.urlopen('http://127.0.0.1:19400/readyz', timeout=1) as response:
            assert response.status == 200
        break
    except (OSError, AssertionError):
        if attempt == 59: raise
        time.sleep(1)
for name in ['redis-cache-dev-hot','redis-cache-dev-cold','redis-gateway-cache','redis-proxy-cache']:
    sp.run(['systemctl','is-active','--quiet',name],check=True)
print('Isolated stack installed; credentials remain in root-owned configuration files.')
