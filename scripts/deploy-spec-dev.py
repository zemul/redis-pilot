#!/usr/bin/env python3
"""Explicit redis01-03 development acceptance installer; never touches user DB data.

Run from a private staging directory containing the freshly built binaries.
Actions are intentionally separate, leaving failures inspectable rather than
deleting resources or restoring an obsolete control database automatically.
"""
import json
import os
import pathlib
import pwd
import secrets
import shutil
import socket
import sqlite3
import subprocess as sp
import sys
import time
import urllib.request
import yaml

ROOT = pathlib.Path(__file__).resolve().parent
HOST = socket.gethostname()
assert HOST in ('redis01', 'redis02', 'redis03'), 'development hosts only'
assert os.geteuid() == 0
CLUSTERS = ['spec-update', 'spec-invalidate', 'spec-nocache']


def run(*args):
    return sp.run(args, check=True, capture_output=True, text=True).stdout.strip()


def save(path, content, mode=0o600):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'x') as f:
        os.chmod(path, mode)
        f.write(content)


def api(path, body=None):
    cfg = yaml.safe_load(pathlib.Path('/root/.redis-pilot-cli/config.yaml').read_text())
    req = urllib.request.Request('http://127.0.0.1:8080'+path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': 'Bearer '+cfg['token'], 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as r:
        result = json.load(r)
    assert result['success'], result.get('error')
    return result.get('data')


def upgrade(kind):
    assert kind in ('server', 'agent')
    assert kind != 'server' or HOST == 'redis01'
    base = pathlib.Path('/opt/redis-pilot-'+kind)
    binary = 'redis-pilot-'+kind
    backup = ROOT / ('backup-'+kind)
    backup.mkdir(mode=0o700)
    shutil.copy2(base/binary, backup/binary)
    shutil.copy2(base/(kind+'.yaml'), backup/(kind+'.yaml'))
    if kind == 'server':
        cfg = yaml.safe_load((base/'server.yaml').read_text())
        dbfile = pathlib.Path(cfg['data_dir'])/'pilot.db'
        with sqlite3.connect(dbfile) as src, sqlite3.connect(backup/'pilot.db') as dst:
            src.backup(dst)
        for cluster in CLUSTERS:
            assert 'gateway-'+cluster not in cfg.get('gateway_tokens', {}), 'already installed'
            cfg.setdefault('gateway_tokens', {})['gateway-'+cluster] = secrets.token_urlsafe(32)
        save(ROOT/'server-next.yaml', yaml.safe_dump(cfg))
    run('systemctl', 'stop', 'redis-pilot-'+kind)
    shutil.copy2(ROOT/binary, base/(binary+'.next'))
    os.chmod(base/(binary+'.next'), 0o755)
    os.replace(base/(binary+'.next'), base/binary)
    if kind == 'server':
        shutil.copy2(ROOT/'server-next.yaml', base/'server.yaml')
    run('systemctl', 'start', 'redis-pilot-'+kind)
    assert run('systemctl', 'is-active', 'redis-pilot-'+kind) == 'active'
    print(HOST, kind, 'upgraded; backup', backup, flush=True)


def firewall():
    assert HOST in ('redis02', 'redis03')
    start = 6385 if HOST == 'redis02' else 6383
    # These previously unused ports are reserved for this acceptance only.
    for port in range(start, 6400):
        with socket.socket() as probe:
            probe.bind(('0.0.0.0', port))
    nft = pathlib.Path('/etc/redis-pilot-spec-dev.nft')
    save(nft, 'table inet pilot_spec_dev {\n chain inbound {\n type filter hook prerouting priority -110; policy accept;\n'
         f' iifname != "lo" tcp dport {start}-6399 ip saddr != 10.70.100.67 drop\n'
         f' iifname != "lo" meta nfproto ipv6 tcp dport {start}-6399 drop\n }}\n}}\n')
    run('nft', '-c', '-f', str(nft))
    save('/etc/systemd/system/redis-pilot-spec-firewall.service', '[Unit]\nDescription=Dedicated spec acceptance backend access\nBefore=network.target\n[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/sbin/nft -f /etc/redis-pilot-spec-dev.nft\n[Install]\nWantedBy=multi-user.target\n', 0o644)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'enable', '--now', 'redis-pilot-spec-firewall')
    print(HOST, 'restricted test backend ports', start, 'through 6399', flush=True)


def create():
    assert HOST == 'redis01'
    existing = api('/api/v1/custom-clusters')
    assert not any(c['id'] in CLUSTERS for c in existing), 'test cluster already exists'
    tasks = {}
    for cluster in CLUSTERS:
        req = {'id': cluster, 'gateway_id': 'gateway-'+cluster, 'node_id': 'redis01',
               'cold': {'count': 2 if cluster == 'spec-update' else 1, 'memory': '256Mi', 'cpus': 0.25, 'disk': '1Gi',
                        'servers': ['redis02', 'redis03'] if cluster == 'spec-update' else ['redis03']}}
        if cluster != 'spec-nocache':
            req['cache'] = {'memory': '128Mi', 'cpus': 0.25, 'server': 'redis02',
                            'write_policy': 'update_existing' if cluster == 'spec-update' else 'invalidate'}
        save(ROOT/(cluster+'-spec.json'), json.dumps(req, indent=2))
        result = api('/api/v1/custom-clusters/create', req)
        replay = api('/api/v1/custom-clusters/create', req)
        assert result['task_id'] == replay['task_id']
        tasks[cluster] = result['task_id']
        print(cluster, 'submitted', result['task_id'], flush=True)
    save(ROOT/'tasks.json', json.dumps(tasks))


def status():
    for cluster, task in json.loads((ROOT/'tasks.json').read_text()).items():
        state = api('/tasks/'+task)
        print(cluster, json.dumps({k: state.get(k) for k in ('status', 'phase', 'error', 'error_message')}), flush=True)


def gateway(port_base=17679, admin_base=19600):
    assert HOST == 'redis01'
    account = pwd.getpwnam('redis-gateway')
    cfg = yaml.safe_load(pathlib.Path('/opt/redis-pilot-server/server.yaml').read_text())
    for i, cluster in enumerate(CLUSTERS):
        db = api('/api/v1/custom-clusters/'+cluster)
        assert db['revision'] == 1 and not db.get('task') and not db.get('ingress')
        port, admin = port_base+i, admin_base+i
        for p in (port, admin):
            with socket.socket() as probe: probe.bind(('127.0.0.1', p))
        conf = pathlib.Path('/etc/redis-gateway-'+cluster)
        state = pathlib.Path('/var/lib/redis-gateway-'+cluster)
        binarydir = pathlib.Path('/opt/redis-gateway-'+cluster)
        for path in (conf, state, binarydir): path.mkdir(mode=0o700)
        token = conf/'control.secret'
        save(token, cfg['gateway_tokens']['gateway-'+cluster]+'\n')
        req = {'server_endpoint': 'http://127.0.0.1:8080', 'listen': f'127.0.0.1:{port}',
               'admin_listen': f'127.0.0.1:{admin}', 'state_dir': str(state), 'token_file': str(token)}
        export = api('/api/v1/custom-clusters/'+cluster+'/gateway-config', req)
        save(conf/'gateway.yaml', export['yaml'])
        shutil.copy2(ROOT/'redis-gateway', binarydir/'redis-gateway')
        os.chmod(binarydir/'redis-gateway', 0o755)
        for path in (conf, state, binarydir, token, conf/'gateway.yaml'):
            os.chown(path, account.pw_uid, account.pw_gid)
        command = [str(binarydir/'redis-gateway'), '-config', str(conf/'gateway.yaml')]
        run('runuser', '-u', 'redis-gateway', '--', *command, '-check-config')
        run('runuser', '-u', 'redis-gateway', '--', *command, '-init-sharding-state')
        unit = f'''[Unit]
Description=Redis Pilot {cluster} acceptance gateway
After=network-online.target redis-pilot-server.service
Wants=network-online.target
[Service]
User=redis-gateway
Group=redis-gateway
ExecStart={' '.join(command)}
Restart=on-failure
RestartSec=3
TimeoutStopSec=20
UMask=0077
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths={state}
[Install]
WantedBy=multi-user.target
'''
        save('/etc/systemd/system/redis-gateway-'+cluster+'.service', unit, 0o644)
        run('systemctl', 'daemon-reload')
        run('systemctl', 'enable', '--now', 'redis-gateway-'+cluster)
        deadline = time.monotonic()+45
        while True:
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{admin}/readyz', timeout=2) as r: assert r.status == 200
                break
            except Exception:
                if time.monotonic() >= deadline: raise
                time.sleep(1)
        ingress = {'gateway_address': req['listen'], 'password': secrets.token_urlsafe(24)}
        save(ROOT/(cluster+'-ingress.json'), json.dumps(ingress))
        api('/api/v1/custom-clusters/'+cluster+'/ingress', ingress)
        print(cluster, 'gateway ready, ingress requested', flush=True)


def backend_boot():
    assert HOST in ('redis02', 'redis03')
    names = (['kvrocks-spec-update-cold-1', 'redis-spec-update-hot', 'redis-spec-invalidate-hot']
             if HOST == 'redis02' else ['kvrocks-spec-update-cold-2', 'kvrocks-spec-invalidate-cold-1', 'kvrocks-spec-nocache-cold-1'])
    for name in names:
        inspected = json.loads(run('podman', 'inspect', name))[0]
        assert inspected['State']['Running'] and inspected['Config']['Labels']['redis-pilot.provision']
    save('/etc/systemd/system/redis-pilot-spec-backends.service',
         '[Unit]\nDescription=Start existing dedicated spec acceptance databases\n'
         'Requires=redis-pilot-spec-firewall.service\nAfter=redis-pilot-spec-firewall.service network-online.target\n'
         '[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/bin/podman start '+' '.join(names)+
         '\n[Install]\nWantedBy=multi-user.target\n', 0o644)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'enable', '--now', 'redis-pilot-spec-backends')
    print(HOST, 'dedicated backend boot service enabled', flush=True)


if __name__ == '__main__':
    actions = {'upgrade-server': lambda: upgrade('server'), 'upgrade-agent': lambda: upgrade('agent'),
               'firewall': firewall, 'create': create, 'status': status, 'gateway': gateway, 'backend-boot': backend_boot}
    actions[sys.argv[1]]()
