#!/usr/bin/env python3
"""Explicit isolated Redis sharding acceptance on redis01-03 only.

Copy alongside deploy-spec-dev.py and release binaries into a private stage.
Credentials stay in root-only files. Never flush or modify existing clusters.
Actions intentionally stop on errors rather than rollback control-plane data.
"""
import importlib.util
import json
import pathlib
import socket
import sys

ROOT = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('installer', ROOT/'deploy-spec-dev.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)
CLUSTER = 'redis-shards-dev'
installer.CLUSTERS = [CLUSTER]
api, run, save = installer.api, installer.run, installer.save


def inventory():
    assert socket.gethostname() == 'redis01'
    clusters = api('/api/v1/custom-clusters')
    print(json.dumps([{'id': c['id'], 'engine': c.get('engine'), 'revision': c['revision'],
                       'ready': (c.get('ingress') or {}).get('ready')} for c in clusters]))
    save(ROOT/'baseline-clusters.json', json.dumps(clusters))


def create():
    assert socket.gethostname() == 'redis01'
    assert not any(c['id'] == CLUSTER for c in api('/api/v1/custom-clusters'))
    req = {'id': CLUSTER, 'engine': 'redis', 'gateway_id': 'gateway-'+CLUSTER,
           'node_id': 'redis01', 'cold': {'count': 2, 'memory': '128Mi', 'cpus': 0.25,
           'disk': '1Gi', 'servers': ['redis02', 'redis03']}}
    save(ROOT/(CLUSTER+'-spec.json'), json.dumps(req))
    result = api('/api/v1/custom-clusters/create', req)
    replay = api('/api/v1/custom-clusters/create', req)
    assert result['task_id'] == replay['task_id']
    save(ROOT/'tasks.json', json.dumps({CLUSTER: result['task_id']}))
    print(CLUSTER, 'creation task', result['task_id'], flush=True)


def target():
    assert socket.gethostname() == 'redis01'
    req = {'name': CLUSTER+'-extra', 'group': CLUSTER+'-extra', 'engine': 'redis',
           'type': 'standalone', 'profile': 'redis-storage', 'category': 'persistent',
           'memory': '128Mi', 'cpus': 0.25, 'disk': '1Gi', 'server': 'redis02', 'disable_proxy': True}
    result = api('/instance/create', req)
    save(ROOT/'target-result.json', json.dumps(result))
    print('extra target creation:', json.dumps(result), flush=True)


def backend_boot():
    host = socket.gethostname()
    assert host in ('redis02', 'redis03')
    names = ['redis-'+CLUSTER+'-cold-'+('1' if host == 'redis02' else '2')]
    if host == 'redis02': names.append('redis-'+CLUSTER+'-extra')
    for name in names:
        inspected = json.loads(run('podman', 'inspect', name))[0]
        assert inspected['State']['Running'] and inspected['Config']['Labels']['redis-pilot.provision']
    save('/etc/systemd/system/redis-shards-dev-backends.service',
         '[Unit]\nDescription=Redis sharding development databases\n'
         'Requires=redis-pilot-spec-firewall.service\nAfter=redis-pilot-spec-firewall.service network-online.target\n'
         '[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/bin/podman start '+' '.join(names)+
         '\n[Install]\nWantedBy=multi-user.target\n', 0o644)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'enable', '--now', 'redis-shards-dev-backends')
    print(host, 'backend boot enabled')


if __name__ == '__main__':
    actions = {'inventory': inventory, 'upgrade-server': lambda: installer.upgrade('server'),
               'upgrade-agent': lambda: installer.upgrade('agent'), 'create': create,
               'status': installer.status, 'gateway': lambda: installer.gateway(17682, 19603),
               'target': target, 'backend-boot': backend_boot}
    actions[sys.argv[1]]()
