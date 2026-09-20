#!/usr/bin/env python3
"""Explicit three-host HA development deployment. Run in a private staging dir.

Creates dedicated ha-* resources only; preserves existing data and runtime state.
Credentials and backups stay in root-only staging, never stdout.
"""
import importlib.util
import json
import pathlib
import socket
import sys
import shutil
import os

ROOT = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('installer', ROOT/'deploy-spec-dev.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)
CLUSTERS = ['ha-redis-dev', 'ha-update-dev', 'ha-invalidate-dev']
installer.CLUSTERS = CLUSTERS
api, run, save = installer.api, installer.run, installer.save


def inventory():
    save(ROOT/'baseline-clusters.json', json.dumps(api('/api/v1/custom-clusters')))
    print('Saved existing cluster baseline (private)')


def details():
    instances = api('/instance/list')
    print(json.dumps({k: v for k, v in instances.get('instances', {}).items() if k.startswith('ha-')}))
    for task in json.loads((ROOT/'tasks.json').read_text()).values():
        print(json.dumps(api('/tasks/'+task)))


def resume_gateways():
    for i, cluster in enumerate(CLUSTERS):
        conf = pathlib.Path('/etc/redis-gateway-'+cluster+'/gateway.yaml')
        if conf.exists():
            # Replay the exact saved credential after a transient API failure;
            # never reinitialize durable state or invent another password.
            ingress = json.loads((ROOT/(cluster+'-ingress.json')).read_text())
            api('/api/v1/custom-clusters/'+cluster+'/ingress', ingress)
            print(cluster, 'existing deployment/ingress confirmed', flush=True)
        else:
            installer.CLUSTERS = [cluster]
            installer.gateway(17683+i, 19604+i)


def patch_server():
    assert socket.gethostname() == 'redis01'
    base = pathlib.Path('/opt/redis-pilot-server/redis-pilot-server')
    suffix = sys.argv[2] if len(sys.argv) > 2 else '1'
    assert suffix.isdigit()
    backup = ROOT/('before-ha-fix-'+suffix+'-server')
    assert not backup.exists()
    shutil.copy2(base, backup)
    nextfile = base.with_suffix('.next')
    shutil.copy2(ROOT/'redis-pilot-server', nextfile)
    os.chmod(nextfile, 0o755)
    run('systemctl', 'stop', 'redis-pilot-server')
    os.replace(nextfile, base)
    run('systemctl', 'start', 'redis-pilot-server')
    print('Server patched; config and pilot.db preserved')


def firewall():
    host = socket.gethostname()
    start = {'redis01': 6383, 'redis02': 6390, 'redis03': 6387}[host]
    for port in range(start, 6400):
        with socket.socket() as probe:
            probe.bind(('0.0.0.0', port))
    addresses = '{ 10.70.100.67, 10.70.100.63, 10.70.100.68 }'
    # Existing earlier test ranges deny replica peers. Add a narrowly scoped
    # exception only for this run's unused ports, keeping earlier DBs protected.
    if host != 'redis01':
        path = pathlib.Path('/etc/redis-pilot-spec-dev.nft')
        previous = path.read_text()
        save(ROOT/'firewall-before.nft', previous)
        rule = f' iifname != "lo" ip saddr {addresses} tcp dport {start}-6399 accept\n'
        assert 'pilot_spec_dev' in previous and rule not in previous
        path.write_text(previous.replace('policy accept;\n', 'policy accept;\n'+rule, 1))
        run('nft', 'insert', 'rule', 'inet', 'pilot_spec_dev', 'inbound',
            'iifname', '!=', 'lo', 'ip', 'saddr', addresses,
            'tcp', 'dport', f'{start}-6399', 'accept')
    nft = '/etc/redis-pilot-ha-dev.nft'
    save(nft, 'table inet pilot_ha_dev {\n chain inbound {\n type filter hook prerouting priority -111; policy accept;\n'
         f' iifname != "lo" tcp dport {start}-6399 ip saddr != {addresses} drop\n'
         f' iifname != "lo" meta nfproto ipv6 tcp dport {start}-6399 drop\n }}\n}}\n')
    run('nft', '-c', '-f', nft)
    save('/etc/systemd/system/redis-pilot-ha-firewall.service',
         '[Unit]\nDescription=Dedicated HA development ports\nBefore=network.target\n'
         '[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/sbin/nft -f '+nft+
         '\n[Install]\nWantedBy=multi-user.target\n', 0o644)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'enable', '--now', 'redis-pilot-ha-firewall')
    print(host, 'HA port access restricted to the three development hosts')


def create():
    tasks = {}
    for cluster in CLUSTERS:
        req = {'id': cluster, 'engine': 'redis' if cluster == CLUSTERS[0] else 'kvrocks',
               'gateway_id': 'gateway-'+cluster, 'node_id': 'redis01',
               'cold': {'count': 2, 'memory': '128Mi' if cluster == CLUSTERS[0] else '256Mi',
                        'cpus': 0.25, 'disk': '1Gi', 'replicas': 1,
                        'servers': ['redis02', 'redis03'], 'replica_servers': [['redis03'], ['redis02']]}}
        if cluster != CLUSTERS[0]:
            req['cache'] = {'memory': '64Mi', 'cpus': 0.25, 'server': 'redis01',
                            'write_policy': 'update_existing' if cluster == CLUSTERS[1] else 'invalidate'}
        save(ROOT/(cluster+'-spec.json'), json.dumps(req))
        result = api('/api/v1/custom-clusters/create', req)
        assert api('/api/v1/custom-clusters/create', req)['task_id'] == result['task_id']
        tasks[cluster] = result['task_id']
        print(cluster, result['task_id'], flush=True)
    save(ROOT/'tasks.json', json.dumps(tasks))


def boot():
    names = run('podman', 'ps', '--format', '{{.Names}}').splitlines()
    names = [n for n in names if n.startswith(('redis-ha-', 'kvrocks-ha-'))]
    assert names and all(c in 'abcdefghijklmnopqrstuvwxyz0123456789- ' for c in ' '.join(names))
    save('/etc/systemd/system/redis-pilot-ha-backends.service',
         '[Unit]\nDescription=Dedicated HA acceptance databases\nRequires=redis-pilot-ha-firewall.service\n'
         'After=redis-pilot-ha-firewall.service redis-pilot-spec-firewall.service network-online.target\n'
         '[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/usr/bin/podman start '+' '.join(names)+
         '\n[Install]\nWantedBy=multi-user.target\n', 0o644)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'enable', '--now', 'redis-pilot-ha-backends')
    print(socket.gethostname(), 'HA backends enabled', len(names))


def bridge():
    host = socket.gethostname()
    assert host in ('redis02', 'redis03')
    peer, start = ('10.70.100.68', 6387) if host == 'redis02' else ('10.70.100.63', 6390)
    # Podman allocates another container IP on restart. The host is trusted;
    # constrain egress by bridge + subnet + exact peer + NEW test port range,
    # not a stale list of per-container addresses. Earlier DB ports stay denied.
    network = json.loads(run('podman', 'network', 'inspect', 'podman'))[0]
    assert network['network_interface'] == 'podman0'
    assert network['subnets'][0]['subnet'] == '10.88.0.0/16'
    ips = '10.88.0.0/16'
    rule = f' iifname "podman0" ip saddr {ips} ip daddr {peer} tcp dport {start}-6399 accept\n'
    for table, path in [('pilot_spec_dev', '/etc/redis-pilot-spec-dev.nft'),
                        ('pilot_ha_dev', '/etc/redis-pilot-ha-dev.nft')]:
        p = pathlib.Path(path)
        previous = p.read_text()
        previous = ''.join(line for line in previous.splitlines(True) if 'iifname "podman0"' not in line)
        for line in run('nft', '-a', 'list', 'chain', 'inet', table, 'inbound').splitlines():
            if 'iifname "podman0"' in line:
                run('nft', 'delete', 'rule', 'inet', table, 'inbound', 'handle', line.rsplit('handle ', 1)[1])
        p.write_text(previous.replace('policy accept;\n', 'policy accept;\n'+rule, 1))
        run('nft', 'insert', 'rule', 'inet', table, 'inbound', 'iifname', 'podman0',
            'ip', 'saddr', ips, 'ip', 'daddr', peer, 'tcp', 'dport', f'{start}-6399', 'accept')
    print(host, 'HA container replication egress permitted')


if __name__ == '__main__':
    actions = {'inventory': inventory, 'firewall': firewall, 'create': create, 'boot': boot, 'details': details, 'bridge': bridge,
               'resume-gateways': resume_gateways,
               'patch-server': patch_server,
               'upgrade-server': lambda: installer.upgrade('server'),
               'upgrade-agent': lambda: installer.upgrade('agent'),
               'status': installer.status, 'gateway': lambda: installer.gateway(17683, 19604)}
    actions[sys.argv[1]]()
