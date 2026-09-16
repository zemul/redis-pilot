#!/usr/bin/env python3
"""Bounded, low-load dev soak; no service restarts or topology changes.

Run on redis01: python3 soak-managed-dev.py /private/path/ingress.json
Uses only random-prefix keys. This is not a throughput benchmark.
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


class Redis:
    def __init__(self, password):
        self.sock = socket.create_connection(('127.0.0.1', 16508), timeout=5)
        self.file = self.sock.makefile('rb')
        assert self.call('AUTH', password) == b'OK'

    def read(self):
        line = self.file.readline()
        if not line.endswith(b'\r\n'):
            raise RuntimeError('truncated reply')
        tag, value = line[:1], line[1:-2]
        if tag == b'+':
            return value
        if tag == b':':
            return int(value)
        if tag == b'$':
            n = int(value)
            if n == -1:
                return None
            data = self.file.read(n + 2)
            assert len(data) == n + 2 and data[-2:] == b'\r\n'
            return data[:-2]
        raise RuntimeError('backend error: ' + line.decode(errors='replace'))

    def call(self, *args):
        values = [str(arg).encode() for arg in args]
        self.sock.sendall(b'*%d\r\n' % len(values) + b''.join(
            b'$%d\r\n' % len(v) + v + b'\r\n' for v in values))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def main():
    assert socket.gethostname() == 'redis01'
    cfg = yaml.safe_load(pathlib.Path('/etc/redis-gateway/gateway.yaml').read_text())
    assert cfg['service_id'] == 'managed-dev'
    ingress = json.loads(pathlib.Path(sys.argv[1]).read_text())
    assert ingress['gateway_address'] == '127.0.0.1:17379'
    prefix = 'managed-soak-' + secrets.token_hex(8) + ':'
    deadline = time.monotonic() + 600
    results = [{'cycles': 0, 'errors': 0, 'mismatches': 0, 'ms': []} for _ in range(2)]
    keys = [prefix + str(i) + suffix for i in range(2) for suffix in (':s', ':h')]

    def worker(i):
        c = None
        r = results[i]
        seq = 0
        while time.monotonic() < deadline:
            try:
                if c is None:
                    c = Redis(ingress['password'])
                seq += 1
                value = str(seq)
                started = time.monotonic()
                assert c.call('SET', keys[i * 2], value, 'EX', 900) == b'OK'
                if c.call('GET', keys[i * 2]) != value.encode():
                    r['mismatches'] += 1
                c.call('HSET', keys[i * 2 + 1], 'counter', value)
                c.call('EXPIRE', keys[i * 2 + 1], 900)
                if c.call('HGET', keys[i * 2 + 1], 'counter') != value.encode():
                    r['mismatches'] += 1
                r['ms'].append((time.monotonic() - started) * 1000)
                r['cycles'] += 1
            except Exception as exc:
                r['errors'] += 1
                print(json.dumps({'worker': i, 'error': str(exc)}), flush=True)
                if c:
                    c.close()
                    c = None
            time.sleep(0.1)
        if c:
            c.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    print(json.dumps({'prefix': prefix, 'seconds': 600, 'policy': cfg['cache']['write_policy']}), flush=True)
    for t in threads:
        t.start()
    health_errors = 0
    try:
        while any(t.is_alive() for t in threads):
            resources = {}
            for unit in ('redis-gateway', 'redis-pilot-proxy', 'redis-pilot-server'):
                pid = subprocess.check_output(['systemctl', 'show', unit, '-p', 'MainPID', '--value'], text=True).strip()
                status = pathlib.Path('/proc/' + pid + '/status').read_text()
                resources[unit] = next(line for line in status.splitlines() if line.startswith('VmRSS:'))
            try:
                with urllib.request.urlopen('http://127.0.0.1:19200/readyz', timeout=3) as response:
                    assert response.status == 200
            except Exception:
                health_errors += 1
            print(json.dumps({'remaining_s': max(0, round(deadline - time.monotonic())), 'cycles': sum(r['cycles'] for r in results), 'errors': sum(r['errors'] for r in results), 'mismatches': sum(r['mismatches'] for r in results), 'health_errors': health_errors, 'rss': resources}), flush=True)
            threads[0].join(timeout=30)
        for t in threads:
            t.join()
    finally:
        deadline = time.monotonic()
        for t in threads:
            t.join()
        c = Redis(ingress['password'])
        try:
            for key in keys:
                c.call('DEL', key)
        finally:
            c.close()
    samples = sorted(ms for r in results for ms in r['ms'])
    summary = {'cycles': sum(r['cycles'] for r in results), 'errors': sum(r['errors'] for r in results), 'mismatches': sum(r['mismatches'] for r in results), 'health_errors': health_errors, 'cleaned_keys': len(keys)}
    summary['cycle_ms'] = {label: samples[min(len(samples)-1, int(len(samples)*q))] if samples else None for label, q in [('p50', .5), ('p95', .95), ('p99', .99)]}
    print(json.dumps({'final': summary}), flush=True)
    assert samples and not (summary['errors'] or summary['mismatches'] or health_errors), summary


if __name__ == '__main__':
    main()
