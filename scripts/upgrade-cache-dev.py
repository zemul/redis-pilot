#!/usr/bin/env python3
"""Upgrade ONLY the isolated redis03 cache Gateway; never change cold data."""
from pathlib import Path
import subprocess as sp
import sys
import yaml

source=Path(sys.argv[1]).resolve()
config=Path('/etc/redis-gateway-cache/gateway.yaml')
cfg=yaml.safe_load(config.read_text())
assert cfg['mode']=='cache' and cfg['redis_address']=='127.0.0.1:19490'
assert cfg['cache']['cold_address']=='127.0.0.1:19491'
assert source.is_file()
sp.run(['systemctl','stop','redis-gateway-cache'],check=True)
# These settings were removed, not silently ignored or replaced by new caps.
for field in ['max_object_bytes','max_object_elements','types']:
    cfg['cache'].pop(field,None)
config.write_text(yaml.safe_dump(cfg))
sp.run([str(source),'-config',str(config),'-check-config'],check=True)
sp.run(['install','-m','0755',str(source),'/opt/redis-gateway-cache/redis-gateway'],check=True)
sp.run(['systemctl','start','redis-gateway-cache'],check=True)
print('Gateway upgraded; removed per-object cache limits. Cold storage unchanged.')
