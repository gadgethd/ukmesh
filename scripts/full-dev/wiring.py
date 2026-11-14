#!/usr/bin/env python3
"""Materialize local endpoint substitutions from the canonical configurations."""
from pathlib import Path
import os
import yaml

root = Path(__file__).resolve().parents[2]
state = root / 'plans/lean-runtime/runtime'
state.mkdir(parents=True, exist_ok=True, mode=0o700)

def publish(name, text):
    target = state / name
    if target.exists() and target.read_text() == text:
        return
    temporary = state / (name + '.next')
    temporary.write_text(text)
    temporary.chmod(0o644)
    os.replace(temporary, target)

publish('prometheus.yml', (root / 'logging/prometheus.yml').read_text().replace(
    'mosquitto:9001', 'ukmesh-sample-mqtt:9001'))
coverage = yaml.safe_load((root / 'rf-coverage/config.ukmesh.yaml').read_text())
# A configured broker enables remote dispatch even when local gpu.mode is cpu.
coverage['gpu']['remote']['broker_addr'] = ''
publish('hopreach.yaml', yaml.safe_dump(coverage, sort_keys=False))

# Local Anubis receives direct TCP clients and replaces X-Real-IP itself;
# Cloudflare's incoming header is not an authority on this fixture ingress.
for name in ('nginx.app.conf', 'nginx.website.conf'):
    publish(name, (root / name).read_text().replace('$http_cf_connecting_ip', '$http_x_real_ip'))
