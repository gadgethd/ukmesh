#!/usr/bin/env python3
"""Materialize local endpoint substitutions from the canonical configurations."""
from pathlib import Path
import os
import re
import stat
import tempfile
import yaml

root = Path(__file__).resolve().parents[2]
state = root / 'plans/lean-runtime/runtime'
state.mkdir(parents=True, exist_ok=True, mode=0o700)

def publish(name, text, mode=0o644):
    target = state / name
    if target.is_file() and not target.is_symlink() and stat.S_IMODE(target.stat().st_mode) == mode and target.read_text() == text:
        return
    with tempfile.NamedTemporaryFile(mode='w', dir=state, delete=False) as output:
        temporary = Path(output.name)
        try:
            output.write(text)
            output.flush()
            temporary.chmod(mode)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def worker_token():
    try:
        fd = os.open(state / 'hopreach-worker.token', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return ''
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.geteuid():
            raise ValueError('HopReach worker token must be an owner-only regular file')
        token = stream.read(66)
    if not re.fullmatch(rb'[0-9a-fA-F]{64}\n?', token):
        raise ValueError('HopReach worker token must contain 64 hexadecimal characters')
    return token.decode('ascii').rstrip('\n')

publish('prometheus.yml', (root / 'logging/prometheus.yml').read_text().replace(
    'mosquitto:9001', 'ukmesh-sample-mqtt:9001'))
coverage = yaml.safe_load((root / 'rf-coverage/config.ukmesh.yaml').read_text())
coverage['remote_worker']['token'] = worker_token()
publish('hopreach.yaml', yaml.safe_dump(coverage, sort_keys=False), mode=0o600)

# Local Anubis receives direct TCP clients and replaces X-Real-IP itself;
# Cloudflare's incoming header is not an authority on this fixture ingress.
for name in ('nginx.app.conf', 'nginx.website.conf'):
    publish(name, (root / name).read_text().replace('$http_cf_connecting_ip', '$http_x_real_ip'))
