#!/usr/bin/env python3
"""Run checkout probes with live credentials only in child memory, read-only.

Usage: python3 scripts/verify-health-latency.py mqtt|spam|analysis
No migrations, leases, persistence, or service startup is called by the probe.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

if len(sys.argv) != 2 or sys.argv[1] not in ('mqtt', 'spam', 'analysis'):
    sys.exit('usage: verify-health-latency.py mqtt|spam|analysis')
root = Path(__file__).resolve().parent.parent
try:
    container = os.environ.get('HEALTH_AUDIT_BACKEND_CONTAINER', 'meshcore-analytics-backend-1')
    config = json.loads(subprocess.check_output(['docker', 'inspect', container], stderr=subprocess.DEVNULL))[0]
    live = dict(entry.split('=', 1) for entry in config['Config']['Env'])
    url = urlsplit(live['DATABASE_URL'])
    # Preserve userinfo without printing it; access the existing host DB port.
    userinfo = url.netloc.rsplit('@', 1)[0]
    options = dict(parse_qsl(url.query))
    options['options'] = '-c default_transaction_read_only=on -c statement_timeout=30000 -c idle_in_transaction_session_timeout=15000'
    child_env = {k: v for k, v in os.environ.items() if not k.startswith(('DATABASE_', 'PG'))}
    child_env['DATABASE_URL'] = urlunsplit((url.scheme, userinfo + '@127.0.0.1:5432', url.path, urlencode(options), ''))
    child_env['DATABASE_APPLICATION_NAME'] = 'health-latency-readonly-verifier'
    child_env['DATABASE_POOL_MAX'] = '1'
    child_env.update({k: v for k, v in live.items() if k.startswith('SPAM_MESSAGE_')})
    if sys.argv[1] == 'spam' and child_env.get('HEALTH_AUDIT_COMPARE_SPAM') == '1':
        # Reproducible old/new comparison on the very same live input in memory.
        # Generated source stays inside this worktree's ignored audit directory.
        baseline_dir = root / '.health-latency-local' / 'spam-before'
        baseline_dir.mkdir(parents=True, exist_ok=True)
        for name in ('cluster', 'similarity'):
            source = subprocess.check_output([
                'git', 'show', f'1d86e043a515d3c2ca381660866ff03eb2833a0a:backend/src/spam/{name}.ts',
            ], cwd=root, text=True)
            for dependency in ('config', 'normalize', 'types'):
                source = source.replace(f"'./{dependency}.js'", json.dumps(str(root / f'backend/src/spam/{dependency}.ts')))
            source = source.replace("'./similarity.js'", "'./similarity.ts'")
            (baseline_dir / f'{name}.ts').write_text(source)
        (baseline_dir / 'package.json').write_text('{"type":"module"}\n')
    result = subprocess.run(['node', '--import', 'tsx', 'src/tools/verifyHealthLatency.ts', sys.argv[1]],
                            cwd=root / 'backend', env=child_env)
    sys.exit(result.returncode)
except Exception:
    sys.exit('Read-only probe setup failed; check Docker access and the host DB port.')
