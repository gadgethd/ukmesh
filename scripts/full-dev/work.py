#!/usr/bin/env python3
"""Native service work oracles. Failures remain failures in the parent report."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import sys
import time
import uuid
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent / 'qualification'))
from common import docker, safe_error, sql
from fixtures import current_targets


def request(url, *, data=None, auth=False, user_agent='UKMesh-owned-work-proof'):
    headers = {'User-Agent': user_agent}
    if auth:
        headers['Authorization'] = 'Basic ' + base64.b64encode(b'admin:local-full-grafana-only').decode()
    if data is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(data).encode()
    with urlopen(Request(url, data=data, headers=headers), timeout=20) as response:
        body = response.read(4 * 1024 * 1024 + 1)
        assert len(body) <= 4 * 1024 * 1024, 'work response exceeded its bound'
        return json.loads(body) if body.strip() else None


def prometheus(expression):
    result = request('http://127.0.0.1:9090/api/v1/query?' + urlencode({'query': expression}))
    assert result['status'] == 'success' and result['data']['result'], 'metric has no samples'
    values = [float(sample['value'][1]) for sample in result['data']['result']]
    assert all(value > 0 for value in values), 'native metric does not demonstrate work'
    return {'samples': len(values)}


def logs():
    result = request('http://127.0.0.1:3104/loki/api/v1/query_range?' + urlencode({
        'query': '{project="ukmesh-full",service="backend"}', 'limit': 10,
        'start': str((time.time_ns() - 900 * 1_000_000_000)), 'end': str(time.time_ns()),
    }))
    assert result['status'] == 'success'
    count = sum(len(stream.get('values', [])) for stream in result['data']['result'])
    assert count > 0, 'no actual backend journal entries reached Loki'
    return {'entries': count}


def check(name):
    metrics = {
        'postgres-exporter': 'pg_up{job="postgres"}',
        'redis-exporter': 'redis_up{job="redis"}',
        'node-exporter': 'node_memory_MemAvailable_bytes{job="node"}',
        'blackbox-exporter': 'probe_success{job="mosquitto-broker"}',
    }
    if name in metrics:
        return prometheus(metrics[name])
    if name in ('loki', 'alloy'):
        return logs()
    if name == 'mosquitto-reloader':
        reload = Request('http://127.0.0.1:3109/reload', data=b'', method='POST', headers={
            'Authorization': 'Bearer local-only-development-secret-at-least-32-characters'})
        with urlopen(reload, timeout=10) as response:
            assert response.status == 204, 'Broker reload was not acknowledged'
        return {'broker_reload_acknowledged': True}
    if name == 'grafana':
        for uid in ('prometheus', 'loki-main'):
            result = request(f'http://127.0.0.1:3010/api/datasources/uid/{uid}/health', auth=True)
            assert result['status'] == 'OK', f'{uid} datasource failed its real upstream query'
        return {'datasources': 2}
    if name.startswith('anubis-'):
        urls = {
            'anubis-website-ukmesh': ('http://127.0.0.1:8923/build-target.json', 'public-website'),
            'anubis-app-ukmesh': ('http://127.0.0.1:8924/build-target.json', 'app'),
            'anubis-health-check': ('http://127.0.0.1:8925/api/bootstrap', None),
        }
        url, target = urls[name]
        result = request(url, user_agent='Chrome-Lighthouse')
        assert result['target'] == target if target else result['mqtt']['connected']
        return {'proxied_json': True}
    if name == 'mesh-health-check-data-init':
        docker(['exec', 'ukmesh-full-mesh-health-check-1', 'sh', '-ec',
                'test -f /app/data/.meshcore-initialized && test -w /app/data'])
        return {'initialized_and_writable': True}
    if name == 'mesh-health-check':
        from common import STATE, read_json
        from benchmarks import active_fixture
        result = request('http://127.0.0.1:3090/api/bootstrap')
        assert result['mqtt']['connected'] and result['testChannel']['configured']
        proof = read_json(STATE / 'runtime/health-proof.json')
        assert proof['fixtureToken'] == active_fixture()['token'] and proof['passed']
        session = request('http://127.0.0.1:3090/api/sessions/' + proof['sessionId'])
        assert session['matchedAt'] > 0 and session['observedCount'] >= 2 and len(session['receipts']) >= 2
        assert session['useCount'] == 1, 'duplicate reception consumed another session use'
        return {'observer_receipts': len(session['receipts'])}
    if name == 'privacy-remat-worker':
        database = current_targets()['ukmesh']
        done = sql('ukmesh-sample-db', database, 'meshcore',
                   "SELECT count(*) FROM ops.privacy_rematerialization_queue WHERE status='done'")
        assert int(done) > 0, 'no real privacy rematerialization completed'
        return {'completed': int(done)}
    if name in ('sample-redis', 'beacon-redis'):
        container = 'ukmesh-sample-redis' if name == 'sample-redis' else 'ukmesh-companions-beacon-redis-1'
        info = docker(['exec', container, 'redis-cli', '--raw', 'INFO', 'stats']).stdout.decode()
        total = next(int(line.split(':')[1]) for line in info.splitlines() if line.startswith('total_commands_processed:'))
        assert total > 10, 'Redis has not processed actual application work'
        return {'commands': total}
    if name == 'hopreach':
        meta = request('http://127.0.0.1:3003/rf-coverage/meta.json')
        for tier in ('standard', 'precision'):
            tiles = meta['coverage'][tier]['tiles']
            assert tiles, f'{tier} has no published output'
            image = tiles[0]['image']
            assert image.startswith('tiles/') and '..' not in image
            with urlopen('http://127.0.0.1:3003/rf-coverage/' + image, timeout=10) as response:
                assert response.read(8) == b'\x89PNG\r\n\x1a\n', f'{tier} tile is not a PNG'
        return {'standard_and_precision': True}
    if name == 'alertmanager':
        metrics = request('http://127.0.0.1:9095/api/v2/status')
        assert metrics['cluster']['status'] in ('ready', 'disabled')
        probe = uuid.uuid4().hex
        request('http://127.0.0.1:9095/api/v2/alerts', data=[{
            'labels': {'alertname': 'OwnedFullStackProbe-' + probe, 'severity': 'info'},
            'annotations': {'summary': 'Owned local forwarding proof'},
        }])
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            result = request('http://127.0.0.1:3107/counts')
            if time.monotonic() <= deadline and probe in result.get('alertProbes', []):
                return {'native_forwarding': True}
            time.sleep(.5)
        raise AssertionError('the admitted Alertmanager probe did not reach the local destination')
    raise ValueError('Unknown native work oracle: ' + name)


if __name__ == '__main__':
    try:
        result = check(sys.argv[1])
        print(json.dumps({'passed': True, 'service': sys.argv[1], 'result': result}))
    except Exception as error:
        print(json.dumps({'passed': False, 'error': safe_error(error)}), file=sys.stderr)
        raise SystemExit(1)
