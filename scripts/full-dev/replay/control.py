#!/usr/bin/env python3
"""Manage the optional archive ingress on the DEV host."""

import argparse
import hashlib
import os
from pathlib import Path
import sys

from observers import seed_archive_observers
from prepare import prepare

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'qualification'))
from common import MIN_FREE_BYTES, REPO, STATE, command, docker, docker_group_command, read_json, sql, write_json
from fixtures import original_values, selection_values
from target_lease import target_lease
from target_safety import require_originals

RUNTIME = STATE / 'runtime/replay'
INPUT = RUNTIME / 'input'
PROGRESS = RUNTIME / 'progress'
ENABLED = RUNTIME / 'enabled.json'
ARCHIVE = REPO / 'data/ukmesh-sample-2026-11-10'
AIRPORTS = REPO / 'third_party/beacon-server/internal/iatadb/gen/airports.csv'
TABLES = ('packets', 'raw_packets', 'nodes', 'node_status_samples')


def compose(*args):
    invocation = ['docker', '--host', 'unix:///var/run/docker.sock', 'compose', '--env-file', '/dev/null',
                  '-f', str(HERE / 'compose.yml'), *args]
    result = command(docker_group_command(invocation), timeout=60, check=False,
                     environment_overrides={'REPLAY_UID': str(os.getuid()), 'REPLAY_GID': str(os.getgid()),
                                            'REPLAY_DISK_RESERVE_BYTES': str(MIN_FREE_BYTES)})
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    return result.returncode


def stage():
    digest = hashlib.sha256((HERE / 'prepare.py').read_bytes())
    for table in TABLES:
        with (ARCHIVE / (table + '.csv.gz')).open('rb') as source:
            digest.update(hashlib.file_digest(source, 'sha256').digest())
    source_id = digest.hexdigest()
    manifest_path = INPUT / 'manifest.json'
    output = INPUT / 'events.jsonl.gz'
    if output.is_file() and manifest_path.is_file() and read_json(manifest_path)['digest'] == source_id:
        return
    active = docker(['ps', '-q', '--filter', 'label=com.docker.compose.project=ukmesh-replay']).stdout.strip()
    if active:
        raise ValueError('stop the archive replay before replacing its input')
    temporary = INPUT / 'events.next'
    try:
        counts = prepare(ARCHIVE, temporary)
        temporary.replace(output)
        write_json(manifest_path, {'digest': source_id, 'day': '2026-11-10',
                                   'events': sum(counts.values()), 'counts': counts})
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('start', 'stop', 'status', 'resume', 'suspend'))
    action = parser.parse_args().action
    if action == 'status':
        status = compose('ps', '-a')
        for path in (INPUT / 'manifest.json', PROGRESS / 'state.json'):
            if path.is_file():
                print(path.read_text().strip())
        return status
    with target_lease():
        if action in ('stop', 'suspend'):
            status = compose('stop')
            if status == 0 and action == 'stop':
                ENABLED.unlink(missing_ok=True)
            return status
        if action == 'resume' and (not ENABLED.is_file() or selection_values() != original_values()):
            return 0
        require_originals()
        for path in (RUNTIME, INPUT, PROGRESS):
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
        stage()
        seeded = seed_archive_observers(ARCHIVE, AIRPORTS, sql)
        print(f'DEV archive observer area estimates added: {seeded}')
        status = compose('up', '-d', '--wait', '--wait-timeout', '45')
        if status == 0 and action == 'start':
            write_json(ENABLED, {'enabled': True})
        return status


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        raise SystemExit(f'DEV replay: {error}')
