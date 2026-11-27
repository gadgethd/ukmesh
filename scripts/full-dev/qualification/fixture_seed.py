"""Replay a synthetic ACK cohort into an owned fixture, certifying each window from storage."""
from __future__ import annotations

import csv
import gzip
import hashlib
from itertools import batched, zip_longest
import json
from pathlib import Path
import shutil
import sqlite3
import time
from urllib.parse import quote

from benchmarks import active_fixture
from common import EVIDENCE, MIN_FREE_BYTES, REPO, command, digest, read_json, sql, utc_now, write_json
from target_observation import observe as observe_intake
from target_safety import assert_no_publishers, inspect_snapshot, publication_lease
from traffic import hex_list

WINDOW = 360
OBSERVER = '0102' + '11' * 30
TOPIC = f'meshcore/LHR/{OBSERVER}/packets'
FILES = ('packets.csv.gz', 'raw_packets.csv.gz')
TABLES = ('ukmesh_observations', 'ukmesh_ledger', 'corescope_packets', 'corescope_observations',
          'beacon_packets', 'beacon_observations')


def seed_source(path: Path) -> tuple[Path, dict, dict]:
    source = path.resolve(strict=True)
    if not source.is_relative_to(EVIDENCE.resolve()) or not source.is_dir():
        raise ValueError('seed source must be within owned qualification evidence')
    for name in ('fixture.json', *FILES):
        entry = source / name
        if entry.is_symlink() or not entry.is_file():
            raise ValueError('seed source requires regular manifest and CSV files')
    manifest = read_json(source / 'fixture.json')
    count = manifest.get('count')
    start = manifest.get('sequenceStart')
    if manifest.get('synthetic') is not True or manifest.get('seed') != 'sequential-ack-v1' \
            or type(count) is not int or type(start) is not int or not 0 < count <= 1_000_000 \
            or not 0 <= start <= 2**32 - count:
        raise ValueError('seed source must declare a bounded sequential synthetic ACK cohort')
    hashes = {name: digest(source / name) for name in ('fixture.json', *FILES)}
    if any(hashes[name] != manifest.get('checksums', {}).get(name) for name in FILES):
        raise ValueError('seed source checksums differ from its manifest')
    return source, manifest, hashes


def seed_rows(source: Path, manifest: dict):
    with gzip.open(source / FILES[0], 'rt', newline='') as packets, \
            gzip.open(source / FILES[1], 'rt', newline='') as raw_packets:
        found = 0
        for position, (row, raw) in enumerate(zip_longest(csv.DictReader(packets), csv.DictReader(raw_packets)), 1):
            if row is None or raw is None or position > manifest['count'] \
                    or any(row.get(field) != raw.get(field) for field in ('time', 'packet_hash', 'rx_node_id')):
                raise ValueError(f'seed packet and raw rows disagree at position {position}')
            wire = bytes.fromhex(raw['raw_hex'])
            if len(wire) not in (10, 14) or wire[0] not in (0x0C, 0x0D) \
                    or len(wire) != (14 if wire[0] == 0x0C else 10) \
                    or wire[-9:-4] != bytes.fromhex('42AABBCCDD') \
                    or int.from_bytes(wire[-4:], 'big') != manifest['sequenceStart'] + position - 1:
                raise ValueError(f'row does not match its synthetic ACK sequence at position {position}')
            identity = hashlib.sha256(b'\x0c' + wire[-4:]).hexdigest()
            if row.get('packet_hash') != identity.upper() or row.get('network') != 'ukmesh' \
                    or row.get('rx_node_id') != OBSERVER or row.get('topic') != TOPIC \
                    or row.get('packet_type') != '3':
                raise ValueError(f'packet identity or scope differs at position {position}')
            found = position
            radio_identity = hashlib.sha256(b'\x03' + wire[-4:]).hexdigest()[:16]
            yield identity, radio_identity, {'row': row, 'rawHex': raw['raw_hex']}
        if found != manifest['count']:
            raise ValueError('seed cohort count differs from its manifest')


def persisted(fixture: dict, identities: dict[str, str]) -> dict[str, dict[str, int]]:
    db = {item['label']: item for item in fixture['databases']}
    results = {}
    for name, label, table, encoded in (
        ('ukmesh_observations', 'ukmesh', 'mesh.packets', False),
        ('ukmesh_ledger', 'ukmesh', 'mesh.ingest_observation_ledger', False),
        ('beacon_packets', 'beacon', 'packets', True),
        ('beacon_observations', 'beacon', 'packet_observations', True),
    ):
        store = db[label]
        if encoded:
            key = "encode(packet_hash, 'hex')"
            predicate = "packet_hash IN (" + ",".join(f"decode('{value}','hex')" for value in identities.values()) + ")"
        else:
            key = "lower(packet_hash)"
            predicate = "packet_hash IN (" + hex_list([value.upper() for value in identities], 64) + ")"
        results[name] = json.loads(sql(store['container'], store['database'], store['user'], f"""
          SELECT coalesce(json_object_agg(key, n), '{{}}') FROM (
            SELECT {key} AS key, count(*) AS n FROM {table}
            WHERE {predicate} GROUP BY packet_hash
          ) AS matches
        """, timeout=25))
    path = Path(fixture['corescope_data_dir']) / 'analyzer.db'
    with sqlite3.connect('file:' + quote(str(path)) + '?mode=ro', uri=True, timeout=15) as reader:
        reader.execute('BEGIN')
        short = list(identities.values())
        placeholders = ','.join('?' for _ in short)
        results['corescope_packets'] = dict(reader.execute(
            f'SELECT hash, count(*) FROM transmissions WHERE hash IN ({placeholders}) GROUP BY hash', short))
        results['corescope_observations'] = dict(reader.execute(
            f'SELECT t.hash, count(*) FROM observations o JOIN transmissions t ON t.id = o.transmission_id '
            f'WHERE t.hash IN ({placeholders}) GROUP BY t.hash', short))
    return results


def classify_window(identities: dict[str, str], counts: dict) -> tuple[list[str], list[str]]:
    missing, inconsistent = [], []
    for identity, radio in identities.items():
        observations = [counts[name].get(identity if name.startswith('ukmesh') else radio, 0)
                        for name in TABLES]
        if all(value == 0 for value in observations):
            missing.append(identity)
        elif any(value != 1 for value in observations):
            inconsistent.append(identity)
    return missing, inconsistent


def publish(rows: list[dict]) -> None:
    script = """
      import { readFileSync } from 'node:fs';
      import { replayOwnedSeedBatch } from './backend/src/tools/replaySampleMqtt.ts';
      await replayOwnedSeedBatch(JSON.parse(readFileSync(0, 'utf8')));
    """
    command(['node', '--max-old-space-size=128', '--import',
             str(REPO / 'backend/node_modules/tsx/dist/loader.mjs'),
             '--input-type=module', '-e', script], data=json.dumps(rows).encode(), timeout=90)


def observe(fixture: dict, identities: dict[str, str], *, until_complete: bool):
    deadline = time.monotonic() + 120
    while True:
        missing, inconsistent = classify_window(identities, persisted(fixture, identities))
        if (not inconsistent and (not until_complete or not missing)) or time.monotonic() >= deadline:
            return missing, inconsistent
        time.sleep(1)


def seed(run: Path, path: Path) -> dict:
    with publication_lease(exclusive=True):
        fixture = active_fixture()
        assert_no_publishers()
        if not observe_intake(inspect_snapshot).get('passed'):
            raise RuntimeError('fixture intake must be quiescent before seed publication')
        return replay_seed(run, path, fixture)


def replay_seed(run: Path, path: Path, fixture: dict) -> dict:
    source, manifest, source_hashes = seed_source(path)
    # Validate the entire ordered cohort before opening the broker connection.
    for _ in seed_rows(source, manifest):
        pass
    checkpoint = run / 'seed-replay.json'
    identity = {'fixture': fixture['token'], 'source_sha256': source_hashes}
    if checkpoint.exists() and read_json(checkpoint).get('identity') != identity:
        raise ValueError('seed checkpoint belongs to another fixture or source')
    progress = {'identity': identity, 'count': manifest['count'], 'verified': 0, 'passed': False }
    for window in batched(seed_rows(source, manifest), WINDOW):
        identities = {value: radio for value, radio, _ in window}
        if len(set(identities.values())) != len(identities):
            raise ValueError('companion seed identities collide in this window')
        missing, inconsistent = observe(fixture, identities, until_complete=False)
        if inconsistent:
            raise RuntimeError(f'partial or duplicate observations at seed window after {progress["verified"]} verified')
        if missing:
            if shutil.disk_usage(REPO).free < MIN_FREE_BYTES:
                raise RuntimeError('fixture seed publication stopped at the existing disk reserve')
            missing_set = set(missing)
            publish([row for value, _, row in window if value in missing_set])
            missing, inconsistent = observe(fixture, identities, until_complete=True)
            if missing or inconsistent:
                raise RuntimeError(f'unreconciled persistence after {progress["verified"]} verified seed rows')
        progress['verified'] += len(window)
        progress['at'] = utc_now()
        write_json(checkpoint, progress)
    if any(digest(source / name) != value for name, value in source_hashes.items()):
        raise RuntimeError('seed source changed during publication')
    progress['passed'] = True
    write_json(checkpoint, progress)
    return progress
