#!/usr/bin/env python3
"""Verify the historical rescue ledger using local Git objects, without VPS access.

--capture additionally checks the ignored local capture. --candidate flags any
change to an audited class-A file for review; byte equality is not a deployment
or semantic compatibility guarantee. No command in this script writes to Git.
"""

import argparse
import base64
from collections import Counter
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / 'evidence/deploy-rescue-20261022.json'


def git(*args):
    return subprocess.run(
        ['git', *args], cwd=ROOT, check=True, capture_output=True,
    ).stdout


def require(condition, message):
    if not condition:
        raise ValueError(message)


def object_id(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()


@lru_cache(maxsize=None)
def file_at(revision, path):
    result = subprocess.run(
        ['git', 'show', f'{revision}:{path}'], cwd=ROOT, capture_output=True,
    )
    if result.returncode:
        # Distinguish an absent path from a missing/invalid commit.
        git('rev-parse', '--verify', f'{revision}^{{commit}}')
        require(not git('ls-tree', '-r', revision, '--', path), f'unreadable Git object: {path}')
        return None
    return result.stdout


def verify(args):
    ledger = json.loads(LEDGER.read_text())
    require(ledger['schema_version'] == 1, 'unsupported ledger schema')
    rows = ledger['files']
    require(len(rows) == 221, 'expected 221 visible VPS status paths')
    require(len({row['path'] for row in rows}) == len(rows), 'duplicate paths')
    for field, summary in [('class', 'class_counts'), ('status', 'status_counts'),
                           ('disposition', 'disposition_counts')]:
        require(dict(Counter(row[field] for row in rows)) == ledger[summary], f'{summary} mismatch')
    require(ledger['class_counts'] == {'A': 208, 'B': 4, 'C': 9}, 'unexpected classification totals')
    require(sorted(row['path'] for row in rows if row['status'] == 'UU')
            == sorted(ledger['unmerged_paths']), 'unmerged path mismatch')
    base = ledger['audited_main']
    commits = ledger['local_commits']
    require(len(commits) == 5, 'expected five VPS-local commits')
    source_commits = git('rev-list', f"{ledger['vps_origin_main']}..{ledger['vps_head']}").decode().splitlines()
    require(source_commits == [row['sha'] for row in commits], 'VPS commit range mismatch')
    for sha in [row['sha'] for row in commits] + [ledger['recovery_commit'], ledger['recovery_merge']]:
        git('merge-base', '--is-ancestor', sha, base)
    proved = 0
    for row in rows:
        path = row['path']
        require(not Path(path).is_absolute() and '..' not in Path(path).parts, 'unsafe ledger path')
        current = file_at(base, path)
        require((object_id(current) if current is not None else None) == row['main_blob'], f'main blob mismatch: {path}')
        disposition = row['disposition']
        if disposition in ('identical-main', 'historical-superseded', 'parked-local-probe'):
            content = file_at(row['proof_ref'], path)
            require(content is not None, f'missing proof file: {path}')
            if disposition == 'parked-local-probe':
                require(path == 'docker-compose.yml', 'unexpected parked transform')
                needle = b'wget -qO- http://127.0.0.1:8080/readyz >/dev/null'
                before, service = content.split(b'\n  alert-receiver:\n', 1)
                receiver, after = service.split(b'\n  alertmanager:\n', 1)
                require(receiver.count(needle) == 1, 'ambiguous alert-receiver probe')
                receiver = receiver.replace(needle, needle.replace(b'/readyz', b'/healthz'))
                content = before + b'\n  alert-receiver:\n' + receiver + b'\n  alertmanager:\n' + after
            require(object_id(content) == row['source_blob'], f'source blob mismatch: {path}')
            if disposition == 'identical-main':
                require(row['source_blob'] == row['main_blob'], f'not identical: {path}')
            proved += 1
        elif disposition == 'deleted-upstream':
            require(row['class'] == 'C' and current is None and row['source_blob'] is None, f'invalid deletion: {path}')
        elif disposition == 'excluded-secret':
            require(row['class'] == 'B' and row['source_blob'] is None and row['proof_ref'] is None, f'excluded content retained: {path}')
        else:
            raise ValueError(f'unknown disposition: {path}')
    if args.capture:
        raw = Path(args.capture).read_bytes()
        require(hashlib.sha256(raw).hexdigest() == ledger['private_capture_sha256'], 'private capture checksum mismatch')
        capture = json.loads(raw)
        captured = {row['path']: row for row in capture['entries']}
        require(set(captured) == {row['path'] for row in rows}, 'capture path coverage mismatch')
        for row in rows:
            source = captured[row['path']]
            require(source['status'] == row['status'], f'capture status mismatch: {row["path"]}')
            raw_content = base64.b64decode(source['content'], validate=True) if 'content' in source else None
            require((object_id(raw_content) if raw_content is not None else None) == row['source_blob'], f'capture bytes mismatch: {row["path"]}')
        print('Private capture: checksum, all 221 paths/statuses, and 211 readable contents verified.')
    if args.candidate:
        candidate = git('rev-parse', '--verify', f'{args.candidate}^{{commit}}').decode().strip()
        git('merge-base', '--is-ancestor', base, candidate)
        changed = [row['path'] for row in rows if row['class'] == 'A'
                   and file_at(candidate, row['path']) != file_at(base, row['path'])]
        require(not changed, 'Candidate changes require feature review: ' + ', '.join(changed))
        print(f'Candidate {candidate}: all 208 audited feature/support files unchanged.')
    print(f'Ledger: 221 paths; A=208 B=4 C=9; {proved} readable source blobs proved; 5/5 commits upstream.')
    print('OPEN: unreadable mosquitto/ (including tracked config); private env backup; local liveness override.')
    print('Historical source evidence only. Does not establish running-image provenance or deployment readiness.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', help='optional private inventory.json from this read-only capture')
    parser.add_argument('--candidate', help='optional Git revision to check against audited feature/support bytes')
    try:
        verify(parser.parse_args())
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f'FAIL: {error}', file=sys.stderr)
        sys.exit(1)
