"""Audit complete matched windows and pool every raw companion observation."""
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import statistics

from common import EVIDENCE
from companion_reads import companion_summary, measured_companion_data
from read_protocol import IDENTITY, PROFILE, STEPS
from read_timeline import timeline


def owned_document(path, expected_sha):
    if not path.resolve().is_relative_to(EVIDENCE.resolve()): raise ValueError('owned read evidence required')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 16*1024*1024:
            raise ValueError('bounded regular read evidence required')
        data = stream.read(16*1024*1024+1)
    if len(data) > 16*1024*1024 or hashlib.sha256(data).hexdigest() != expected_sha:
        raise ValueError('read evidence digest or size differs')
    return json.loads(data)


def native_evidence(mode, record, collector=None):
    path = Path(mode['source'])
    value = owned_document(path, mode['evidence_sha256'])
    source = record['measured_source']
    if mode['passed'] is not True or mode['exit_code'] != 0 or value['passed'] is not True \
            or value['sourceIdentity'] != source or value['sourceIdentityAfter'] != source or value['sourceStable'] is not True \
            or mode['source_identity'] != source or mode['source_stable'] is not True \
            or value['results'] != mode['results'] or value['settings'] != mode['settings']:
        raise ValueError('native read result/source differs from its sealed evidence')
    settings = value['settings']
    wanted = {'samples': PROFILE['samples'][str(mode['concurrency'])], 'trials': PROFILE['native_trials'],
              'concurrency': mode['concurrency'], 'cache': mode['cache']}
    if any(settings.get(key) != expected or (key != 'cache' and type(settings.get(key)) is not int)
           for key, expected in wanted.items()):
        raise ValueError('native read workload differs from the fixed protocol')
    rows = value['results']; names = {row['name'] for row in rows}
    keys = [(row['name'], row['trial']) for row in rows]
    expected = {(name, trial) for name in names for trial in range(1, PROFILE['native_trials'] + 1)}
    if not names or len(keys) != len(set(keys)) or set(keys) != expected \
            or any(type(row['p95Ms']) not in (int, float) or not math.isfinite(row['p95Ms']) or row['p95Ms'] < 0
                   or row['samples'] != wanted['samples'] for row in rows):
        raise ValueError('native read cases/trials are missing, duplicated or invalid')
    timeline_path = path.parent / 'timeline.json'
    if value.get('timeline') != 'timeline.json':
        raise ValueError('native timeline digest/association differs')
    value_clock = owned_document(timeline_path, mode['timeline_sha256'])
    proof = timeline(timeline_path, value, document=value_clock)
    if collector is not None:
        scope = f"c{mode['concurrency']}-{mode['cache']}"
        boundaries = collector['boundaries']
        opening = next(row for row in boundaries if row['scope'] == scope)
        closing = boundaries[opening['id']+1]
        parse = lambda at: dt.datetime.fromisoformat(at.replace('Z', '+00:00'))
        first, last = parse(value_clock['startedAt']), parse(value_clock['finishedAt'])
        # Date exposes an integer millisecond. Association is possible only if
        # that actual quantization interval intersects the owning mode window.
        if first + dt.timedelta(milliseconds=1) <= parse(opening['at']) or last >= parse(closing['at']) \
                or not first <= parse(value['at']) <= last + dt.timedelta(milliseconds=1):
            raise ValueError('native timeline belongs outside its actual collector mode interval')
    if record['label'] == 'candidate' and any(row['p95Ms'] > PROFILE['budgets_ms'][record['scale']] for row in rows):
        raise ValueError('a current-source native trial exceeds its absolute budget')
    return proof


def read_comparison(runs, *, scales=('1x', '10x'), diagnostic_only=False):
    missing, comparisons, companions, descriptive = [], [], [], []
    raw_identities = set()
    try:
        ids = [row['companion_evidence']['collector_id'] for row in runs]
        if len(ids) != len(set(ids)):
            raise ValueError('independent companion collector identity was reused')
        if any(row['scale'] not in scales for row in runs):
            raise ValueError('unexpected read scale')
        for scale in scales:
            windows = [row for row in runs if row['scale'] == scale]
            if len(windows) != len(STEPS):
                missing.append(scale + '/six-matched-windows'); continue
            batch_ids, window_ids, tokens, context = set(), set(), set(), windows[0]['context']
            pooled = {'A': [], 'B': []}; native = {'A': {}, 'B': {}}
            reference_settings, reference_cases, last_finished = {}, {}, None
            for row, step in zip(windows, STEPS):
                window = row['read_window']
                if row.get('diagnostic_only') is not diagnostic_only or row['protocol'] != IDENTITY or row['passed'] is not True:
                    raise ValueError('diagnostic, failed or obsolete read windows cannot qualify')
                if set(window) != {'batch_id', 'id', 'pair', 'position', 'side'} or any(window[key] != value for key, value in step.items()) \
                        or any(not re.fullmatch('[a-f0-9]{32}', window[key]) for key in ('batch_id', 'id')):
                    raise ValueError('read pair/window identity or AB-BA-AB order differs')
                label = 'candidate' if diagnostic_only else PROFILE['source_labels'][step['side']]
                if row['label'] != label or row['context'] != context or row['context_before'] != context:
                    raise ValueError('read source label, fixture or runtime context differs')
                batch_ids.add(window['batch_id']); window_ids.add(window['id']); tokens.add(row['fixture_token'])
                raw = measured_companion_data(row, cadence_seconds=PROFILE['companion_cadence_seconds'])
                collector = raw['collector']
                data_identity = hashlib.sha256(json.dumps({key: collector[key] for key in
                    ('records', 'boundaries', 'origin_monotonic', 'started_at', 'finished_at')}, sort_keys=True).encode()).hexdigest()
                if data_identity in raw_identities: raise ValueError('raw companion observation data was reused')
                raw_identities.add(data_identity)
                if last_finished is not None and collector['origin_monotonic'] < last_finished:
                    raise ValueError('read windows overlap rather than run serially')
                last_finished = collector['origin_monotonic'] + collector['finished_offset_seconds']
                if any(v['requests'] < PROFILE['minimum_requests_per_app_window'] or v['failures'] != 0 for v in raw['summary'].values()):
                    raise ValueError('companion window is incomplete or has failed requests')
                pooled[step['side']].extend(collector['records'])
                if [{key: mode[key] for key in ('concurrency', 'cache')} for mode in row['modes']] != PROFILE['modes']:
                    raise ValueError('read mode order differs')
                for mode in row['modes']:
                    native_evidence(mode, row, collector)
                    key = (mode['concurrency'], mode['cache'])
                    shape = [(v['trial'], v['name']) for v in mode['results']]
                    if key in reference_settings and (reference_settings[key] != mode['settings'] or reference_cases[key] != shape):
                        raise ValueError('matched read settings or case order differ')
                    reference_settings[key], reference_cases[key] = mode['settings'], shape
                    for result in mode['results']:
                        native[step['side']].setdefault((*key, result['name']), []).append(result['p95Ms'])
            if len(batch_ids) != 1 or len(window_ids) != len(STEPS) or len(tokens) != 1 or None in tokens:
                raise ValueError('read windows reuse identity or mix batches/fixtures')
            if diagnostic_only and any(row['measured_source'] != windows[0]['measured_source'] for row in windows):
                raise ValueError('same-source control selected different source identities')
            totals = {side: companion_summary(values) for side, values in pooled.items()}
            for name in ('corescope', 'beacon'):
                before, after = totals['A'][name], totals['B'][name]
                old, new = before['p95_ms'], after['p95_ms']
                passed = old is not None and new is not None and new <= old * PROFILE['regression_ratio']
                if diagnostic_only: passed = passed and old <= new * PROFILE['regression_ratio']
                companions.append({'scale': scale, 'name': name, 'baseline_p95_ms': old, 'candidate_p95_ms': new,
                    'baseline': before, 'candidate': after, 'pooled_windows_per_source': len(PROFILE['pair_order']), 'passed': passed})
            if set(native['A']) != set(native['B']): raise ValueError('read case parity differs')
            wanted_trials = PROFILE['native_trials'] * len(PROFILE['pair_order'])
            for (concurrency, cache, name), old in native['A'].items():
                new = native['B'][(concurrency, cache, name)]
                nine = len(old) == len(new) == wanted_trials
                budget = PROFILE['budgets_ms'][scale]
                within = all(value <= budget for value in new) and (not diagnostic_only or all(value <= budget for value in old))
                comparisons.append({'scale': scale, 'concurrency': concurrency, 'cache': cache, 'name': name,
                    'baseline_p95_ms': statistics.median(old), 'candidate_p95_ms': statistics.median(new),
                    'baseline_trials_ms': old, 'candidate_trials_ms': new, 'nine_trials': nine, 'within_budget': within})
            for pair in range(1, len(PROFILE['pair_order']) + 1):
                chosen = [row for row in windows if row['read_window']['pair'] == pair]
                descriptive.append({'scale': scale, 'pair': pair, 'windows': [row['read_window'] for row in chosen],
                                    'companions': [row['companions'] for row in chosen]})
    except (ValueError, KeyError, TypeError, OSError) as error:
        missing.append(str(error))
    valid = not missing and bool(comparisons) and len(companions) == len(scales)*2 \
            and all(row['nine_trials'] and row['within_budget'] for row in comparisons)
    return {'passed': valid and all(row['passed'] for row in companions), 'valid': valid,
            'diagnostic_only': diagnostic_only, 'protocol': IDENTITY, 'missing': missing, 'comparisons': comparisons,
            'companion_comparisons': companions, 'descriptive_pairs': descriptive}


def validate_read_batches(value, *, scales=('1x', '10x')):
    """A passing comparison cannot replace successful owning batch closure."""
    batches = value.get('batches', [])
    if len(batches) != len(scales) or {item['scale'] for item in batches} != set(scales):
        raise ValueError('successful read batch closure is missing')
    for item in batches:
        path = Path(item['path'])
        batch = owned_document(path, item['sha256']); rows = [r for r in value['runs'] if r['scale'] == item['scale']]
        elapsed = batch['collection_seconds']
        if item.get('passed') is not True or batch.get('passed') is not True or batch.get('diagnostic_only') is not False \
                or batch['protocol'] != IDENTITY or batch['batch_id'] != item['batch_id'] or batch['runs'] != rows \
                or batch['context'] != batch['context_before'] or batch['context'] != rows[0]['context'] \
                or batch.get('errors') or batch.get('final_context_error') \
                or batch['collection_budget_seconds'] != PROFILE['collection_budget_seconds'] \
                or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or not 0 <= elapsed < PROFILE['collection_budget_seconds'] \
                or not read_comparison(rows, scales=(item['scale'],))['passed']:
            raise ValueError('read batch finalization/source/deadline failed')
