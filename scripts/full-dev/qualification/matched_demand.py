"""Own fixed-demand cohort collection and audit every planned arrival and read."""
import datetime as dt
import math
import heapq
from role_evidence import validate_database_identity
from pathlib import Path
import re
import time
import uuid

from common import digest, owned_document, safe_error, utc_now, write_json
from companion_reads import CompanionReads, companion_summary, measured_companion_data
from context import phase_context
from loopback_http import http_get
from read_protocol import IDENTITY, PROFILE, STEPS
from read_timeline import clock_intersection


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def native_demand(record):
    pointer = record['native_evidence']; path = Path(pointer['path'])
    value = owned_document(path, pointer['sha256']); policy = PROFILE['demand']
    if record['exit_code'] != 0 or value.get('passed') is not True or value['settings'] != policy \
            or value['sourceIdentity'] != record['measured_source'] or value['sourceIdentityAfter'] != record['measured_source'] \
            or value.get('sourceStable') is not True or value.get('timeline') != 'timeline.json':
        raise ValueError('fixed-demand source/settings/native completion differs')
    validate_database_identity(value.get('databaseIdentity'), record['context'])
    clock_value = owned_document(path.parent/'timeline.json', pointer['timeline_sha256'])
    clock = clock_intersection(clock_value)
    if clock_value.get('passed') is not True:
        raise ValueError('fixed-demand timeline failed')
    window = value['window']; start, end = window['startMs'], window['endMs']
    opening = [event for event in clock_value['events'] if event['phase'] == 'demand' and event['edge'] == 'start']
    closing = [event for event in clock_value['events'] if event['phase'] == 'demand' and event['edge'] == 'finish']
    if not number(start) or not number(end) or len(opening) != 1 or opening[0]['offsetMs'] != start \
            or end != start + policy['duration_seconds']*1000 or clock_value['durationMs'] < end:
        raise ValueError('fixed120-second schedule window is missing or shortened')
    if len(closing) != 1 or len([event for event in clock_value['events'] if event['phase']=='demand']) != 2 \
            or [event['sequence'] for event in clock_value['events']] != list(range(len(clock_value['events']))) \
            or type(opening[0]['span']) is not int or opening[0]['span'] < 0 \
            or any(closing[0].get(key) != opening[0].get(key) for key in ('span', 'parent', 'phase')) \
            or closing[0].get('outcome') != 'passed' or closing[0]['offsetMs'] < end:
        raise ValueError('fixed-demand span is missing its ordered successful completion')
    rows = value['samples']; count = policy['requests']; period = 1000/policy['rps']
    if len(rows) != count or [row['index'] for row in rows] != list(range(count)):
        raise ValueError('planned fixed-demand arrivals are missing, duplicated or reordered')
    previous = None; active_ends = []
    for index, row in enumerate(rows):
        if row['name'] != policy['case_order'][index % len(policy['case_order'])] \
                or row['actor'] != f'127.0.1.{index % policy["clients"] + 1}' \
                or row['scheduledMs'] != start + index*period:
            raise ValueError('fixed-demand cyclic case, actor or schedule differs')
        if not all(number(row.get(key)) for key in ('attemptMs', 'startMs', 'socketMs', 'connectedMs', 'endMs', 'completedMs', 'latenessMs', 'ms')) \
                or not row['scheduledMs'] <= row['attemptMs'] <= row['startMs'] <= row['socketMs'] <= row['connectedMs'] <= row['endMs'] <= row['completedMs'] \
                or row['latenessMs'] != row['socketMs']-row['scheduledMs'] \
                or row['latenessMs'] > policy['max_admission_lateness_ms'] \
                or not math.isclose(row['ms'], row['endMs']-row['startMs'], abs_tol=1e-6, rel_tol=1e-9) \
                or not 0 <= row['ms'] <= policy['response_deadline_ms']:
            raise ValueError('fixed-demand arrival was undelivered, late or exceeded its response deadline')
        if previous is not None and row['socketMs']-previous < policy['min_dispatch_interval_ms']:
            raise ValueError('fixed-demand arrivals were dispatched in a catch-up batch')
        previous = row['socketMs']
        while active_ends and active_ends[0] <= row['startMs']: heapq.heappop(active_ends)
        heapq.heappush(active_ends, row['completedMs'])
        if len(active_ends) > policy['max_active'] or row['completedMs'] > clock_value['durationMs']:
            raise ValueError('fixed-demand active work exceeded8 or outlived native final closure')
        if type(row['active']) is not int or not 1 <= row['active'] <= policy['max_active'] \
                or type(row['queue']) is not int or not 0 <= row['queue'] <= policy['max_admission_lateness_ms']/period \
                or row.get('passed') is not True or row['status'] != (404 if row['name']=='packet-missing' else 200) or not 0 < row['bytes'] <= 8*1024*1024 \
                or row['entries'] is not None and (type(row['entries']) is not int or row['entries'] < 0) \
                or any(not re.fullmatch('[a-f0-9]{64}', row.get(key, '')) for key in ('digest', 'dataDigest')):
            raise ValueError('fixed-demand response validation/concurrency/queue failed')
    actual_span = rows[-1]['socketMs']-rows[0]['socketMs']; admission = value['admission']
    expected = {'planned': count, 'attempted': count, 'admitted': count, 'completed': count, 'errors': 0, 'undelivered': 0,
                'maxActive': max(row['active'] for row in rows),
                'maxLatenessMs': max(row['latenessMs'] for row in rows), 'dispatchSpanMs': actual_span}
    if any(admission.get(key) != wanted for key, wanted in expected.items()) \
            or type(admission['maxQueue']) is not int or not max(row['queue'] for row in rows) <= admission['maxQueue'] <= policy['max_admission_lateness_ms']/period \
            or abs(actual_span-(count-1)*period) > policy['dispatch_span_tolerance_ms']:
        raise ValueError('fixed-demand admission totals/span differ from all raw arrivals')
    warmup = value['warmup']
    if [(row['name'], row['repeat']) for row in warmup] != [(name, repeat) for name in policy['case_order'] for repeat in range(policy['warmups_per_case'])] \
            or any(row.get('passed') is not True or row['status'] != (404 if row['name']=='packet-missing' else 200) or not number(row['ms'])
                   or not 0 <= row['ms'] <= policy['response_deadline_ms']
                   or not 0 < row['bytes'] <= 8*1024*1024 or row['entries'] is not None and (type(row['entries']) is not int or row['entries'] < 0)
                   or any(not re.fullmatch('[a-f0-9]{64}', row.get(key, '')) for key in ('digest', 'dataDigest')) for row in warmup):
        raise ValueError('fixed-demand warm cache preparation is missing or failed')
    limits = {'workerCount': 1, 'maxPending': 8, 'maxBytes': 8388608, 'timeoutMs': 30000, 'memoMaxEntries': 8,
              'resourceLimits': {'maxOldGenerationSizeMb': 128, 'maxYoungGenerationSizeMb': 16, 'stackSizeMb': 4}}
    counters = value['resources']['responseValidation']; total = count+len(warmup)
    if value['responseValidationLimits'] != limits or set(counters) != {'calls', 'freshHashes', 'hits', 'misses', 'evictions', 'fullValidations', 'errors', 'entries', 'maxEntries'} \
            or any(type(value) is not int or value < 0 for value in counters.values()) \
            or counters['calls'] != total or counters['freshHashes'] != total or counters['errors'] != 0 \
            or counters['hits']+counters['misses'] != total or counters['fullValidations'] != counters['misses'] \
            or not 1 <= counters['entries'] <= counters['maxEntries'] <= 8:
        raise ValueError('fixed-demand original bounded verifier limits/fresh hashes differ')
    return value, clock_value, clock


def cohort_evidence(record):
    value, clocks, clock = native_demand(record)
    raw = measured_companion_data(record, cadence_seconds=PROFILE['companion_cadence_seconds'], scopes=['demand'])
    collector = raw['collector']; origin = dt.datetime.fromisoformat(clock['origin_utc'].replace('Z', '+00:00'))
    relative = lambda at: (dt.datetime.fromisoformat(at.replace('Z', '+00:00'))-origin).total_seconds()*1000
    anchor = clocks['originMonotonicMs']-clock['anchor_monotonic_ms']
    start = value['window']['startMs']+anchor; end = value['window']['endMs']+anchor
    low, high = clock['offset_lower_ms'], clock['offset_upper_ms']
    if relative(collector['started_at']) > start+low or relative(collector['stop_requested_at']) < end+high \
            or relative(collector['finished_at']) < end+high:
        raise ValueError('companion collector did not cover the complete scheduled120-second window')
    if any(row['failures'] for row in raw['summary'].values()):
        raise ValueError('fixed-demand setup/window/drain companion errors are required failures')
    selected, joined = [], []
    for row in collector['records']:
        at = relative(row['started_at'])
        # Include a possibly in-window start; never discard boundary uncertainty.
        included = start+low <= at < end+high
        relation = 'inside' if start+high <= at < end+low else 'boundary-uncertain' if included else 'outside'
        joined.append({'actor': row['actor'], 'sequence': row['sequence'], 'included': included,
                       'relation': relation, 'crosses_finish': included and relative(row['finished_at']) >= end+low})
        if included: selected.append(row)
    totals = companion_summary(selected)
    if any(row['requests'] < PROFILE['minimum_requests_per_app_window'] for row in totals.values()) \
            or {row['actor'] for row in selected} != set(collector['actors']):
        raise ValueError('all fixed-demand companion actors require complete in-window observations')
    return raw, selected, {'summary': totals, 'window': value['window'], 'clock': clock, 'membership': joined}


def demand_comparison(runs, *, scales=('1x', '10x')):
    checks, missing, identities, collector_ids = [], [], set(), set()
    try:
        if any(row['scale'] not in scales for row in runs): raise ValueError('unexpected demand scale')
        for scale in scales:
            rows = [row for row in runs if row['scale'] == scale]
            if len(rows) != len(STEPS): missing.append(scale+'/six-fixed-demand-cohorts'); continue
            pooled = {'A': [], 'B': []}; first = rows[0]; last_end = None
            for row, step in zip(rows, STEPS):
                window = row['read_window']
                if row.get('passed') is not True or row.get('diagnostic_only') is not False or row['protocol'] != IDENTITY \
                        or row['label'] != PROFILE['source_labels'][step['side']] \
                        or any(row[key] != first[key] for key in ('scale', 'fixture_token', 'context')) \
                        or row['context_before'] != row['context'] \
                        or set(window) != {'batch_id', 'id', 'pair', 'position', 'side'} \
                        or any(window[key] != value for key, value in step.items()) \
                        or window['batch_id'] != first['read_window']['batch_id'] \
                        or any(not re.fullmatch('[a-f0-9]{32}', window[key]) for key in ('batch_id', 'id')):
                    raise ValueError('fixed-demand source/context/protocol/pair order differs')
                if window['id'] in identities: raise ValueError('fixed-demand cohort identity was reused')
                identities.add(window['id'])
                raw, selected, _ = cohort_evidence(row); collector = raw['collector']
                if collector['collector_id'] in collector_ids: raise ValueError('fixed-demand collector was reused')
                collector_ids.add(collector['collector_id'])
                if last_end is not None and collector['origin_monotonic'] < last_end:
                    raise ValueError('fixed-demand cohorts overlapped')
                last_end = collector['origin_monotonic']+collector['finished_offset_seconds']
                pooled[step['side']].extend(selected)
            totals = {side: companion_summary(values) for side, values in pooled.items()}
            for name in ('corescope', 'beacon'):
                before, after = totals['A'][name], totals['B'][name]
                checks.append({'scale': scale, 'name': name, 'baseline': before, 'candidate': after,
                    'baseline_p95_ms': before['p95_ms'], 'candidate_p95_ms': after['p95_ms'],
                    'pooled_windows_per_source': len(PROFILE['pair_order']),
                    'passed': after['p95_ms'] <= before['p95_ms']*PROFILE['regression_ratio']})
    except (ValueError, KeyError, TypeError, OSError) as caught: missing.append(str(caught))
    valid = not missing and len(checks) == len(scales)*2
    return {'passed': valid and all(row['passed'] for row in checks), 'valid': valid,
            'missing': missing, 'companion_comparisons': checks}


def collect_cohort(run, fixture, source, label, directory, window, context, deadline, node_tool):
    directory.mkdir(mode=0o700); collector = CompanionReads(http_get, cadence_seconds=PROFILE['companion_cadence_seconds'])
    error = None; code = None; final = context
    try:
        collector.start(); collector.set_scope('demand')
        database = next(row['database'] for row in fixture['databases'] if row['label'] == 'ukmesh')
        from read_protocol import PATH
        code = node_tool('benchmarkMatchedDemand', ['--source-root', source['source_root'], '--reference-revision', source['revision'],
            '--policy', str(PATH), '--output', str(directory)], database, directory/'native.log', timeout=max(.001, deadline-time.monotonic()))
        if code != 0: raise RuntimeError('fixed-demand native cohort failed; partial planned arrivals retained')
    except BaseException as caught: error = caught
    finally:
        try:
            if collector.origin is not None: collector.set_scope('teardown')
        except BaseException as caught: error = error or caught
        try: collector.finish()
        except BaseException as caught: error = error or caught
        try: final = phase_context(run)
        except BaseException as caught: error = error or caught
        identity = {'scale': fixture['kind'], 'label': label, 'fixture_token': fixture['token'], 'measured_source': source,
                    'context': final, 'protocol': IDENTITY, 'read_window': window, 'diagnostic_only': False}
        raw = {'version': 1, **identity, 'collector': collector.evidence(), 'summary': companion_summary(collector.records)}
        write_json(directory/'companions.json', raw)
        native, timeline = directory/'matched-demand.json', directory/'timeline.json'
        record = {**identity, 'at': utc_now(), 'context_before': context, 'exit_code': code,
            'native_evidence': {'path': str(native), 'sha256': digest(native) if native.exists() else None,
                                'timeline_sha256': digest(timeline) if timeline.exists() else None},
            'companion_evidence': {'path': str(directory/'companions.json'), 'sha256': digest(directory/'companions.json'), 'collector_id': collector.collector_id},
            'companions': raw['summary'], 'passed': error is None and final == context,
            'errors': [] if error is None else [type(error).__name__+': '+safe_error(error)]}
        try:
            if record['passed']: _, _, record['window_audit'] = cohort_evidence(record)
            if time.monotonic() >= deadline: raise TimeoutError('fixed-demand collection deadline exceeded')
        except BaseException as caught:
            error = caught; record['passed'] = False; record['errors'].append(type(caught).__name__+': '+safe_error(caught))
        write_json(directory/'summary.json', record)
    if error is not None: raise error
    return record
