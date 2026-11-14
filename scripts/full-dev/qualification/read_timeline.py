"""Audit the existing native UTC brackets and describe all companion crossings."""
import datetime as dt
import math
from common import digest, read_json


def clock_intersection(value):
    if value.get('version') != 2: raise ValueError('bracketed native clock version required')
    origin, final = value['originClock'], value['finishedClock']
    if origin['at'] != value['startedAt'] or final['at'] != value['finishedAt'] or origin['monotonicAfterMs'] != value['originMonotonicMs']:
        raise ValueError('native origin/final clock aliases differ')
    captures = [origin] + [event['clock'] for event in value['events']] + [final]
    base = dt.datetime.fromisoformat(origin['at'].replace('Z', '+00:00'))
    if base.tzinfo is None: raise ValueError('native UTC must be timezone-aware')
    anchor = origin['monotonicBeforeMs']
    lower, upper, previous_after, previous_wall = -math.inf, math.inf, -math.inf, base
    windows = []
    for capture in captures:
        before, after = capture['monotonicBeforeMs'], capture['monotonicAfterMs']
        wall = dt.datetime.fromisoformat(capture['at'].replace('Z', '+00:00'))
        if any(type(x) not in (int, float) or not math.isfinite(x) for x in (before, after)) or not 0 <= before <= after or before < previous_after:
            raise ValueError('native monotonic brackets overlap or go backwards')
        if wall.tzinfo is None or wall < previous_wall or wall.microsecond % 1000:
            raise ValueError('native UTC is backwards or not Date-millisecond precision')
        delta = wall - base
        relative = delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds / 1000
        lower, upper = max(lower, relative - (after - anchor)), min(upper, relative + 1 - (before - anchor))
        windows.append((before, after)); previous_after, previous_wall = after, wall
    if not lower < upper: raise ValueError('native clock brackets have no consistent offset intersection')
    if value['durationMs'] != final['monotonicAfterMs'] - origin['monotonicAfterMs']:
        raise ValueError('native final duration differs from captured clocks')
    for event in value['events']:
        if event['at'] != event['clock']['at'] or event['offsetMs'] != event['clock']['monotonicAfterMs'] - origin['monotonicAfterMs']:
            raise ValueError('native event differs from its captured clocks')
    return {'origin_utc': origin['at'], 'anchor_monotonic_ms': anchor, 'offset_lower_ms': lower, 'offset_upper_ms': upper,
            'upper_exclusive': True, 'capture_widths_ms': [a - b for b, a in windows]}


def timeline(path, native, *, document=None):
    value = read_json(path) if document is None else document
    if value.get('passed') is not True or value['settings'] != {key: native['settings'][key] for key in ('samples', 'concurrency', 'trials', 'cache')}:
        raise ValueError('native timeline completion/settings differ')
    events, stack, pairs = value['events'], [], {}
    if not events or len(events) > 1024: raise ValueError('native timeline absent/unbounded')
    clock = clock_intersection(value)
    previous = 0
    for index, event in enumerate(events):
        offset = event['offsetMs']
        if event['sequence'] != index or type(offset) not in (int, float) or not math.isfinite(offset) or not previous <= offset <= value['durationMs']:
            raise ValueError('native timeline ordering/clock differs')
        previous = offset
        if event['edge'] == 'start':
            if event['span'] in pairs or event['parent'] != (stack[-1] if stack else None): raise ValueError('native timeline parent differs')
            pairs[event['span']] = {'start': event}; stack.append(event['span'])
        elif event['edge'] == 'finish':
            if not stack or stack.pop() != event['span'] or event['outcome'] != 'passed': raise ValueError('native timeline incomplete/failed')
            start = pairs[event['span']]['start']
            if any(start.get(key) != event.get(key) for key in ('parent', 'phase', 'trial', 'case')): raise ValueError('native timeline span differs')
            pairs[event['span']]['finish'] = event
        else: raise ValueError('native timeline edge differs')
    if stack: raise ValueError('native timeline has missing finishes')
    spans = list(pairs.values()); expected = {(row['trial'], row['name']) for row in native['results']}
    if len(expected) != len(native['results']) or {row['start'].get('trial') for row in spans if row['start']['phase'] == 'trial'} != set(range(1, native['settings']['trials'] + 1)):
        raise ValueError('native timeline trials/results differ')
    for phase in ('case', 'warmup', 'samples'):
        actual = [(row['start'].get('trial'), row['start'].get('case')) for row in spans if row['start']['phase'] == phase]
        if len(actual) != len(expected) or set(actual) != expected: raise ValueError('native timeline missing/duplicate cases')
    return {'path': str(path), 'sha256': digest(path), 'events': len(events), 'spans': spans, 'clock': clock}


def join_samples(records, modes):
    joined = []
    for row in records:
        regions = []
        for mode in modes:
            clock = mode['timeline']['clock']; anchor = clock['anchor_monotonic_ms']
            origin = dt.datetime.fromisoformat(clock['origin_utc'].replace('Z', '+00:00'))
            start = (dt.datetime.fromisoformat(row['started_at'].replace('Z', '+00:00')) - origin).total_seconds() * 1000
            finish = (dt.datetime.fromisoformat(row['finished_at'].replace('Z', '+00:00')) - origin).total_seconds() * 1000
            for span in mode['timeline']['spans']:
                if span['start']['phase'] == 'benchmark': continue
                opening, closing = span['start']['clock'], span['finish']['clock']
                low = opening['monotonicBeforeMs'] - anchor + clock['offset_lower_ms']
                high = closing['monotonicAfterMs'] - anchor + clock['offset_upper_ms']
                if finish >= low and start < high:
                    after_start = opening['monotonicAfterMs'] - anchor + clock['offset_upper_ms']
                    before_finish = closing['monotonicBeforeMs'] - anchor + clock['offset_lower_ms']
                    regions.append({'mode': mode['mode'], 'span': span['start']['span'], 'phase': span['start']['phase'],
                        'trial': span['start'].get('trial'), 'case': span['start'].get('case'),
                        'relation': 'certainly-inside' if start >= after_start and finish <= before_finish else 'crossing-or-uncertain'})
        joined.append({**row, 'native_regions': regions})
    return joined


