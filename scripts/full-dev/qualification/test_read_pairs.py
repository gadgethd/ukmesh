"""Behavioral evidence tests for fixed matched read windows; no app traffic."""
import copy
import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

import common
import companion_reads
import read_measurements as measured
from read_protocol import IDENTITY, PROFILE, STEPS
import read_timeline
import report
import benchmarks
import matched_demand
import source_identity
from test_matched_demand import cohorts
from test_role_evidence import database_identity, role_context


class ReadPairs(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(patch.stopall)
        patch.object(companion_reads, 'EVIDENCE', self.root).start()
        patch.object(common, 'EVIDENCE', self.root).start()
        self.base = dt.datetime(2026, 11, 14, tzinfo=dt.timezone.utc)
        self.sources = {label: {'source_root': '/' + label, 'revision': label, 'sha256': label, 'files': 1}
                        for label in ('baseline', 'candidate')}

    def native_timeline(self, base, settings):
        at = lambda n: (base + dt.timedelta(milliseconds=n)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        capture = lambda n: {'at': at(n), 'monotonicBeforeMs': n, 'monotonicAfterMs': n}
        events, spans, offset = [], [], 0
        def edge(phase, trial=None, case=None, end=False):
            nonlocal offset
            offset += 1
            if end: span, parent = spans.pop()
            else:
                span = sum(row['edge'] == 'start' for row in events); parent = spans[-1][0] if spans else None
                spans.append((span, parent))
            events.append({'phase': phase, 'trial': trial, 'case': case, 'span': span, 'parent': parent,
                'sequence': len(events), 'edge': 'finish' if end else 'start', 'at': at(offset), 'offsetMs': offset,
                'clock': capture(offset), **({'outcome': 'passed'} if end else {})})
        edge('benchmark')
        for trial in (1, 2, 3):
            edge('trial', trial); edge('case', trial, 'map')
            for phase in ('warmup', 'samples'):
                edge(phase, trial, 'map'); edge(phase, trial, 'map', True)
            edge('case', trial, 'map', True); edge('trial', trial, end=True)
        edge('benchmark', end=True)
        return {'version': 2, 'passed': True, 'maxEvents': 1024, 'settings': {k: settings[k] for k in ('samples', 'trials', 'concurrency', 'cache')},
            'originClock': capture(0), 'originMonotonicMs': 0, 'startedAt': at(0), 'finishedClock': capture(offset+1),
            'finishedAt': at(offset+1), 'durationMs': offset+1, 'events': events}

    def windows(self, *, diagnostic=False, scales=('1x', '10x'), companion_values=None):
        runs = []; count = 0
        for scale in scales:
            batch = uuid.uuid4().hex
            context = {'run_id': self.root.name, 'source': {'root_content_sha256': 'source', 'submodules_sha256': 'sub'},
                       'profile_sha256': 'profile', **role_context(scale), 'runtime': {}}
            for step in STEPS:
                index = count; count += 1; base = self.base + dt.timedelta(seconds=index*10)
                at = lambda n: (base + dt.timedelta(seconds=n)).isoformat()
                side = step['side']; label = 'candidate' if diagnostic else PROFILE['source_labels'][side]
                ms = 10 if companion_values is None else companion_values(side, step['pair'])
                window = {**step, 'batch_id': batch, 'id': uuid.uuid4().hex}
                record = {'scale': scale, 'label': label, 'fixture_token': scale, 'measured_source': self.sources[label],
                    'context': context, 'context_before': context, 'protocol': IDENTITY, 'read_window': window,
                    'diagnostic_only': diagnostic, 'passed': True, 'modes': []}
                scopes = [('setup', 0), ('c1-miss', .1), ('between', .3), ('c1-warm', .4), ('between', .5),
                          ('c8-miss', .6), ('between', .7), ('c8-warm', .8), ('between', 1.5), ('teardown', 2)]
                boundaries = [{'id': i, 'scope': scope, 'offset_seconds': n, 'at': at(n)} for i, (scope, n) in enumerate(scopes)]
                scope_at = lambda n: max(v['id'] for v in boundaries if v['offset_seconds'] <= n)
                rows = []
                for actor, name in companion_reads.COMPANION_ACTORS.items():
                    for sequence in range(6):
                        start = .05 + sequence*.25; end = start+ms/1000
                        rows.append({'actor': actor, 'name': name, 'sequence': sequence, 'passed': True, 'ms': ms,
                            'start_offset_seconds': start, 'end_offset_seconds': end, 'started_at': at(start), 'finished_at': at(end),
                            'scope_at_start': scope_at(start), 'scope_at_finish': scope_at(end)})
                collector = {'collector_id': uuid.uuid4().hex, 'actors': companion_reads.COMPANION_ACTORS,
                    'cadence_seconds': .25, 'attempted': {actor: 6 for actor in companion_reads.COMPANION_ACTORS},
                    'origin_monotonic': 100+index*10, 'started_at': at(0), 'stop_requested_at': at(2), 'stop_requested_offset_seconds': 2,
                    'finished_at': at(2.5), 'finished_offset_seconds': 2.5, 'boundaries': boundaries, 'records': rows}
                output = self.root / f'window-{index}'; output.mkdir()
                raw = {'version': 1, **{key: record[key] for key in ('scale', 'label', 'fixture_token', 'measured_source', 'context', 'protocol', 'read_window', 'diagnostic_only')},
                       'collector': collector, 'summary': companion_reads.companion_summary(rows)}
                common.write_json(output/'companions.json', raw)
                record.update(companions=raw['summary'], companion_evidence={'path': str(output/'companions.json'),
                    'sha256': common.digest(output/'companions.json'), 'collector_id': collector['collector_id']})
                for mode in PROFILE['modes']:
                    c, cache = mode['concurrency'], mode['cache']; directory = output/f'c{c}-{cache}'; directory.mkdir()
                    settings = {**mode, 'samples': PROFILE['samples'][str(c)], 'trials': 3, 'productionRateLimits': True, 'clients': 128}
                    results = [{'trial': trial, 'name': 'map', 'samples': settings['samples'], 'p95Ms': 500 if label == 'baseline' else 40} for trial in (1, 2, 3)]
                    mode_base = base + dt.timedelta(seconds=next(n for name,n in scopes if name == f'c{c}-{cache}') + .01)
                    native = {'at': (mode_base+dt.timedelta(milliseconds=20)).isoformat(), 'timeline': 'timeline.json', 'passed': True, 'sourceIdentity': self.sources[label], 'sourceIdentityAfter': self.sources[label],
                              'sourceStable': True, 'settings': settings, 'results': results,
                              'databaseIdentity': database_identity(context)}
                    common.write_json(directory/'lookups.json', native)
                    common.write_json(directory/'timeline.json', self.native_timeline(mode_base, settings))
                    record['modes'].append({**mode, 'passed': True, 'exit_code': 0, 'source_identity': self.sources[label],
                        'source_stable': True, 'settings': settings, 'results': results, 'source': str(directory/'lookups.json'),
                        'evidence_sha256': common.digest(directory/'lookups.json'), 'timeline_sha256': common.digest(directory/'timeline.json')})
                runs.append(record)
        return runs

    def complete_summary(self, runs):
        demand=cohorts(self.root,runs)
        result = {**measured.read_comparison(runs,demand_runs=demand), 'runs': runs, 'demand_runs': demand,
                  'context': runs[-1]['context'], 'batches': []}
        for scale in ('1x', '10x'):
            rows = [r for r in runs if r['scale'] == scale]; path = self.root/f'batch-{scale}.json'
            batch = {'passed': True, 'diagnostic_only': False, 'protocol': IDENTITY, 'batch_id': rows[0]['read_window']['batch_id'],
                'runs': rows, 'context': rows[0]['context'], 'context_before': rows[0]['context'], 'collection_seconds': 60,
                'demand_runs': [row for row in demand if row['scale']==scale],
                'demand_collection_seconds': 800, 'demand_collection_budget_seconds': PROFILE['demand']['collection_budget_seconds'],
                'collection_budget_seconds': PROFILE['collection_budget_seconds'], 'errors': [], 'final_context_error': None}
            common.write_json(path, batch)
            result['batches'].append({'passed': True, 'scale': scale, 'batch_id': batch['batch_id'], 'path': str(path), 'sha256': common.digest(path)})
        return result

    def test_complete_pairs_keep_baseline_unbounded_and_all_nine_candidate_trials(self):
        runs = self.windows(); value = measured.native_read_comparison(runs)
        self.assertTrue(value['passed'], value['missing'])
        self.assertEqual(len(value['companion_comparisons']), 4)
        self.assertTrue(all(row['nine_trials'] and len(row['candidate_trials_ms']) == 9 for row in value['comparisons']))
        self.assertTrue(report.phase_check('reads', self.complete_summary(runs))[0])
        source_identity.validate_measurements(runs, self.sources)

    def test_rehashed_native_role_proof_is_required_for_baseline_and_candidate(self):
        runs = self.windows(scales=('1x',))
        self.assertTrue(measured.native_read_comparison(runs, scales=('1x',))['passed'])
        for row in runs[:2]:
            mode = row['modes'][0]
            path = Path(mode['source']); original = path.read_bytes()
            for fault in ('missing', 'legacy', 'consistent-admin', 'after-store'):
                value = json.loads(original)
                if fault == 'missing': value.pop('databaseIdentity')
                elif fault == 'legacy': value['databaseIdentity']['scope'] = 'legacy-inspector-readonly'
                elif fault == 'consistent-admin':
                    value['databaseIdentity']['expected']['http']['user'] = 'meshcore'
                    for phase in ('setup', 'after'):
                        value['databaseIdentity'][phase]['http']['current_user'] = 'meshcore'
                        value['databaseIdentity'][phase]['http']['session_user'] = 'meshcore'
                else: value['databaseIdentity']['after']['http']['database'] = 'meshcore'
                try:
                    common.write_json(path, value); mode['evidence_sha256'] = common.digest(path)
                    result = measured.native_read_comparison(runs, scales=('1x',))
                    with self.subTest(label=row['label'], fault=fault):
                        self.assertFalse(result['valid'])
                        self.assertIn('application-role', result['missing'][0])
                finally:
                    path.write_bytes(original); mode['evidence_sha256'] = common.digest(path)
        self.assertTrue(measured.native_read_comparison(runs, scales=('1x',))['passed'])

    def test_all_raw_pooling_is_primary_and_never_median_of_window_p95(self):
        runs = self.windows(scales=('10x',), companion_values=lambda side, pair: 100 if side == 'A' and pair == 3 else 10 if side == 'A' else 20)
        value = measured.native_read_comparison(runs, scales=('10x',))
        self.assertTrue(value['passed'], value['missing'])
        self.assertTrue(all(row['baseline_p95_ms'] == 100 and row['candidate_p95_ms'] == 20 for row in value['companion_comparisons']))

    def test_fixed_demand_primary_is_separate_and_saturated_failures_stay_visible(self):
        runs=self.windows(scales=('10x',),companion_values=lambda side,pair:20 if side=='B' else 10)
        demand=cohorts(self.root,runs)
        result=measured.read_comparison(runs,scales=('10x',),demand_runs=demand)
        self.assertTrue(result['passed'],result['missing'])
        self.assertTrue(all(not row['passed'] for row in result['saturated_companion_comparisons']))
        self.assertTrue(all(row['passed'] for row in result['companion_comparisons']))
        self.assertFalse(measured.read_comparison(runs,scales=('10x',))['passed'])
        demand[0]['measured_source']=self.sources['candidate']
        with self.assertRaises(ValueError):source_identity.validate_measurements(demand,self.sources)

    def test_one_of_nine_over_budget_or_actual_11_percent_companion_shift_fails(self):
        runs = self.windows(); row = next(v for v in runs if v['label'] == 'candidate')
        mode = row['modes'][0]; mode['results'][0]['p95Ms'] = 101
        p = Path(mode['source']); native = common.read_json(p); native['results'] = mode['results']; common.write_json(p, native); mode['evidence_sha256'] = common.digest(p)
        self.assertFalse(measured.native_read_comparison(runs)['passed'])
        # Separate fresh evidence demonstrates the actual regression gate, with valid native timings.
        with tempfile.TemporaryDirectory() as other:
            self.root = Path(other)
            with patch.object(companion_reads, 'EVIDENCE', self.root), patch.object(common, 'EVIDENCE', self.root):
                shifted = self.windows(scales=('10x',), companion_values=lambda side, pair: 11.1 if side == 'B' and pair == 3 else 10)
                result = measured.native_read_comparison(shifted, scales=('10x',))
                self.assertTrue(result['valid'], result['missing']); self.assertTrue(result['passed'])
                self.assertTrue(any(not row['passed'] for row in result['companion_comparisons']))
                self.assertFalse(measured.read_comparison(shifted,scales=('10x',))['passed'], 'missing matched demand cannot qualify')

    def test_missing_duplicate_wrong_order_cadence_diagnostic_and_native_digest_are_refused(self):
        original = self.windows()
        for mutation in ('missing', 'duplicate', 'order', 'cadence', 'diagnostic', 'digest', 'protocol', 'source'):
            runs = copy.deepcopy(original); restore = None
            if mutation == 'missing': runs.pop()
            elif mutation == 'duplicate': runs[-1]['read_window']['id'] = runs[-2]['read_window']['id']
            elif mutation == 'order': runs[0], runs[1] = runs[1], runs[0]
            elif mutation == 'diagnostic': runs[0]['diagnostic_only'] = True
            elif mutation == 'digest': runs[0]['modes'][0]['evidence_sha256'] = '0'*64
            elif mutation == 'protocol': runs[0]['protocol'] = {'version': 2}
            elif mutation == 'source': runs[0]['measured_source'] = self.sources['candidate']
            else:
                p = Path(runs[0]['companion_evidence']['path']); restore = (p, p.read_bytes()); raw = common.read_json(p); raw['collector']['cadence_seconds'] = 1
                common.write_json(p, raw); runs[0]['companion_evidence']['sha256'] = common.digest(p)
            try:
                with self.subTest(mutation=mutation): self.assertFalse(measured.read_comparison(runs)['passed'])
            finally:
                if restore: restore[0].write_bytes(restore[1])

    def test_older_valid_or_other_mode_timeline_cannot_serve_current_mode(self):
        runs = self.windows(scales=('10x',)); row = runs[1]; mode = row['modes'][1]
        target = Path(mode['source']).parent/'timeline.json'; original = target.read_bytes()
        other = Path(runs[0]['modes'][1]['source']).parent/'timeline.json'
        target.write_bytes(other.read_bytes()); mode['timeline_sha256'] = common.digest(target)
        value = measured.read_comparison(runs, scales=('10x',))
        self.assertFalse(value['passed']); self.assertIn('mode interval', value['missing'][0])
        target.write_bytes(original)

    def test_native_observer_proofs_must_match_the_mode_and_paired_workset(self):
        runs = self.windows(scales=('10x',))

        def set_workset(window, sha256):
            window['observer_workset_sha256'] = sha256
            for index, mode in enumerate(window['modes']):
                file = Path(mode['source'])
                native = common.read_json(file)
                native['observerWorkset'] = {'sha256': sha256, 'reused': index > 0}
                common.write_json(file, native)
                mode['evidence_sha256'] = common.digest(file)
                mode['observer_workset_verified'] = True

        for window in runs:
            set_workset(window, 'a' * 64)
        self.assertTrue(measured.native_read_comparison(runs, scales=('10x',))['passed'])

        mode = runs[0]['modes'][0]
        file = Path(mode['source'])
        native = common.read_json(file)
        native['observerWorkset']['reused'] = True
        common.write_json(file, native)
        mode['evidence_sha256'] = common.digest(file)
        with self.assertRaisesRegex(ValueError, 'native mode observer proof differs'):
            measured.native_evidence(mode, runs[0])

        set_workset(runs[0], 'a' * 64)
        del runs[0]['observer_workset_sha256']
        with self.assertRaisesRegex(ValueError, 'native mode observer proof differs'):
            measured.native_evidence(mode, runs[0])

        set_workset(runs[0], 'a' * 64)
        set_workset(runs[-1], 'b' * 64)
        result = measured.native_read_comparison(runs, scales=('10x',))
        self.assertFalse(result['passed'])
        self.assertIn('observer workset differs across paired windows', result['missing'][0])

    def test_report_recomputes_source_and_batch_closure_instead_of_trusting_green(self):
        runs = self.windows(); value = self.complete_summary(runs); common.write_json(self.root/'reads.json', value)
        with patch.object(report, 'REQUIRED', ('reads',)), patch.object(report, 'phase_context', return_value=value['context']), \
             patch.object(source_identity, 'expected_sources', return_value=self.sources):
            self.assertTrue(report.report(self.root)['passed'])
            bad = copy.deepcopy(value); bad['runs'][0]['measured_source'] = self.sources['candidate']; common.write_json(self.root/'reads.json', bad)
            self.assertFalse(report.report(self.root)['passed'])
            bad = copy.deepcopy(value); bad['demand_runs'][0]['measured_source'] = self.sources['candidate']; common.write_json(self.root/'reads.json', bad)
            self.assertFalse(report.report(self.root)['passed'])
            bad = copy.deepcopy(value); path = Path(bad['batches'][0]['path']); batch = common.read_json(path); batch['final_context_error'] = 'inspection failed'
            common.write_json(path, batch); bad['batches'][0]['sha256'] = common.digest(path); common.write_json(self.root/'reads.json', bad)
            self.assertFalse(report.report(self.root)['passed'])

    def test_mqtt_first_report_uses_real_native_validators_and_preserves_full_profile(self):
        runs = self.windows()
        value = {**measured.native_read_comparison(runs), 'runs': runs, 'demand_runs': [],
                 'native_only': True, 'context': runs[-1]['context'], 'batches': []}
        for scale in ('1x', '10x'):
            rows = [r for r in runs if r['scale'] == scale]
            path = self.root / ('native-batch-' + scale + '.json')
            batch = {'passed': True, 'diagnostic_only': False, 'native_only': True, 'protocol': IDENTITY,
                     'batch_id': rows[0]['read_window']['batch_id'], 'runs': rows, 'demand_runs': [],
                     'context': rows[0]['context'], 'context_before': rows[0]['context'],
                     'collection_seconds': 60, 'collection_budget_seconds': PROFILE['collection_budget_seconds'],
                     'demand_collection_seconds': 0, 'demand_collection_budget_seconds': PROFILE['demand']['collection_budget_seconds'],
                     'errors': [], 'final_context_error': None}
            common.write_json(path, batch)
            value['batches'].append({'passed': True, 'scale': scale, 'batch_id': batch['batch_id'],
                                      'path': str(path), 'sha256': common.digest(path)})
        common.write_json(self.root / 'reads-native.json', value)
        before = (self.root / 'reads-native.json').read_bytes()
        common.write_json(self.root / 'report.json', {'passed': False, 'historical': 'full-stack'})
        old_report = (self.root / 'report.json').read_bytes()
        with patch.object(report, 'REQUIRED', ('reads',)), patch.object(report, 'phase_context', return_value=value['context']), \
                patch.object(source_identity, 'expected_sources', return_value=self.sources):
            result = report.report(self.root, acceptance_profile='mqtt-first')
            self.assertTrue(result['passed'])
            self.assertEqual(result['acceptance_profile'], 'mqtt-first')
            self.assertTrue((self.root / 'report-mqtt-first.json').is_file())
            self.assertEqual(old_report, (self.root / 'report.json').read_bytes())
            self.assertFalse(report.report(self.root)['passed'])
            self.assertEqual(before, (self.root / 'reads-native.json').read_bytes())
            wrong = copy.deepcopy(value)
            wrong['runs'][0]['measured_source'] = self.sources['candidate']
            common.write_json(self.root / 'reads-native.json', wrong)
            self.assertFalse(report.report(self.root, acceptance_profile='mqtt-first')['passed'])

    def test_owned_native_document_refuses_fifo_symlink_and_changed_digest(self):
        path = self.root/'evidence.json'; common.write_json(path, {'owned': True}); sha = common.digest(path)
        self.assertEqual(measured.owned_document(path, sha), {'owned': True})
        with self.assertRaises(ValueError): measured.owned_document(path, '0'*64)
        link = self.root/'link.json'; link.symlink_to(path)
        with self.assertRaises(OSError): measured.owned_document(link, sha)
        fifo = self.root/'fifo.json'; os.mkfifo(fifo)
        with self.assertRaises(ValueError): measured.owned_document(fifo, sha)

    def test_candidate_aliases_only_pass_explicit_symmetric_diagnostic_and_never_report(self):
        runs = self.windows(diagnostic=True, scales=('10x',))
        self.assertTrue(measured.read_comparison(runs, scales=('10x',), diagnostic_only=True)['passed'])
        self.assertFalse(measured.read_comparison(runs, scales=('10x',))['passed'])
        self.assertFalse(report.phase_check('reads', {'passed': True, 'diagnostic_only': True, 'protocol': IDENTITY, 'runs': runs})[0])
        source_identity.validate_measurements(runs, self.sources)
        runs[0]['label'] = 'baseline'
        with self.assertRaises(ValueError): source_identity.validate_measurements(runs, self.sources)

    def test_shared_collector_defaults_to_one_second_and_paired_cadence_is_explicit(self):
        counts = {}; ready = threading.Event(); lock = threading.Lock()
        def request(_):
            actor = threading.get_ident()
            with lock:
                counts[actor] = counts.get(actor, 0) + 1
                if len(counts) == 4 and min(counts.values()) >= 2: ready.set()
            return b'{}', 'application/json'
        default = companion_reads.CompanionReads(request)
        self.assertEqual(default.cadence, 1); default.finish()
        collector = companion_reads.CompanionReads(request, cadence_seconds=PROFILE['companion_cadence_seconds'])
        collector.start()
        try:
            self.assertTrue(ready.wait(1.5))
            for scope in ('c1-miss', 'between', 'c1-warm', 'between', 'c8-miss', 'between', 'c8-warm', 'between', 'teardown'):
                collector.set_scope(scope)
        finally: summary = collector.finish()
        raw = {'collector': collector.evidence(), 'summary': summary}
        self.assertEqual(raw['collector']['cadence_seconds'], .25)
        self.assertEqual(companion_reads.validate_companion_samples(raw, cadence_seconds=.25), summary)
        with self.assertRaises(ValueError): companion_reads.validate_companion_samples(raw)

    def test_same_source_control_applies_the_pooled_ratio_in_both_directions(self):
        runs = self.windows(diagnostic=True, scales=('10x',), companion_values=lambda side, pair: 8.9 if side == 'B' else 10)
        value = measured.read_comparison(runs, scales=('10x',), diagnostic_only=True)
        self.assertTrue(value['valid'], value['missing']); self.assertFalse(value['passed'])

    def test_clock_intervals_preserve_preemption_and_reject_touching_or_inconsistent_offsets(self):
        def evidence(captures):
            rows = [{'at': f'2026-11-14T00:00:00.{wall:03d}Z', 'monotonicBeforeMs': before, 'monotonicAfterMs': after} for before, after, wall in captures]
            return {'version': 2, 'startedAt': rows[0]['at'], 'originMonotonicMs': rows[0]['monotonicAfterMs'], 'originClock': rows[0],
                'finishedAt': rows[-1]['at'], 'finishedClock': rows[-1], 'durationMs': rows[-1]['monotonicAfterMs']-rows[0]['monotonicAfterMs'],
                'events': [{'at': row['at'], 'offsetMs': row['monotonicAfterMs']-rows[0]['monotonicAfterMs'], 'clock': row} for row in rows[1:-1]]}
        for captures in ([(0,2.4,0),(3,3,3),(4,4,4)], [(0,0,0),(3,13,3),(14,14,14)]):
            value = read_timeline.clock_intersection(evidence(captures))
            self.assertEqual((value['offset_lower_ms'], value['offset_upper_ms']), (0,1))
        for captures in ([(0,0,0),(1,1,2),(2,2,3)], [(0,100,0),(101,101,2),(102,102,4)], [(0,0,0),(2,2,2),(3,3,1)]):
            with self.assertRaises(ValueError): read_timeline.clock_intersection(evidence(captures))

    def test_read_once_hands_owned_context_to_real_native_evidence_for_all_modes(self):
        context = {'run_id': self.root.name, **role_context('1x')}
        fixture = {'kind': '1x', 'token': 'owned-synthetic-1x', 'databases': [
            {'label': label, 'database': database} for label, database in context['targets'].items()]}
        for label in ('baseline', 'candidate'):
            with self.subTest(label=label):
                base = self.base
                collectors, invocations, observer_receipts = [], [], []

                class Collector:
                    def __init__(self, request, *, cadence_seconds):
                        self.origin = None
                        self.collector_id = uuid.uuid4().hex
                        self.boundaries, self.records = [], []
                        self.finished = False
                        collectors.append(self)

                    def set_scope(self, scope):
                        offset = len(self.boundaries)
                        self.boundaries.append({'id': offset, 'scope': scope, 'offset_seconds': offset,
                            'at': (base + dt.timedelta(seconds=offset)).isoformat()})

                    def start(self):
                        self.origin = 100
                        self.set_scope('setup')
                        self.records = [{'actor': actor, 'name': name, 'passed': True, 'ms': 1,
                            'sequence': 0, 'start_offset_seconds': .05, 'end_offset_seconds': .051,
                            'started_at': (base + dt.timedelta(seconds=.05)).isoformat(),
                            'finished_at': (base + dt.timedelta(seconds=.051)).isoformat(),
                            'scope_at_start': 0, 'scope_at_finish': 0}
                            for actor, name in companion_reads.COMPANION_ACTORS.items()]

                    def finish(self):
                        self.finished = True
                        return companion_reads.companion_summary(self.records)

                    def evidence(self):
                        return {'collector_id': self.collector_id, 'boundaries': self.boundaries,
                                'records': self.records}

                def native(tool, args, database, log, *, timeout):
                    self.assertEqual(tool, 'benchmarkLookups')
                    self.assertEqual(database, context['targets']['ukmesh'])
                    setting = lambda name: args[args.index('--' + name) + 1]
                    concurrency, cache = int(setting('concurrency')), setting('cache')
                    self.assertEqual((concurrency, cache),
                        tuple(PROFILE['modes'][len(invocations)][key] for key in ('concurrency', 'cache')))
                    reuse = bool(invocations)
                    receipt = Path(setting('observer-receipt'))
                    self.assertEqual('--reuse-observer-receipt' in args, reuse)
                    if reuse:
                        self.assertEqual(receipt, observer_receipts[0])
                    else:
                        self.assertFalse(receipt.exists())
                        common.write_json(receipt, {'observer': 'fixture-observer'})
                    observer_receipts.append(receipt)
                    invocations.append((concurrency, cache))
                    settings = {'samples': int(setting('samples')), 'trials': int(setting('trials')),
                                'concurrency': concurrency, 'cache': cache}
                    self.assertEqual(settings['samples'], PROFILE['samples'][str(concurrency)])
                    self.assertEqual(settings['trials'], PROFILE['native_trials'])
                    output = Path(setting('output')); output.mkdir()
                    opening = dt.datetime.fromisoformat(collectors[-1].boundaries[-1]['at'])
                    native_base = opening + dt.timedelta(milliseconds=10)
                    value = {'at': (native_base + dt.timedelta(milliseconds=20)).isoformat(),
                        'timeline': 'timeline.json', 'passed': True,
                        'sourceIdentity': self.sources[label], 'sourceIdentityAfter': self.sources[label],
                        'sourceStable': True, 'databaseIdentity': database_identity(context), 'settings': settings,
                        'observerWorkset': {'sha256': common.digest(receipt), 'reused': reuse},
                        'results': [{'trial': trial, 'name': 'map', 'samples': settings['samples'], 'p95Ms': 40}
                                    for trial in range(1, settings['trials'] + 1)]}
                    common.write_json(output / 'lookups.json', value)
                    common.write_json(output / 'timeline.json', self.native_timeline(native_base, settings))
                    return 0

                with patch.object(benchmarks, 'active_fixture', return_value=fixture), \
                     patch.object(benchmarks, 'expected_sources', return_value=self.sources), \
                     patch.object(benchmarks, 'inventory', return_value={'passed': True}), \
                     patch.object(benchmarks, 'phase_context', return_value=context), \
                     patch.object(benchmarks, 'CompanionReads', Collector), \
                     patch.object(benchmarks, 'lookup_query_counters', return_value={'error': 'Unavailable'}), \
                     patch.object(benchmarks, 'node_tool', side_effect=native):
                    record = benchmarks.read_once(self.root, label=label,
                        source_root=Path(self.sources[label]['source_root']))
                self.assertEqual(invocations, [(mode['concurrency'], mode['cache']) for mode in PROFILE['modes']])
                self.assertEqual(len(collectors), 1)
                self.assertTrue(collectors[0].finished)
                self.assertTrue(record['passed'])
                self.assertEqual(record['context_before'], context)
                self.assertEqual(record['context'], context)
                self.assertEqual(len(record['modes']), 4)
                self.assertFalse(observer_receipts[0].parent.exists())
                self.assertRegex(record['observer_workset_sha256'], '^[a-f0-9]{64}$')
                self.assertTrue(all(mode['observer_workset_verified'] for mode in record['modes']))
                summary = common.read_json(self.root / f'reads-1x-{label}/summary.json')
                self.assertTrue(summary['passed'])
                self.assertEqual(summary['errors'], [])
                for mode in record['modes']:
                    self.assertEqual(mode['postgresql_query_deltas'],
                                     {'before_error': 'Unavailable', 'after_error': 'Unavailable'})
                    native_value = common.read_json(Path(mode['source']))
                    self.assertEqual(native_value['databaseIdentity']['expected']['http']['database'],
                                     context['targets']['ukmesh'])
                    self.assertEqual(native_value['databaseIdentity']['setup']['http']['current_user'], 'meshcore_api')
                    proof = measured.native_evidence(mode, record, {'boundaries': collectors[0].boundaries})
                    self.assertGreater(proof['events'], 0)

    def test_final_context_failure_or_late_completion_cannot_publish_green(self):
        for fault in ('context', 'deadline'):
            with tempfile.TemporaryDirectory() as temporary:
                run = Path(temporary); produced = []; closed = False; clock = [0]
                context = {'run_id': run.name}; fixture = {'kind': '10x'}
                def read(*args, **kwargs):
                    row = {'scale': '10x', 'passed': True, 'label': kwargs['label'], 'read_window': kwargs['window']}
                    produced.append(row); return row
                def current(_):
                    nonlocal closed
                    if len(produced) == 6:
                        if closed and fault == 'context': raise RuntimeError('final inspection failed')
                        closed = True
                    return context
                def comparison(rows, **kwargs):
                    if len(produced) == 6 and fault == 'deadline': clock[0] = PROFILE['collection_budget_seconds'] + 1
                    return {'passed': True, 'valid': True, 'protocol': IDENTITY, 'diagnostic_only': False, 'missing': []}
                with patch.object(benchmarks, 'active_fixture', return_value=fixture), patch.object(benchmarks, 'expected_sources', return_value=self.sources), \
                     patch.object(benchmarks, 'phase_context', side_effect=current), patch.object(benchmarks, 'read_once', side_effect=read), \
                     patch.object(benchmarks, 'read_comparison', side_effect=comparison), patch.object(benchmarks, 'validate_measurements'), \
                     patch.object(benchmarks.time, 'monotonic', side_effect=lambda: clock[0]):
                    with self.assertRaises((RuntimeError, TimeoutError)): benchmarks.reads_pair(run)
                self.assertEqual(len(produced), 6)
                self.assertFalse(common.read_json(run/'reads.json')['passed'])
                batch = next(run.glob('reads-10x-paired-*'))
                self.assertFalse(common.read_json(batch/'summary.json')['passed'])
                self.assertFalse(common.read_json(batch/'progress.json')['passed'])

    def test_all_six_demand_cohorts_cannot_publish_success_before_final_context_and_deadline(self):
        for fault in ('context','deadline'):
            with tempfile.TemporaryDirectory() as temporary:
                run=Path(temporary);native=[];demand=[];clock=[0];closed=[False];context={'run_id':run.name}
                def read(*args,**kwargs):
                    row={'scale':'10x','passed':True,'label':kwargs['label'],'read_window':kwargs['window']}
                    native.append(row);return row
                def cohort(run,fixture,source,label,directory,window,context,deadline,node_tool):
                    row={'scale':'10x','passed':True,'label':label,'read_window':window};demand.append(row);return row
                def current(_):
                    if len(demand)==6:
                        if closed[0] and fault=='context':raise RuntimeError('final demand inspection failed')
                        closed[0]=True
                    return context
                def comparison(*args,**kwargs):
                    if len(demand)==6 and fault=='deadline':clock[0]=PROFILE['demand']['collection_budget_seconds']+1
                    return {'passed':True,'valid':True,'protocol':IDENTITY,'diagnostic_only':False,'missing':[]}
                with patch.object(benchmarks,'active_fixture',return_value={'kind':'10x'}),patch.object(benchmarks,'expected_sources',return_value=self.sources), \
                     patch.object(benchmarks,'phase_context',side_effect=current),patch.object(benchmarks,'read_once',side_effect=read), \
                     patch.object(matched_demand,'collect_cohort',side_effect=cohort),patch.object(benchmarks,'validate_measurements'), \
                     patch.object(benchmarks,'native_read_comparison',return_value={'valid':True}),patch.object(benchmarks,'read_comparison',side_effect=comparison), \
                     patch.object(benchmarks.time,'monotonic',side_effect=lambda:clock[0]):
                    with self.assertRaises((RuntimeError,TimeoutError)):benchmarks.reads_pair(run)
                self.assertEqual(len(native),6);self.assertEqual(len(demand),6)
                self.assertFalse(common.read_json(run/'reads.json')['passed'])
                path=next(run.glob('reads-10x-paired-*'))
                self.assertFalse(common.read_json(path/'summary.json')['passed'])
                self.assertFalse(common.read_json(path/'demand-progress.json')['passed'])

if __name__ == '__main__': unittest.main()
