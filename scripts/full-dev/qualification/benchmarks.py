"""Reuse the production-route lookup benchmark with the complete stack resident."""

from __future__ import annotations

import json
from pathlib import Path
import time
import uuid

from common import REPO, STATE, digest, command, read_json, safe_error, utc_now, write_json
from companion_reads import CompanionReads, companion_summary
from context import phase_context
from fixtures import bundle, current_targets
from probes import inventory
from loopback_http import http_get
from source_identity import expected_sources, identify, validate_measurements
from read_protocol import IDENTITY, PROFILE, STEPS
from read_measurements import native_evidence, native_read_comparison, read_comparison, validate_read_batches
from node_invocation import tool_category, tool_environment


def active_fixture() -> dict:
    selected = read_json(STATE / "runtime/selected-fixture.json")
    current_targets()
    from probes import inspected_containers
    from targets import runtime_targets
    runtime_targets(inspected_containers())
    return bundle(selected["token"])


def node_tool(tool: str, args: list[str], database: str, log: Path, *, timeout: int = 1800) -> int:
    category = tool_category(tool)
    fixture = active_fixture()
    if category == 'application-read':
        from probes import inspected_containers
        settings, configuration = tool_environment(tool, database, fixture, containers=inspected_containers())
    else:
        # Existing owned fixture administrator definition; no second password
        # registry and no fallback from an application permission failure.
        endpoint = "postgresql://meshcore:meshcore@127.0.0.1:5433/" + database
        settings, configuration = tool_environment(tool, database, fixture, admin_endpoint=endpoint)
    invocation = ["node", "--max-old-space-size=512", "--import",
                  str(REPO / "backend/node_modules/tsx/dist/loader.mjs"),
                  str(REPO / f"backend/src/tools/{tool}.ts"), *args]
    log.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    write_json(log.with_suffix(log.suffix + ".invocation.json"), configuration)
    with log.open("xb") as output:
        log.chmod(0o600)
        result = command(invocation, output_stream=output, timeout=timeout, check=False,
                         environment_overrides=settings)
    if result.stderr:
        error = log.with_suffix(log.suffix + ".stderr")
        error.write_bytes(result.stderr)
        error.chmod(0o600)
    return result.returncode



def read_once(run: Path, *, label: str, source_root: Path, directory: Path | None = None,
              window: dict | None = None, diagnostic_only: bool = False, cadence_seconds: float = 1,
              deadline: float | None = None) -> dict:
    if label not in ('baseline', 'candidate') or (diagnostic_only and label != 'candidate'):
        raise ValueError('normal source authority is required; diagnostics use candidate only')
    fixture = active_fixture(); scale = fixture['kind']
    if scale not in PROFILE['budgets_ms']: raise ValueError('matched reads require retained1x/10x fixtures')
    measured = expected_sources()[label]
    if source_root.resolve() != Path(measured['source_root']):
        raise ValueError('selected lookup source differs from its baseline/candidate authority')
    if not inventory(run)['passed']: raise RuntimeError('complete service inventory must pass before timing')
    database = next(item['database'] for item in fixture['databases'] if item['label'] == 'ukmesh')
    directory = directory or run / f'reads-{scale}-{label}'
    if not directory.resolve().is_relative_to(run.resolve()) or directory.resolve() == run.resolve():
        raise ValueError('fresh read output must stay inside this evidence run')
    directory.mkdir(mode=0o700)
    context_before = phase_context(run); modes = []; error = None
    companions = CompanionReads(http_get, cadence_seconds=cadence_seconds)
    try:
        companions.start()
        for mode in PROFILE['modes']:
            concurrency, cache = mode['concurrency'], mode['cache']
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError('paired read collection budget exhausted')
            companions.set_scope(f'c{concurrency}-{cache}')
            output = directory / f'c{concurrency}-{cache}'
            args = ['--source-root', str(source_root), '--samples', str(PROFILE['samples'][str(concurrency)]),
                    '--trials', str(PROFILE['native_trials']), '--concurrency', str(concurrency), '--cache', cache,
                    '--output', str(output), '--reference-revision', measured['revision']]
            if label == 'candidate': args += ['--budget-ms', str(PROFILE['budgets_ms'][scale])]
            code = node_tool('benchmarkLookups', args, database, output.with_suffix('.log'),
                             timeout=1800 if deadline is None else max(.001, deadline-time.monotonic()))
            companions.set_scope('between')
            path = output / 'lookups.json'; value = read_json(path) if path.exists() else {}
            modes.append({**mode, 'exit_code': code, 'passed': code == 0 and bool(value.get('results'))
                and value.get('sourceIdentity') == measured and value.get('sourceIdentityAfter') == measured
                and value.get('sourceStable') is True and value.get('passed') is True,
                'source_identity': value.get('sourceIdentity'), 'source_stable': value.get('sourceStable'),
                'results': value.get('results', []), 'settings': value.get('settings', {}), 'source': str(path),
                'evidence_sha256': digest(path) if path.exists() else None,
                'timeline_sha256': digest(output/'timeline.json') if (output/'timeline.json').exists() else None})
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError('paired read collection budget exhausted')
            if not modes[-1]['passed']: raise RuntimeError('native read window failed; exact partial evidence retained')
            native_evidence(modes[-1], {'label': label, 'scale': scale, 'measured_source': measured,
                            'context': context_before},
                            {'boundaries': companions.boundaries})
    except BaseException as caught:
        error = caught
    finally:
        try:
            if companions.origin is not None: companions.set_scope('teardown')
        except BaseException as caught: error = caught
        try: companions.finish()
        except BaseException as caught: error = caught
        raw_context_error = None
        try: context = phase_context(run)
        except Exception as caught:
            context, raw_context_error = context_before, safe_error(caught)
            error = RuntimeError('companion observations retained but final runtime context failed')
        identity = {'scale': scale, 'label': label, 'fixture_token': fixture['token'], 'measured_source': measured,
                    'context': context, 'protocol': IDENTITY, 'read_window': window, 'diagnostic_only': diagnostic_only}
        totals = companion_summary(companions.records)
        minimum = PROFILE['minimum_requests_per_app_window'] if window is not None else 1
        raw = {'version': 1, **identity, 'collector': companions.evidence(), 'summary': totals, 'context_error': raw_context_error}
        raw_path = directory / 'companions.json'; write_json(raw_path, raw)
        record = {'at': utc_now(), **identity, 'context_before': context_before,
            'companion_evidence': {'path': str(raw_path), 'sha256': digest(raw_path), 'collector_id': companions.collector_id},
            'modes': modes, 'companions': totals, 'errors': [] if error is None else [type(error).__name__+': '+safe_error(error)],
            'passed': error is None and context_before == context and len(modes) == len(PROFILE['modes'])
                and all(mode['passed'] for mode in modes) and all(v['requests'] >= minimum and v['failures'] == 0 for v in totals.values())}
        write_json(directory / 'summary.json', record)
    if error is not None: raise error
    return record


def publish_reads(run, records):
    validate_measurements(records, expected_sources())
    comparison = read_comparison(records)
    value = {'at': utc_now(), 'context': phase_context(run), 'runs': records, **comparison}
    write_json(run / 'reads.json', value)
    return value


def reads(run: Path, *, label: str, source_root: Path) -> dict:
    record = read_once(run, label=label, source_root=source_root)
    path = run / 'reads.json'; prior = read_json(path).get('runs', []) if path.exists() else []
    publish_reads(run, prior + [record])
    return record


def reads_pair(run: Path, *, diagnostic_only: bool = False) -> dict:
    fixture = active_fixture(); scale = fixture['kind']; source = expected_sources()
    if scale not in PROFILE['budgets_ms']: raise ValueError('paired reads require retained1x/10x fixtures')
    context = phase_context(run); batch = uuid.uuid4().hex
    directory = run / f'reads-{scale}-paired-{batch}'
    directory.mkdir(mode=0o700)
    before = time.monotonic(); deadline = before + PROFILE['collection_budget_seconds']; records = []; error = None
    path = run / ('reads-control.json' if diagnostic_only else 'reads.json')
    prior_value = {} if diagnostic_only or not path.exists() else read_json(path)
    prior = prior_value.get('runs', [])
    prior_batches = prior_value.get('batches', [])
    prior_demand = prior_value.get('demand_runs', [])
    if prior and (not prior_batches or any(not batch.get('passed') for batch in prior_batches)):
        raise ValueError('previous read batch did not close successfully; archive its evidence first')
    if prior:
        validate_measurements(prior, source)
        validate_read_batches(prior_value, scales=tuple(sorted({row['scale'] for row in prior})))
    if prior and any(row['scale'] == scale for row in prior): raise ValueError('this scale already has retained read measurements; archive before another attempt')
    try:
        for step in STEPS:
            if time.monotonic() >= deadline: raise TimeoutError('paired read collection budget exhausted')
            if phase_context(run) != context: raise ValueError('source/runtime/fixture drift between paired windows')
            label = 'candidate' if diagnostic_only else PROFILE['source_labels'][step['side']]
            window = {**step, 'batch_id': batch, 'id': uuid.uuid4().hex}
            output = directory / f"window-{step['position']}-{step['side']}"
            try:
                record = read_once(run, label=label, source_root=Path(source[label]['source_root']), directory=output,
                    window=window, diagnostic_only=diagnostic_only,
                    cadence_seconds=PROFILE['companion_cadence_seconds'], deadline=deadline)
            except BaseException:
                if (output / 'summary.json').exists(): records.append(read_json(output / 'summary.json'))
                raise
            records.append(record)
            validate_measurements(records, source)
            if phase_context(run) != context: raise ValueError('source/runtime/fixture drift after paired window')
            if not record['passed']: raise RuntimeError('paired read window failed')
            write_json(directory / 'progress.json', {'passed': False, 'pending': True, 'protocol': IDENTITY,
                'diagnostic_only': diagnostic_only, 'batch_id': batch, 'runs': records})
    except BaseException as caught: error = caught
    native_elapsed = time.monotonic()-before
    if time.monotonic() >= deadline: error = error or TimeoutError('paired native read collection budget exhausted')
    demands = []; demand_before = time.monotonic(); demand_deadline = demand_before+PROFILE['demand']['collection_budget_seconds']
    if not diagnostic_only and error is None:
        from matched_demand import collect_cohort
        try:
            if not native_read_comparison(records, scales=(scale,))['valid']:
                raise ValueError('native absolute read gates or evidence failed')
            native_elapsed = time.monotonic()-before
            if time.monotonic() >= deadline: raise TimeoutError('native read closure exceeded collection budget')
            for step in STEPS:
                if time.monotonic() >= demand_deadline: raise TimeoutError('fixed-demand collection budget exhausted')
                if phase_context(run) != context: raise ValueError('runtime drift before fixed-demand cohort')
                label = PROFILE['source_labels'][step['side']]
                window = {**step, 'batch_id': batch, 'id': uuid.uuid4().hex}
                output = directory/f"demand-{step['position']}-{step['side']}"
                try:
                    record = collect_cohort(run, fixture, source[label], label, output, window, context, demand_deadline, node_tool)
                except BaseException:
                    if (output/'summary.json').exists(): demands.append(read_json(output/'summary.json'))
                    raise
                demands.append(record); validate_measurements(demands, source)
                if phase_context(run) != context: raise ValueError('runtime drift after fixed-demand cohort')
                write_json(directory/'demand-progress.json', {'passed': False, 'pending': True, 'runs': demands})
        except BaseException as caught: error = caught
    comparison = read_comparison(records, scales=(scale,), diagnostic_only=diagnostic_only, demand_runs=demands)
    canonical = None
    if not diagnostic_only:
        try:
            validate_measurements(prior + records, expected_sources())
            validate_measurements(prior_demand + demands, expected_sources())
            canonical = read_comparison(prior + records, demand_runs=prior_demand + demands)
        except BaseException as caught: error = caught
    final, final_context_error = context, None
    try: final = phase_context(run)
    except BaseException as caught:
        error = caught; final_context_error = safe_error(caught)
    if (diagnostic_only and time.monotonic() >= deadline) or (not diagnostic_only and time.monotonic() >= demand_deadline):
        error = TimeoutError('paired read collection budget exhausted at final closure')
    stable = final == context and error is None
    result = {'at': utc_now(), 'context': final, 'context_before': context, 'protocol': IDENTITY,
              'diagnostic_only': diagnostic_only, 'batch_id': batch, 'runs': records, 'demand_runs': demands,
              'collection_seconds': native_elapsed, 'collection_budget_seconds': PROFILE['collection_budget_seconds'],
              'demand_collection_seconds': time.monotonic()-demand_before,
              'demand_collection_budget_seconds': PROFILE['demand']['collection_budget_seconds'],
              'final_context_error': final_context_error,
              'errors': [] if error is None else [type(error).__name__+': '+safe_error(error)], 'comparison': comparison}
    if diagnostic_only:
        result.update(valid=stable and comparison['valid'], symmetric_within_ten_percent=stable and comparison['passed'])
        write_json(path, result)
    else:
        result['passed'] = stable and comparison['passed']
        write_json(directory / 'summary.json', result)
        value = {'at': utc_now(), 'context': final, 'runs': prior + records, 'demand_runs': prior_demand + demands,
                 **(canonical or {'passed': False, 'diagnostic_only': False, 'protocol': IDENTITY, 'missing': ['read batch finalization failed']}),
                 'batches': prior_batches + [{'passed': result['passed'], 'scale': scale, 'batch_id': batch,
                    'path': str(directory/'summary.json'), 'sha256': digest(directory/'summary.json')}]}
        value['passed'] = stable and value['passed']
        write_json(path, value)
    if error is not None: raise error
    return result


def parity(run: Path, *, source_root: Path) -> dict:
    fixture = active_fixture()
    if not source_root.resolve().is_relative_to(REPO / "plans") or not (source_root / "repositories/nodes.ts").is_file():
        raise ValueError("parity requires a frozen baseline backend source under plans")
    sources=expected_sources()
    if source_root.resolve()!=Path(sources['baseline']['source_root']):
        raise ValueError('parity baseline differs from the Git-verified source seal')
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    output = run / "native-parity"
    context_output = run / "path-context.json"
    codes = [node_tool("checkLookupParity", ["--baseline-source-root", str(source_root), "--output", str(output)],
                       database, run / "parity.log"),
             node_tool("checkPathContext", ["--output", str(context_output)], database, run / "path-context.log")]
    repository = read_json(output / "parity.json") if (output / "parity.json").exists() else {}
    graph = read_json(context_output) if context_output.exists() else {}
    source_stable=all(identify(Path(value['source_root']),value['revision'])==value for value in sources.values())
    checks = [{"name": "complete repository parity", "passed": codes[0] == 0 and repository.get("passed") is True
               and repository.get("caseCount", 0) > 0, "cases": repository.get("caseCount")},
              {"name": "complete path graph parity and unchanged budget", "passed": codes[1] == 0 and graph.get("passed") is True
               and graph.get("links", 0) > 0, "nodes": graph.get("nodes"), "links": graph.get("links")}]
    result = {"at": utc_now(), "passed": all(check["passed"] for check in checks), "checks": checks,
              "context": phase_context(run),'sources':sources}
    checks.append({'name':'sealed baseline and current source remained unchanged','passed':source_stable})
    result['passed']=all(check['passed'] for check in checks)
    write_json(run / "parity.json", result)
    return result
