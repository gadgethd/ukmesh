"""Sealed-source statistics/export measurements with the complete stack resident."""
from __future__ import annotations

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import json
import re
import statistics
import math
import time
import uuid

from benchmarks import active_fixture, node_tool, CompanionReads
from common import REPO, read_json, utc_now, write_json
from context import phase_context
from probes import inventory
from source_identity import expected_sources

NAMES = {"summary", "charts", "nodes-csv", "nodes-geojson"}
SHA = re.compile(r"[a-f0-9]{64}\Z")
ORDER = ("summary", "charts", "nodes-csv", "nodes-geojson")
PAIR_POLICY=read_json(REPO/'backend/src/tools/operationPairPolicy.json')


def timestamp(value: str) -> float:
    parsed=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('operation timestamp must be timezone-aware')
    elapsed=parsed-dt.datetime(1970,1,1,tzinfo=dt.timezone.utc)
    return elapsed.days*86400000+elapsed.seconds*1000+elapsed.microseconds/1000


def validate_operations(runs: list[dict], expected: dict) -> None:
    for row in runs:
        identity = expected.get(row.get("label"))
        if not identity or row.get("measured_source") != identity or row.get("source_identity") != identity \
                or row.get("source_identity_after") != identity or row.get("source_stable") is not True:
            raise ValueError("operations measured an unsealed or stale source tree")
        settings = row.get("settings", {})
        if settings != {"trials": 3, "deadlineMs": 30000, "maxResponseBytes": 8*1024*1024,
                        "exportSpacingMs": 13000, "readOnly": True, "httpConcurrency": 1}:
            raise ValueError("operation deadlines, rate spacing or read-only profile differ")
        pair=row.get('pair',{})
        if not re.fullmatch('[a-f0-9]{32}',pair.get('token','')) or pair.get('label')!=row['label'] \
                or row.get('source_settings_fixture_stable') is not True or row.get('context_before')!=row.get('context'):
            raise ValueError('operations require a source-stable simultaneous matched pair')
        records = row.get("records", [])
        snapshot = row.get("chart_snapshot", {})
        if not snapshot.get("generatedAt") or not isinstance(snapshot.get("visibilityGeneration"), int) \
                or snapshot["visibilityGeneration"] < 1 or snapshot.get("maxAgeMs", 0) <= 0:
            raise ValueError("eligible persisted chart snapshot was not recorded")
        if len(records) != 12 or {(item.get("name"), item.get("trial")) for item in records} != \
                {(name, trial) for name in NAMES for trial in range(3)}:
            raise ValueError("three complete statistics/export trials are required")
        for item in records:
            paired=item.get('paired',{})
            if paired.get('token')!=pair['token'] or not timestamp(paired['readyAt'])<=timestamp(paired['releaseAt'])<=timestamp(item['startedAt']) \
                    or timestamp(item['finishedAt'])<timestamp(item['startedAt']):
                raise ValueError('operation response lacks its actual paired UTC bounds')
            if item.get("passed") is not True or item.get("status") != 200 or not 0 < item.get("bytes", 0) <= 8*1024*1024 \
                    or not 0 <= item.get("ms", -1) <= 30000 or not all(SHA.fullmatch(item.get(key, "")) for key in ("digest", "dataDigest")):
                raise ValueError("operation response or deadline failed")
            if item["name"].startswith("nodes-") and (not 0 < item.get("entries", 0) <= 5000 or not SHA.fullmatch(item.get("identityDigest", ""))):
                raise ValueError("populated bounded export identities are required")
            if item["name"] == "charts":
                state = item.get("snapshot", {})
                if state.get("status") != "complete" or state.get("scope") != "ukmesh" \
                        or any(state.get(key) != snapshot[key] for key in ("generatedAt", "visibilityGeneration", "maxAgeMs")) \
                        or not isinstance(state.get("ageMs"), (float, int)) or not math.isfinite(state["ageMs"]) \
                        or state["ageMs"] > snapshot["maxAgeMs"]:
                    raise ValueError("chart snapshot freshness or generation was not proved")
        checks = row.get("identity_checks", [])
        if len(checks) != 3 or {item.get("trial") for item in checks} != {0, 1, 2} or not all(item.get("passed") is True for item in checks):
            raise ValueError("CSV/GeoJSON identity parity was not proved")
        for trial in range(3):
            identities = {item["identityDigest"] for item in records if item["trial"] == trial and item["name"].startswith("nodes-")}
            if len(identities) != 1:
                raise ValueError("CSV/GeoJSON export identity sets differ")
        exports=sorted((item for item in records if item['name'].startswith('nodes-')),key=lambda item:timestamp(item['startedAt']))
        if any(timestamp(later['startedAt'])-timestamp(earlier['finishedAt'])<13000 for earlier,later in zip(exports,exports[1:])):
            raise ValueError('operation export spacing fell below13 seconds')
        first=next(item for item in records if item['trial']==0 and item['name']=='summary')
        if timestamp(snapshot['generatedAt'])+snapshot['maxAgeMs']-timestamp(first['paired']['releaseAt'])<PAIR_POLICY['minSnapshotFreshMs']:
            raise ValueError('operation pair began without five minutes of chart freshness')


def comparison(runs: list[dict], expected: dict) -> dict:
    checks, missing = [], []
    try:
        validate_operations(runs, expected)
    except (ValueError,TypeError,KeyError) as error:
        return {"passed": False, "checks": [], "missing": [str(error)]}
    for scale in ("1x", "10x"):
        matches = {label: [row for row in runs if row["scale"] == scale and row["label"] == label]
                   for label in ("baseline", "candidate")}
        if not all(len(items) == 1 for items in matches.values()):
            missing.append(scale)
            continue
        before_run, after_run = matches["baseline"][0], matches["candidate"][0]
        if before_run["fixture_token"] != after_run["fixture_token"]:
            missing.append(scale + ":fixture-identity")
            continue
        if before_run['pair']['token']!=after_run['pair']['token'] or before_run['chart_snapshot']!=after_run['chart_snapshot']:
            missing.append(scale+':pair-or-snapshot-identity')
            continue
        for name in sorted(NAMES):
            before = [item for item in before_run["records"] if item["name"] == name]
            after = [item for item in after_run["records"] if item["name"] == name]
            old, new = statistics.median(item["ms"] for item in before), statistics.median(item["ms"] for item in after)
            paired=[]
            for trial in range(3):
                left=next(item for item in before if item['trial']==trial)
                right=next(item for item in after if item['trial']==trial)
                skew=abs(timestamp(left['startedAt'])-timestamp(right['startedAt']))
                paired.append({'trial':trial,'start_skew_ms':skew,'passed':skew<=PAIR_POLICY['maxStartSkewMs']
                    and left['dataDigest']==right['dataDigest'] and left['paired']['releaseAt']==right['paired']['releaseAt']})
            parity = all(item['passed'] for item in paired)
            checks.append({"name": scale+":"+name, "baseline_ms": [item["ms"] for item in before],
                           "candidate_ms": [item["ms"] for item in after], "data_parity": parity,
                           'paired_trials':paired,
                           "change_percent": (new/old-1)*100 if old else None, "deadline_ms": 30000,
                           "passed": parity and new <= old*1.1 and max(item["ms"] for item in after) <= 30000})
    return {"passed": not missing and len(checks) == 8 and all(row.get("passed") is True for row in runs)
            and all(row["passed"] for row in checks), "checks": checks, "missing": missing}


def release_pair(directory: Path, token: str, sources: dict, futures: dict) -> list[dict]:
    releases=[]
    for trial in range(3):
        for name in ORDER:
            deadline=time.monotonic()+PAIR_POLICY['barrierTimeoutMs']/1000
            while True:
                ready={}
                for label in ('baseline','candidate'):
                    file=directory/f'{trial}-{name}-{label}.ready.json'
                    if file.is_symlink():raise ValueError('operation readiness must be an owned regular file')
                    if file.is_file():
                        if file.stat().st_size>32768:raise ValueError('operation readiness exceeds its budget')
                        try:ready[label]=read_json(file)
                        except json.JSONDecodeError:pass  # A process may still be completing its exclusive write.
                if time.monotonic()>=deadline:raise RuntimeError('paired operation readiness exceeded45 seconds')
                if len(ready)==2:break
                if any(future.done() for future in futures.values()):raise RuntimeError('paired operation process exited before its release')
                time.sleep(.01)
            snapshots=[]
            now=dt.datetime.now(dt.timezone.utc)
            for label,value in ready.items():
                if value.get('token')!=token or value.get('label')!=label or value.get('trial')!=trial \
                        or value.get('name')!=name or value.get('source')!=sources[label]:
                    raise ValueError('operation readiness source or step differs')
                if not 0<=now.timestamp()*1000-timestamp(value['readyAt'])<=PAIR_POLICY['barrierTimeoutMs']:
                    raise ValueError('operation readiness timestamp is stale')
                snapshot=value['snapshot'];snapshots.append(snapshot)
                remaining=timestamp(snapshot['generatedAt'])+snapshot['maxAgeMs']-now.timestamp()*1000
                if remaining<=0 or trial==0 and name=='summary' and remaining<PAIR_POLICY['minSnapshotFreshMs']:
                    raise ValueError('paired operations require the same eligible chart with five minutes remaining')
            if snapshots[0]!=snapshots[1]:raise ValueError('paired operation chart snapshots differ')
            release={'token':token,'trial':trial,'name':name,
                     'releaseAt':(now+dt.timedelta(milliseconds=100)).isoformat(timespec='milliseconds')}
            write_json(directory/f'{trial}-{name}.release.json',release);releases.append(release)
    return releases


def operations_pair(run: Path) -> dict:
    fixture=active_fixture()
    if fixture['kind'] not in ('1x','10x') or not inventory(run)['passed']:
        raise ValueError('paired operations require the complete stack on a retained lookup fixture')
    sources=expected_sources();before=phase_context(run)
    token=uuid.uuid4().hex
    directory=run/f"operations-{fixture['kind']}-paired-{token[:8]}";directory.mkdir(mode=0o700)
    write_json(directory/'pair.json',{'token':token,'fixture_token':fixture['token'],'sources':sources})
    database=next(item['database'] for item in fixture['databases'] if item['label']=='ukmesh')
    companions=CompanionReads();companions.start()
    codes={};error=None;releases=[]
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures={label:executor.submit(node_tool,'benchmarkOperations',
                ['--source-root',source['source_root'],'--reference-revision',source['revision'],
                 '--output',str(directory/label),'--pair-directory',str(directory),'--pair-token',token,'--pair-label',label],
                 database,directory/label/'native.log',timeout=600) for label,source in sources.items()}
            try:releases=release_pair(directory,token,sources,futures)
            except Exception as failure:
                error=str(failure);write_json(directory/'abort.json',{'token':token})
            finally:
                for label,future in futures.items():
                    try:codes[label]=future.result(timeout=610)
                    except Exception as failure:codes[label]=-1;error=error or type(failure).__name__
    finally:companion_results=companions.finish()
    after=phase_context(run)
    stable=before==after and active_fixture()['token']==fixture['token'] and expected_sources()==sources
    records=[]
    for label,source in sources.items():
        native_path=directory/label/'operations.json'
        native=read_json(native_path) if native_path.exists() else {}
        record={'at':utc_now(),'scale':fixture['kind'],'label':label,'fixture_token':fixture['token'],
            'context_before':before,'context':after,'source_settings_fixture_stable':stable,
            'passed':error is None and stable and codes.get(label)==0 and native.get('passed') is True,
            'exit_code':codes.get(label),'measured_source':source,'source_identity':native.get('sourceIdentity'),
            'source_identity_after':native.get('sourceIdentityAfter'),'source_stable':native.get('sourceStable'),
            'settings':native.get('settings',{}),'records':native.get('records',[]),'pair':native.get('pair',{}),
            'chart_snapshot':native.get('chartSnapshot',{}),'identity_checks':native.get('identityChecks',[]),
            'companions':companion_results,'attempt':str(directory)}
        record['passed'] &= all(item['requests']>0 and item['failures']==0 for item in companion_results.values())
        records.append(record)
    local=comparison(records,sources)
    local_passed=len(local['checks'])==4 and all(item['passed'] for item in local['checks']) \
        and local['missing']==[('10x' if fixture['kind']=='1x' else '1x')]
    result={'at':utc_now(),'passed':all(row['passed'] for row in records) and local_passed,'records':records,'context':after,
            'paired_comparison':local,
            'attempt':str(directory),'releases':releases,'error':error}
    write_json(directory/'summary.json',result)
    path=run/'other-operations.json'
    prior=read_json(path).get('runs',[]) if path.exists() else []
    prior.extend(records)
    write_json(path,{'at':utc_now(),'context':after,'runs':prior,**comparison(prior,expected_sources())})
    return result


def operations(run: Path, *, label: str, source_root: Path | None = None) -> dict:
    if label not in ("baseline", "candidate"):
        raise ValueError("operations label must be baseline or candidate")
    fixture = active_fixture()
    if fixture["kind"] not in ("1x", "10x"):
        raise ValueError("aggregate measurements require retained lookup fixtures")
    expected = expected_sources()
    source = expected[label]
    if source_root is not None and source_root.resolve() != Path(source["source_root"]):
        raise ValueError("operation source differs from its sealed authority")
    if not inventory(run)["passed"]:
        raise RuntimeError("complete service inventory must pass before statistics/export timing")
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    output = run / f"operations-{fixture['kind']}-{label}"
    output.mkdir(mode=0o700)
    companions = CompanionReads()
    companions.start()
    try:
        code = node_tool("benchmarkOperations", ["--source-root", source["source_root"], "--reference-revision", source["revision"],
                         "--output", str(output)], database, output / "native.log", timeout=600)
        native = read_json(output / "operations.json") if (output / "operations.json").exists() else {}
    finally:
        companion_results = companions.finish()
    record = {"at": utc_now(), "scale": fixture["kind"], "label": label, "fixture_token": fixture["token"],
              "context": phase_context(run), "passed": code == 0 and native.get("passed") is True,
              "exit_code": code, "measured_source": source, "source_identity": native.get("sourceIdentity"),
              "source_identity_after": native.get("sourceIdentityAfter"), "source_stable": native.get("sourceStable"),
              "settings": native.get("settings", {}), "records": native.get("records", []),
              "chart_snapshot": native.get("chartSnapshot", {}),
              "identity_checks": native.get("identityChecks", []), "companions": companion_results}
    record["passed"] &= all(row["requests"] > 0 and row["failures"] == 0 for row in companion_results.values())
    write_json(output / "summary.json", record)
    path = run / "other-operations.json"
    prior = read_json(path).get("runs", []) if path.exists() else []
    prior.append(record)
    write_json(path, {"at": utc_now(), "context": phase_context(run), "runs": prior,
                     **comparison(prior, expected_sources())})
    return record
