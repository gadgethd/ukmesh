"""Full-stack soak composes existing replay/read checks and native companion proofs."""

from __future__ import annotations

import fcntl
import csv
import gzip
import hashlib
import json
import math
import os
import re
from pathlib import Path
import threading
import stat
import uuid

from benchmarks import active_fixture, node_tool
from companion_reads import CompanionReads
from common import REPO, STATE, digest, read_json, safe_error, sql, utc_now, write_json
from context import phase_context
from probes import inventory
from loopback_http import http_get
from resources import sample
from traffic import hex_list, verify_companion_acks, verify_typed
from health import create_session, verify_session
from read_transport import start_reader, stop_reader
from runtime_logs import collect_logs

SOAK_PHASES = ('publication', 'delivery', 'drain', 'reconciliation')


def phase_receipts(output: Path, *, invocation: str, database: str, source: Path) -> list[dict]:
    """Retain native stage identities without rewriting their source-bound bytes."""
    records = []
    for phase in SOAK_PHASES:
        path = output / ('phase-' + phase + '.json')
        record = {'phase': phase, 'path': str(path), 'passed': False}
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                raise ValueError('bounded regular phase receipt required')
            value = read_json(path)
            record['sha256'] = digest(path)
            if type(value.get('version')) is not int or value['version'] != 1 or value.get('phase') != phase \
                    or value.get('invocationToken') != invocation or value.get('database') != database \
                    or value.get('outputDirectory') != str(output.resolve()) \
                    or value.get('dataDirectory') != str(source.resolve()) \
                    or value.get('toolSha256') != digest(REPO / 'backend/src/tools/soakDatabaseLookups.ts'):
                raise ValueError('phase receipt belongs to another invocation or source')
            record['passed'] = value.get('passed') is True
        except (OSError, ValueError, TypeError, AttributeError) as error:
            record['error'] = type(error).__name__ + ': native phase evidence unavailable or invalid'
        records.append(record)
    return records


def prepublication_failure(output, source, auxiliary, fixture, count, invocation, code):
    """Only this invocation's source/input-bound, guarded zero-publisher receipt is authoritative."""
    if type(code) is not int or code == 0: return None
    path = output/'failure.json'
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536: return None
            raw = stream.read(65537)
        if len(raw) > 65536: return None
        value = json.loads(raw)
        database = next(row['database'] for row in fixture['databases'] if row['label'] == 'ukmesh')
        expected_inputs = {'fixture': digest(source/'fixture.json'), 'packets': digest(source/'packets.csv.gz'),
                           'raw': digest(source/'raw_packets.csv.gz'), 'auxiliary': digest(auxiliary/'traffic.json')}
        if type(value.get('version')) is not int or value['version'] != 1 or value.get('passed') is not False or value.get('phase') != 'pre-publication' \
                or value.get('publicationStarted') is not False or value.get('guardCompleted') is not True \
                or any(type(value.get(key)) is not int or value[key] != 0 for key in ('ackPublished', 'auxiliaryPublished')) \
                or value.get('invocationToken') != invocation or value.get('database') != database \
                or value.get('dataDirectory') != str(source.resolve()) or value.get('outputDirectory') != str(output.resolve()) \
                or value.get('inputSha256') != expected_inputs or value.get('settings') != {'count': count, 'rate': 30} \
                or value.get('toolSha256') != digest(REPO/'backend/src/tools/soakDatabaseLookups.ts') \
                or value.get('helperSha256') != {key: digest(REPO/'backend/src/tools'/name) for key, name in
                    (('publication', 'soakPublication.ts'), ('counts', 'soakWorksetCounts.ts'), ('socket', 'soakSocket.ts'))}:
            return None
        with gzip.open(source/'packets.csv.gz', 'rt', newline='') as stream:
            inputs = [row['packet_hash'] for row in csv.DictReader(stream)]
        auxiliary_inputs = read_json(auxiliary/'traffic.json')['ukmeshHashes']
        if len(inputs) != count or any(not isinstance(item, str) or not re.fullmatch('[a-fA-F0-9]{64}', item)
                                      for item in inputs+auxiliary_inputs): return None
        ack = {item.upper() for item in inputs}; aux = {item.upper() for item in auxiliary_inputs}
        hashes = sorted(ack | aux)
        wanted = {'kind': 'owned-packet-derived-observation-workset-v1', 'hashCount': len(hashes),
                  'ackHashCount': len(ack), 'auxiliaryHashCount': len(aux),
                  'sha256': hashlib.sha256(json.dumps(hashes, separators=(',', ':')).encode()).hexdigest()}
        if len(ack) != count or any(value.get('countsScope', {}).get(key) != item for key, item in wanted.items()): return None
        return {**value, 'receipt_path': str(path), 'receipt_sha256': hashlib.sha256(raw).hexdigest()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, csv.Error, StopIteration): return None


def not_published(result, failure):
    return {**result, 'passed': False, 'not_published': True,
            'publication_failure_sha256': failure['receipt_sha256'],
            'checks': result.get('checks', [])+[{'name': 'native workload publication started', 'passed': False, 'not_published': True}]}


def verify_persistence(source, auxiliary, fixture, health_result, failure=None):
    if not failure:
        return verify_typed(read_json(auxiliary/'traffic.json'), fixture), verify_companion_acks(source, fixture), health_result
    snapshots = []
    for name, snapshot in (('typed', lambda: verify_typed(read_json(auxiliary/'traffic.json'), fixture, timeout=0)),
                           ('ACK', lambda: verify_companion_acks(source, fixture, timeout=0))):
        try: value = snapshot()
        except Exception as error:
            message = type(error).__name__+': persistence snapshot failed; private details withheld'
            value = {'passed': False, 'checks': [{'name': name+' one-shot persistence snapshot',
                'passed': False, 'error': message}], 'snapshot_error': message}
        snapshots.append(not_published(value, failure))
    health_result = not_published(health_result, failure)
    from health import PROOF
    try: write_json(PROOF, health_result)
    except OSError as error:
        health_result['checks'].append({'name': 'cancelled Health receipt persisted', 'passed': False, 'error': safe_error(error)})
    return snapshots[0], snapshots[1], health_result


def await_first_sample(ready, result):
    if not ready.wait(30) or result.get('passed') is False:
        raise RuntimeError('whole-VM sampler did not capture its first complete sample before workload startup')


def stop_measurements(worker, health_worker, companions, stop, reader_name):
    stop.set()
    try:
        if worker.is_alive(): worker.join(timeout=40)
        return companions.finish()
    finally:
        try:
            if health_worker.is_alive(): health_worker.join(timeout=130)
        finally:
            stop_reader(reader_name)


def reserve_ack_sequence(count: int) -> int:
    path = STATE / "runtime/ack-sequence"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a+") as counter:
        path.chmod(0o600)
        fcntl.flock(counter, fcntl.LOCK_EX)
        counter.seek(0)
        value = int(counter.read().strip() or "1000000000")
        if value + count > 2 ** 32:
            raise ValueError("owned ACK sequence space is exhausted")
        counter.seek(0)
        counter.truncate()
        counter.write(str(value + count) + "\n")
        counter.flush()
    return value



def verify_prepared_fixture(database: str, source: Path) -> None:
    with gzip.open(source / "nodes.csv.gz", "rt", newline="") as file:
        nodes = [row["node_id"] for row in csv.DictReader(file)]
    node_ids = hex_list(nodes, 64)
    # Bulk imports explicitly clear the visibility fence. Reuse requires a
    # converged retained fixture with the generated identities ready for reads.
    ready = sql("ukmesh-sample-db", database, "meshcore", f"""SELECT EXISTS (
      SELECT 1 FROM ops.public_visibility_state v
      JOIN ops.packet_visibility_materialization_state s USING (singleton)
      WHERE v.singleton AND v.generation = s.visibility_generation AND v.generation > 0
      AND (SELECT count(DISTINCT node_id) FROM core.node_identity_nodes
           WHERE network = 'ukmesh' AND node_id IN ({node_ids})) = {len(nodes)}
      AND NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE job_id >= 1000 AND scheduled)
    )""")
    if ready.strip() != "t":
        raise RuntimeError("owned soak fixture requires successful full preparation before reuse")


from target_safety import managed_publisher

@managed_publisher
def soak(run: Path, *, count: int = 72000, health_code: str | None = None, reuse_prepared: bool = False,
         acceptance_profile: str = 'full-stack') -> dict:
    from report import ACCEPTANCE_PROFILES
    if acceptance_profile not in ACCEPTANCE_PROFILES:
        raise ValueError('unknown qualification acceptance profile')
    if not isinstance(count, int) or not 30 <= count <= 108000:
        raise ValueError("soak count must be 30..108000; final qualification requires 72000")
    fixture = active_fixture()
    if fixture["kind"] not in ("1x", "10x"):
        raise ValueError("soak requires a retained public lookup fixture")
    if not inventory(run)["passed"]:
        raise RuntimeError("complete service inventory must pass before the soak")
    context = phase_context(run)
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    source = run / "soak-source"
    sequence = reserve_ack_sequence(count)
    generate = ["--data-dir", str(source), "--count", str(count), "--sequence-start", str(sequence)]
    if node_tool("generateSampleDataset", generate, database, run / "generateSampleDataset.log"):
        raise RuntimeError("soak workset generation failed")
    if reuse_prepared:
        verify_prepared_fixture(database, source)
    else:
        preparation = [
            ("loadSampleDataset", ["--data-dir", str(source), "--omit-packets", "--fast"]),
            ("prepareSampleDataset", []),
        ]
        for tool, args in preparation:
            if node_tool(tool, args, database, run / (tool + ".log"), timeout=1800):
                raise RuntimeError(tool + " fixture preparation failed")
    write_json(run / "soak-preparation.json", {"fixture": fixture["token"], "reuse_prepared": reuse_prepared})
    auxiliary = run / "auxiliary-source"
    health = create_session(run, fixture, health_code)
    cycles = max(1, min(40, count // 1800))
    interval = 60000 if count >= 1800 else 1000
    args = ["--output", str(auxiliary), "--seed", uuid.uuid4().hex, "--cycles", str(cycles),
            "--interval-ms", str(interval), "--generate-only"]
    args += ["--health-code", health["code"]]
    if node_tool("fullStackTraffic", args, database, run / "auxiliary-generate.log"):
        raise RuntimeError("invented auxiliary traffic preparation failed")
    resources_result = {}
    stop = threading.Event()
    ready=threading.Event()
    def measure():
        try:
            duration = math.ceil(count / 30)
            resources_result.update(sample(run, seconds=duration, interval=max(.5, min(5, duration / 3)),
                warmup=min(120, max(0, duration // 4)), stop_event=stop,ready_event=ready))
        except Exception as error:
            resources_result.update({"passed": False, "error": safe_error(error)})
            write_json(run / "resources-failure.json", resources_result)
            ready.set()
    worker = threading.Thread(target=measure, name="whole-vm-qualification", daemon=True)
    health_result = {}
    health_stop = threading.Event()
    health_worker = threading.Thread(target=lambda: health_result.update(verify_session(health, stop_event=health_stop)),
                                     name="health-code-qualification", daemon=True)
    companions = CompanionReads(http_get)
    log_started=utc_now()
    reader_name, reader_socket = start_reader(run)
    invocation = uuid.uuid4().hex
    prepublication = None
    try:
        companions.start()
        worker.start()
        await_first_sample(ready,resources_result)
        health_worker.start()
        native_output = run / "native-soak"
        code = node_tool("soakDatabaseLookups", ["--data-dir", str(source), "--output", str(native_output),
            "--base-url", "http://127.0.0.1:3100", "--metrics-url", "http://127.0.0.1:9091/metrics",
            "--broker", "ws://127.0.0.1:9002", "--limit", str(count), "--rate", "30",
            "--envelope-format", "observer", "--auxiliary-traffic", str(auxiliary / "traffic.json"),
            "--http-reader-socket", str(reader_socket),
            '--warmup-reads',
            *(['--advisory-read-latency'] if acceptance_profile == 'mqtt-first' else []),
            '--invocation-token', invocation,
            "--budget-ms", "100" if fixture["kind"] == "1x" else "250"], database,
            run / "soak.log", timeout=math.ceil(count / 30) + 300)
        prepublication = prepublication_failure(native_output, source, auxiliary, fixture, count, invocation, code)
        if prepublication: health_stop.set()
    finally:
        companion_results=stop_measurements(worker,health_worker,companions,stop,reader_name)
    native = read_json(native_output / "soak.json") if (native_output / "soak.json").exists() else {"passed": False}
    stages = phase_receipts(native_output, invocation=invocation, database=database, source=source)
    typed, ack_proof, health_result = verify_persistence(source, auxiliary, fixture, health_result, prepublication)
    logs=collect_logs(run,started=log_started,finished=utc_now())
    logs['context']=context
    write_json(run/'steady-runtime-logs.json',logs)
    commit_proof = {"at": utc_now(), "passed": typed["passed"] and ack_proof["passed"] and health_result.get("passed", False),
                    "checks": typed["checks"] + ack_proof["checks"] + health_result.get("checks", []),
                    "count": count, "context": context, 'not_published': prepublication is not None,
                    'verification_mode': 'one-shot-not-published' if prepublication else 'full-bounded',
                    'prepublication_failure': prepublication}
    write_json(run / "companion-commits.json", commit_proof)
    native.update({"native_passed": native.get("passed", False), "exit_code": code, "context": context,
                   'acceptance_profile': acceptance_profile, 'phase_receipts': stages,
                   'phase_receipts_context': context,
                   'prepublication_failure': prepublication,
                   "resources_passed": resources_result.get("passed", False), "companions": companion_results,
                   "companion_commits_passed": commit_proof["passed"],"logs_passed":logs['passed']})
    native["passed"] = code == 0 and native["native_passed"] and native["resources_passed"] and commit_proof["passed"] \
        and logs['passed'] and all(row['passed'] for row in stages) \
        and all(value["requests"] > 0 and value["failures"] == 0 for value in companion_results.values())
    write_json(run / "soak.json", native)
    return native
