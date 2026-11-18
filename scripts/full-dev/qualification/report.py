"""Qualification closure requires actual, complete evidence from every phase."""

from __future__ import annotations

from pathlib import Path

from common import digest, read_json, safe_error, utc_now, write_json
from context import phase_context


REQUIRED = ("backups", "inventory", "work-smoke", "access", "reads", "parity",
            "other-operations", "resources", "soak", "steady-runtime-logs", "companion-commits", "core-projection", "recovery", "interfaces", "tests")
ACCEPTANCE_PROFILES = ("full-stack", "mqtt-first")


def phase_check(name: str, value: dict, *, acceptance_profile: str = "full-stack") -> tuple[bool, str | None]:
    if acceptance_profile not in ACCEPTANCE_PROFILES:
        raise ValueError("unknown qualification acceptance profile")
    if value.get("passed") is not True:
        return False, "phase failed or has no explicit successful result"
    if name == "resources" and (value.get("duration_seconds", 0) < 2400 * .98
                                or value.get("warmup_seconds", 121) > 120):
        return False, "full 40-minute resource window with at most 120s warmup is required"
    if name == "soak":
        settings = value.get("settings", {})
        if settings.get("count") != 72000 or settings.get("rate") != 30 or settings.get("httpConcurrency") != 8:
            return False, "full 72,000-packet, 30/s, C8 soak evidence is required"
        if not value.get("drained") or value.get("wsPackets") != 72000:
            return False, "durable drain and complete live delivery were not proved"
        if value.get('logs_passed') is not True:
            return False,'resident application logs contain failures or were not checked'
        readiness=value.get('socketReadiness',{})
        if readiness.get('initialState') is not True or readiness.get('unexpectedCloses')!=0:
            return False,'authorized live subscription readiness and continuity were not proved'
        cold=value.get('warmupReads',[])
        if settings.get('httpWarmup') is not True or len(cold)!=8 or any(row.get('status')!=200 for row in cold):
            return False,'explicit C8 route warmup with retained successful cold responses is required'
        latency = value.get('readLatency', {})
        if latency.get('advisory') is not (acceptance_profile == 'mqtt-first') \
                or type(latency.get('passed')) is not bool:
            return False, 'mixed-read acceptance mode does not match the requested profile'
        if acceptance_profile == 'full-stack' and latency['passed'] is not True:
            return False, 'mixed-read latency failed'
        if value.get('acceptance_profile') != acceptance_profile:
            return False, 'soak receipt used another acceptance profile'
    if name == "reads":
        from read_protocol import IDENTITY, STEPS
        native_only = acceptance_profile == 'mqtt-first'
        demand_count = 0 if native_only else len(STEPS)*2
        if value.get('diagnostic_only') is not False or value.get('protocol') != IDENTITY \
                or value.get("missing") or len(value.get("runs", [])) != len(STEPS)*2 \
                or value.get('native_only', False) is not native_only \
                or len(value.get('demand_runs', [])) != demand_count \
                or len(value.get('batches', [])) != 2 or any(item.get('passed') is not True for item in value.get('batches', [])):
            return False, "three complete canonical matched read pairs at both scales are required"
    if name == "access":
        checks = value.get("checks", [])
        if len(checks) < 20 or not all(check.get("passed") is True for check in checks):
            return False, "authenticated permission matrix is incomplete"
    if name == "backups" and (len(value.get("stores", [])) != 4 or not all(
            store.get("verified") is True for store in value.get("stores", []))):
        return False, "four verified original stores are required"
    if name in ("inventory", "work-smoke"):
        records = value.get("services", [])
        if not records or not all(row.get("passed") is True for row in records if row.get("required", True)):
            return False, "required service proofs are incomplete"
    if name in ("other-operations", "parity", "companion-commits", "core-projection", "steady-runtime-logs", "recovery", "interfaces", "tests"):
        checks = value.get("checks", [])
        if not checks or not all(row.get("passed") is True and not row.get("skipped") for row in checks):
            return False, "required checks are missing, skipped or failed"
    if name == "companion-commits" and value.get("count") != 72000:
        return False, "complete companion commit evidence for 72,000 ACKs is required"
    if name=='recovery':
        from recovery import REQUIRED_CHECKS
        if not REQUIRED_CHECKS.issubset({row.get('name') for row in value.get('checks',[])}):
            return False,'concrete required recovery faults or native persistence proofs are missing'
    return True, None


def report(run: Path, *, acceptance_profile: str = "full-stack") -> dict:
    if acceptance_profile not in ACCEPTANCE_PROFILES:
        raise ValueError("unknown qualification acceptance profile")
    native_only = acceptance_profile == 'mqtt-first'
    if (run / "corescope-public-pg.json").exists() and (run / "corescope-public-ram.json").exists():
        try:
            from core_projection import core_projection
            core_projection(run)
        except Exception as error:
            write_json(run / "core-projection.json", {"passed": False, "error": safe_error(error)})
    phases = []
    advisory_evidence = []
    try:
        current = phase_context(run)
    except Exception:
        current = None
    contexts = {}
    for name in REQUIRED:
        path = run / (("reads-native" if name == 'reads' and native_only else name) + ".json")
        try:
            value = read_json(path)
            passed, error = phase_check(name, value, acceptance_profile=acceptance_profile)
            if name == 'soak' and native_only:
                advisory_evidence.append({'name': 'mixed-read-latency', 'path': str(path),
                                          'result': value.get('readLatency'), 'sha256': digest(path)})
            context = value.get("context")
            if name != "backups":
                contexts[name] = context
                if not current or not context or context.get("run_id") != run.name:
                    passed, error = False, "phase is not bound to this evidence run"
                elif context.get("profile_sha256") != current["profile_sha256"]:
                    passed, error = False, "phase used another runtime profile"
                elif any(context.get("source", {}).get(key) != current["source"][key]
                         for key in ("root_content_sha256", "submodules_sha256")):
                    passed, error = False, "source changed since this phase"
            if name in ("reads", "other-operations"):
                if name=='reads':
                    from source_identity import expected_sources,validate_measurements
                    validate_measurements(value.get('runs',[]),expected_sources())
                    validate_measurements(value.get('demand_runs',[]),expected_sources())
                    from read_measurements import native_read_comparison, read_comparison, validate_read_batches
                    validate_read_batches(value, native_only=native_only)
                    comparison = (native_read_comparison(value.get('runs', [])) if native_only else
                                  read_comparison(value.get('runs', []), demand_runs=value.get('demand_runs', [])))
                    if not comparison['passed']:
                        passed,error=False,'read source, raw companion evidence or regression comparison failed'
                else:
                    from source_identity import expected_sources
                    from operations import validate_operations,comparison
                    expected=expected_sources()
                    validate_operations(value.get('runs',[]),expected)
                    if not comparison(value.get('runs',[]),expected)['passed']:
                        passed,error=False,'statistics/export source, data or regression comparison failed'
                for measurement in value.get("runs", []):
                    if measurement.get("context", {}).get("run_id") != run.name or any(
                            measurement.get("context", {}).get(key) != context.get(key)
                            for key in ("profile_sha256", "source")):
                        passed, error = False, "matched read measurements used different source or runtime profiles"
                for scale in ("1x", "10x"):
                    tokens = {row.get("fixture_token") for row in value.get("runs", []) if row.get("scale") == scale}
                    if len(tokens) != 1 or None in tokens:
                        passed, error = False, "matched read measurements used different fixtures"
            phases.append({"phase": name, "passed": passed, "error": error, "path": str(path), "sha256": digest(path)})
        except Exception as error:
            phases.append({"phase": name, "passed": False, "error": safe_error(error)})
    soak_targets = (contexts.get("soak") or {}).get("targets")
    for row in phases:
        if row["phase"] in ("resources", "access", "work-smoke") and row["passed"]:
            if not soak_targets or (contexts.get(row["phase"]) or {}).get("targets") != soak_targets:
                row.update({"passed": False, "error": "phase did not use the qualified soak fixture"})
    if native_only:
        demand_path = run / 'reads.json'
        advisory_evidence.append({'name': 'fixed-demand-80rps', 'path': str(demand_path),
                                  'available': demand_path.is_file(),
                                  **({'sha256': digest(demand_path)} if demand_path.is_file() else {})})
    result = {"at": utc_now(), "acceptance_profile": acceptance_profile,
              "advisory": ['fixed-demand-80rps', 'mixed-read-latency'] if native_only else [],
              'advisory_evidence': advisory_evidence,
              "passed": all(row["passed"] for row in phases), "phases": phases,
              "failed_phases": [row["phase"] for row in phases if not row["passed"]]}
    report_name = 'report-mqtt-first' if native_only else 'report'
    write_json(run / (report_name + '.json'), result)
    lines = ["# MQTT-first qualification" if native_only else "# Full-stack qualification", "", "Acceptance profile: " + acceptance_profile,
             "Passed: " + str(result["passed"]), "",
             "| Phase | Result |", "| --- | --- |"]
    lines.extend(f"| {row['phase']} | {'PASS' if row['passed'] else 'FAIL'} |" for row in phases)
    (run / (report_name + '.md')).write_text("\n".join(lines) + "\n")
    (run / (report_name + '.md')).chmod(0o600)
    return result
