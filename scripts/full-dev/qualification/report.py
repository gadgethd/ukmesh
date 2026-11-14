"""Qualification closure requires actual, complete evidence from every phase."""

from __future__ import annotations

from pathlib import Path

from common import digest, read_json, safe_error, utc_now, write_json
from context import phase_context


REQUIRED = ("backups", "inventory", "work-smoke", "access", "reads", "parity",
            "other-operations", "resources", "soak", "steady-runtime-logs", "companion-commits", "core-projection", "recovery", "interfaces", "tests")


def phase_check(name: str, value: dict) -> tuple[bool, str | None]:
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
    if name == "reads" and (value.get("missing") or len(value.get("runs", [])) != 4):
        return False, "matched baseline/candidate measurements at both scales are incomplete"
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


def report(run: Path) -> dict:
    if (run / "corescope-public-pg.json").exists() and (run / "corescope-public-ram.json").exists():
        try:
            from core_projection import core_projection
            core_projection(run)
        except Exception as error:
            write_json(run / "core-projection.json", {"passed": False, "error": safe_error(error)})
    phases = []
    try:
        current = phase_context(run)
    except Exception:
        current = None
    contexts = {}
    for name in REQUIRED:
        path = run / (name + ".json")
        try:
            value = read_json(path)
            passed, error = phase_check(name, value)
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
                for measurement in value.get("runs", []):
                    if measurement.get("context", {}).get("run_id") != run.name or any(
                            measurement.get("context", {}).get(key) != context.get(key)
                            for key in ("profile_sha256", "source")):
                        passed, error = False, "matched read measurements used different source or runtime profiles"
                for scale in ("1x", "10x"):
                    tokens = {row.get("fixture_token") for row in value.get("runs", []) if row.get("scale") == scale}
                    if len(tokens) != 1 or None in tokens:
                        passed, error = False, "matched read measurements used different fixtures"
            phases.append({"phase": name, "passed": passed, "error": error, "sha256": digest(path)})
        except Exception as error:
            phases.append({"phase": name, "passed": False, "error": safe_error(error)})
    soak_targets = (contexts.get("soak") or {}).get("targets")
    for row in phases:
        if row["phase"] in ("resources", "access", "work-smoke") and row["passed"]:
            if not soak_targets or (contexts.get(row["phase"]) or {}).get("targets") != soak_targets:
                row.update({"passed": False, "error": "phase did not use the qualified soak fixture"})
    result = {"at": utc_now(), "passed": all(row["passed"] for row in phases), "phases": phases,
              "failed_phases": [row["phase"] for row in phases if not row["passed"]]}
    write_json(run / "report.json", result)
    lines = ["# Full-stack qualification", "", "Passed: " + str(result["passed"]), "",
             "| Phase | Result |", "| --- | --- |"]
    lines.extend(f"| {row['phase']} | {'PASS' if row['passed'] else 'FAIL'} |" for row in phases)
    (run / "report.md").write_text("\n".join(lines) + "\n")
    (run / "report.md").chmod(0o600)
    return result
