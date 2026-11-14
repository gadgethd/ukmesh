"""Sealed-source statistics/export measurements with the complete stack resident."""
from __future__ import annotations

from pathlib import Path
import re
import statistics
import math

from benchmarks import active_fixture, node_tool, CompanionReads
from common import REPO, read_json, utc_now, write_json
from context import phase_context
from probes import inventory
from source_identity import expected_sources

NAMES = {"summary", "charts", "nodes-csv", "nodes-geojson"}
SHA = re.compile(r"[a-f0-9]{64}\Z")


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
        records = row.get("records", [])
        snapshot = row.get("chart_snapshot", {})
        if not snapshot.get("generatedAt") or not isinstance(snapshot.get("visibilityGeneration"), int) \
                or snapshot["visibilityGeneration"] < 1 or snapshot.get("maxAgeMs", 0) <= 0:
            raise ValueError("eligible persisted chart snapshot was not recorded")
        if len(records) != 12 or {(item.get("name"), item.get("trial")) for item in records} != \
                {(name, trial) for name in NAMES for trial in range(3)}:
            raise ValueError("three complete statistics/export trials are required")
        for item in records:
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


def comparison(runs: list[dict], expected: dict) -> dict:
    checks, missing = [], []
    try:
        validate_operations(runs, expected)
    except ValueError as error:
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
        for name in sorted(NAMES):
            before = [item for item in before_run["records"] if item["name"] == name]
            after = [item for item in after_run["records"] if item["name"] == name]
            old, new = statistics.median(item["ms"] for item in before), statistics.median(item["ms"] for item in after)
            parity = len({item["dataDigest"] for item in before + after}) == 1
            checks.append({"name": scale+":"+name, "baseline_ms": [item["ms"] for item in before],
                           "candidate_ms": [item["ms"] for item in after], "data_parity": parity,
                           "change_percent": (new/old-1)*100 if old else None, "deadline_ms": 30000,
                           "passed": parity and new <= old*1.1 and max(item["ms"] for item in after) <= 30000})
    return {"passed": not missing and len(checks) == 8 and all(row.get("passed") is True for row in runs)
            and all(row["passed"] for row in checks), "checks": checks, "missing": missing}


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
