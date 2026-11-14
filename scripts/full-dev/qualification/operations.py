"""Matched aggregate/export measurements through the running public API."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
import statistics
import time

from benchmarks import active_fixture
from common import read_json, safe_error, utc_now, write_json
from context import phase_context
from probes import http_get


CASES = (("summary", "/api/stats?network=ukmesh", "json"),
         ("charts", "/api/stats/charts?network=ukmesh", "json"),
         ("nodes-csv", "/api/v1/exports/nodes.csv?network=ukmesh&limit=5000", "csv"),
         ("nodes-geojson", "/api/v1/exports/nodes.geojson?network=ukmesh&limit=5000", "geojson"))


def operations(run: Path, *, label: str) -> dict:
    if label not in ("baseline", "candidate"):
        raise ValueError("operations label must be baseline or candidate")
    fixture = active_fixture()
    if fixture["kind"] not in ("1x", "10x"):
        raise ValueError("aggregate measurements require retained lookup fixtures")
    records = []
    # Preserve the real five-per-minute download limiter across invocations.
    last_export = 0.0
    existing = run / "other-operations.json"
    prior = read_json(existing).get("runs", []) if existing.exists() else []
    if prior:
        last_wall = max((row.get("last_export_at", 0) for row in prior), default=0)
        last_export = time.monotonic() - max(0, time.time() - last_wall)
    for trial in range(3):
        for name, route, format in CASES:
            if format != "json":
                delay = 13 - (time.monotonic() - last_export)
                if delay > 0:
                    time.sleep(delay)
            started = time.monotonic()
            record = {"name": name, "trial": trial, "passed": False}
            try:
                body, content_type = http_get("http://127.0.0.1:3100" + route, timeout=30)
                if format == "csv":
                    rows = list(csv.DictReader(io.StringIO(body.decode())))
                    valid = "text/csv" in content_type and bool(rows) and len(rows) <= 5000 and "node_id" in rows[0]
                else:
                    value = json.loads(body)
                    valid = isinstance(value, dict) and bool(value)
                    if format == "geojson":
                        valid &= value.get("type") == "FeatureCollection" and 0 < len(value.get("features", [])) <= 5000
                record.update({"passed": valid, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
            except Exception as error:
                record["error"] = safe_error(error)
            record["ms"] = (time.monotonic() - started) * 1000
            record["passed"] &= record["ms"] <= 30000
            records.append(record)
            if format != "json":
                last_export = time.monotonic()
    result = {"at": utc_now(), "scale": fixture["kind"], "label": label, "fixture_token": fixture["token"],
              "context": phase_context(run), "passed": all(row["passed"] for row in records),
              "records": records, "last_export_at": time.time()}
    prior.append(result)
    checks = []
    missing = []
    for scale in ("1x", "10x"):
        matches = {kind: [row for row in prior if row["scale"] == scale and row["label"] == kind]
                   for kind in ("baseline", "candidate")}
        if not all(len(values) == 1 for values in matches.values()):
            missing.append(scale)
            continue
        if matches["baseline"][0]["fixture_token"] != matches["candidate"][0]["fixture_token"]:
            missing.append(scale + ":fixture-identity")
            continue
        for name, _, _ in CASES:
            before = [row["ms"] for row in matches["baseline"][0]["records"] if row["name"] == name]
            after = [row["ms"] for row in matches["candidate"][0]["records"] if row["name"] == name]
            old, new = statistics.median(before), statistics.median(after)
            checks.append({"name": scale + ":" + name, "baseline_ms": before, "candidate_ms": after,
                           "change_percent": (new / old - 1) * 100 if old else None, "deadline_ms": 30000,
                           "passed": len(before) == len(after) == 3 and new <= old * 1.1 and max(after) <= 30000})
    write_json(existing, {"at": utc_now(), "context": phase_context(run), "passed": not missing
               and len(checks) == 8 and all(row["passed"] for row in prior) and all(row["passed"] for row in checks),
               "runs": prior, "checks": checks, "missing": missing})
    return result
