"""Reuse the production-route lookup benchmark with the complete stack resident."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import statistics
import threading
import time

from common import REPO, STATE, command, read_json, safe_error, utc_now, write_json
from context import phase_context
from fixtures import bundle, current_targets
from probes import http_get, inventory
from source_identity import expected_sources, identify, validate_measurements


def active_fixture() -> dict:
    selected = read_json(STATE / "runtime/selected-fixture.json")
    current_targets()
    from probes import inspected_containers
    from targets import runtime_targets
    runtime_targets(inspected_containers())
    return bundle(selected["token"])


def node_tool(tool: str, args: list[str], database: str, log: Path, *, timeout: int = 1800) -> int:
    if tool not in ("benchmarkLookups", "benchmarkOperations", "generateSampleDataset", "loadSampleDataset", "prepareSampleDataset",
                    "soakDatabaseLookups", "checkLookupParity", "checkPathContext", "fullStackTraffic", "dataLifecycle"):
        raise ValueError("unknown existing qualification tool")
    endpoint = "postgresql://meshcore:meshcore@127.0.0.1:5433/" + database
    settings = ["env", "DATABASE_URL=" + endpoint, "DATABASE_POOL_MAX=8",
                "DATABASE_STATEMENT_TIMEOUT_MS=30000", "PUBLIC_FEATURE_REGION_SCOPING_ENABLED=true"]
    invocation = [*settings, "node", "--max-old-space-size=512", "--import",
                  str(REPO / "backend/node_modules/tsx/dist/loader.mjs"),
                  str(REPO / f"backend/src/tools/{tool}.ts"), *args]
    log.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with log.open("xb") as output:
        log.chmod(0o600)
        result = command(invocation, output_stream=output, timeout=timeout, check=False)
    if result.stderr:
        error = log.with_suffix(log.suffix + ".stderr")
        error.write_bytes(result.stderr)
        error.chmod(0o600)
    return result.returncode


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    import math
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


class CompanionReads:
    def __init__(self) -> None:
        self.stop = threading.Event()
        self.records: list[dict] = []
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.tasks = []

    def start(self) -> None:
        for name, url in (("corescope", "http://127.0.0.1:3102/api/packets?limit=20"),
                          ("beacon", "http://127.0.0.1:3103/api/v1/packets?limit=20")):
            for _ in range(2):
                self.tasks.append(self.executor.submit(self.reader, name, url))

    def reader(self, name: str, url: str) -> None:
        while not self.stop.is_set():
            start = time.monotonic()
            record = {"name": name, "passed": False}
            try:
                body, _ = http_get(url)
                json.loads(body)
                record["passed"] = True
            except Exception as error:
                record["error"] = safe_error(error)
            record["ms"] = (time.monotonic() - start) * 1000
            self.records.append(record)
            self.stop.wait(max(0, 1 - (time.monotonic() - start)))

    def finish(self) -> dict:
        self.stop.set()
        for task in self.tasks:
            task.result(timeout=15)
        self.executor.shutdown(wait=True)
        result = {}
        for name in ("corescope", "beacon"):
            rows = [row for row in self.records if row["name"] == name]
            result[name] = {"requests": len(rows), "failures": sum(not row["passed"] for row in rows),
                            "p95_ms": percentile([row["ms"] for row in rows], .95)}
        return result


def read_comparison(runs: list[dict]) -> dict:
    missing = []
    comparisons = []
    companion_comparisons = []
    for scale in ("1x", "10x"):
        companion_runs = {label: [row for row in runs if row["scale"] == scale and row["label"] == label]
                          for label in ("baseline", "candidate")}
        if all(len(values) == 1 for values in companion_runs.values()):
            for name in ("corescope", "beacon"):
                before = companion_runs["baseline"][0].get("companions", {}).get(name, {})
                after = companion_runs["candidate"][0].get("companions", {}).get(name, {})
                old, new = before.get("p95_ms"), after.get("p95_ms")
                valid = old is not None and new is not None and before.get("requests", 0) >= 12 and after.get("requests", 0) >= 12
                companion_comparisons.append({"scale": scale, "name": name, "baseline_p95_ms": old,
                    "candidate_p95_ms": new, "passed": valid and before.get("failures") == after.get("failures") == 0
                    and new <= old * 1.1})
        for concurrency in (1, 8):
            for cache in ("miss", "warm"):
                matches = {}
                for label in ("baseline", "candidate"):
                    selected = [row for row in runs if row["scale"] == scale and row["label"] == label]
                    modes = [mode for run in selected for mode in run["modes"]
                             if mode["concurrency"] == concurrency and mode["cache"] == cache]
                    if len(modes) != 1:
                        missing.append(f"{scale}/{label}/c{concurrency}/{cache}")
                    else:
                        matches[label] = modes[0]
                if len(matches) != 2:
                    continue
                grouped = {}
                for label, mode in matches.items():
                    grouped[label] = {}
                    for row in mode["results"]:
                        grouped[label].setdefault(row["name"], []).append(row["p95Ms"])
                if set(grouped["baseline"]) != set(grouped["candidate"]):
                    missing.append(f"{scale}/c{concurrency}/{cache}/case-parity")
                    continue
                for name in grouped["baseline"]:
                    old = statistics.median(grouped["baseline"][name])
                    new = statistics.median(grouped["candidate"][name])
                    comparisons.append({"scale": scale, "concurrency": concurrency, "cache": cache, "name": name,
                                        "baseline_p95_ms": old, "candidate_p95_ms": new,
                                        "change_percent": (new / old - 1) * 100 if old else None,
                                        "three_trials": len(grouped["baseline"][name]) == len(grouped["candidate"][name]) == 3,
                                        "within_budget": new <= (100 if scale == "1x" else 250)})
    return {"passed": not missing and bool(comparisons) and len(companion_comparisons) == 4
            and all(row["three_trials"] and row["within_budget"] for row in comparisons)
            and all(row["passed"] for row in companion_comparisons),
            "missing": missing, "comparisons": comparisons, "companion_comparisons": companion_comparisons}


def reads(run: Path, *, label: str, source_root: Path) -> dict:
    if label not in ("baseline", "candidate"):
        raise ValueError("read label must be baseline or candidate")
    fixture = active_fixture()
    scale = fixture["kind"]
    if scale not in ("1x", "10x"):
        raise ValueError("matched lookup reads require a retained 1x or 10x fixture")
    if not source_root.resolve().is_relative_to(REPO) or not (source_root / "repositories/nodes.ts").is_file():
        raise ValueError("source root must be an existing backend source directory inside this repository")
    expected=expected_sources()
    measured=expected[label]
    if source_root.resolve()!=Path(measured['source_root']):
        raise ValueError('selected lookup source differs from its baseline/candidate authority')
    if not inventory(run)["passed"]:
        raise RuntimeError("complete service inventory must pass before timing")
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    directory = run / f"reads-{scale}-{label}"
    directory.mkdir(mode=0o700)
    modes = []
    companions = CompanionReads()
    companions.start()
    try:
        for concurrency in (1, 8):
            for cache in ("miss", "warm"):
                output = directory / f"c{concurrency}-{cache}"
                args = ["--source-root", str(source_root), "--samples", "20" if concurrency == 1 else "200",
                        "--trials", "3", "--concurrency", str(concurrency), "--cache", cache, "--output", str(output),
                        '--reference-revision',measured['revision']]
                if label == "candidate":
                    args += ["--budget-ms", "100" if scale == "1x" else "250"]
                code = node_tool("benchmarkLookups", args, database, output.with_suffix(".log"))
                result = read_json(output / "lookups.json") if (output / "lookups.json").exists() else {}
                modes.append({"concurrency": concurrency, "cache": cache, "exit_code": code,
                              "passed": code == 0 and bool(result.get("results")) and result.get('sourceIdentity')==measured
                                and result.get('sourceIdentityAfter')==measured and result.get('sourceStable') is True,
                              'source_identity':result.get('sourceIdentity'),'source_stable':result.get('sourceStable'),
                              "results": result.get("results", []), "settings": result.get("settings", {}),
                              "source": str(output / "lookups.json")})
    finally:
        companion_results = companions.finish()
    record = {"at": utc_now(), "scale": scale, "label": label, "fixture_token": fixture["token"],
              'measured_source':measured,
              "context": phase_context(run),
              "modes": modes, "companions": companion_results,
              "passed": len(modes) == 4 and all(mode["passed"] for mode in modes)
              and all(value["requests"] > 0 and value["failures"] == 0 for value in companion_results.values())}
    write_json(directory / "summary.json", record)
    summary_path = run / "reads.json"
    prior = read_json(summary_path).get("runs", []) if summary_path.exists() else []
    prior.append(record)
    validate_measurements(prior,expected_sources())
    comparison = read_comparison(prior)
    summary = {"at": utc_now(), "context": phase_context(run),
               "passed": comparison["passed"] and all(row["passed"] for row in prior),
               "runs": prior, **{key: value for key, value in comparison.items() if key != "passed"}}
    write_json(summary_path, summary)
    return record


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
