"""Manifest-driven liveness and native work checks; readiness alone is insufficient."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time

from common import REPO, command, docker, safe_error, service_manifest, sql, utc_now, write_json
from context import phase_context
from fixtures import current_targets
from inventory_schema import coverage
from loopback_http import http_get


def inspected_containers() -> dict:
    listing = docker(["ps", "-aq"]).stdout.decode().split()
    if not listing:
        return {}
    return {item["Name"].lstrip("/"): item for item in json.loads(docker(["inspect", *listing]).stdout)}


def inventory(run: Path) -> dict:
    manifest = service_manifest()
    declared = coverage(REPO, manifest)
    containers = inspected_containers()
    tunnel_off = not any(item["State"]["Running"] and
        item["Config"].get("Labels", {}).get("com.docker.compose.project") == "ukmesh-full" and
        item["Config"].get("Labels", {}).get("com.docker.compose.service") == "cloudflared"
        for item in containers.values())
    from targets import runtime_targets
    target_checks = runtime_targets(containers)
    checks = []
    for service in manifest["services"]:
        required = service.get("required", True)
        name = service["name"]
        record = {"name": name, "required": required, "passed": False}
        try:
            kind = service.get("kind", "container")
            if kind in ("container", "oneshot"):
                item = containers.get(service.get("container", name))
                if item is None:
                    raise RuntimeError("container is absent")
                state = item["State"]
                record.update({"state": state["Status"], "exit_code": state["ExitCode"],
                               "health": state.get("Health", {}).get("Status"),
                               "restarts": item["RestartCount"], "oom_killed": state["OOMKilled"]})
                if kind == "oneshot":
                    record["passed"] = state["Status"] == "exited" and state["ExitCode"] == 0
                else:
                    record["passed"] = state["Running"] and record["health"] in (None, "healthy") and not state["OOMKilled"]
            elif kind == "process":
                marker = (REPO / service["pid_file"]).resolve()
                if not marker.is_relative_to(REPO / "plans"):
                    raise ValueError("PID marker is outside owned state")
                pid = int(marker.read_text().strip())
                args = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
                expected = service.get("entrypoint")
                if not expected or expected not in args:
                    raise RuntimeError("native process ownership was not proved")
                record.update({"pid": pid, "passed": True})
            else:
                raise ValueError("unknown service kind")
            if service.get("health_url"):
                http_get(service["health_url"])
        except Exception as error:
            record.update({"passed": False, "error": safe_error(error)})
        checks.append(record)
    result = {"at": utc_now(), "passed": tunnel_off and declared["passed"] and bool(checks) and all(c["passed"] for c in checks if c["required"]),
              "public_tunnel_off": tunnel_off,
              "coverage": declared, "actual_store_consumers": target_checks,
              "services": checks, "context": phase_context(run)}
    write_json(run / "inventory.json", result)
    return result


def numeric_check(value: float, spec: dict) -> dict:
    minimum = spec.get("min", 1)
    maximum = spec.get("max")
    return {"value": value, "min": minimum, "max": maximum,
            "passed": value >= minimum and (maximum is None or value <= maximum)}


def work_probe(spec: dict) -> dict:
    kind = spec["kind"]
    result = {"kind": kind, "description": spec.get("description", ""), "passed": False}
    if kind == "sql":
        query = spec["query"]
        database = spec.get("database")
        if spec.get("database_target"):
            target = spec["database_target"]
            if target not in ("ukmesh", "owner", "beacon"):
                raise ValueError("unknown database target alias")
            database = current_targets()[target]
        if not database:
            raise ValueError("SQL work oracle requires a database or database_target")
        # Work oracles are read-only. Mutating permission tests have a separate owner.
        value = sql(spec["container"], database, spec.get("user", "meshcore"),
                    "BEGIN READ ONLY; SET LOCAL statement_timeout='15s'; " + query + "; COMMIT;", timeout=20)
        result.update(numeric_check(float(value.splitlines()[-1]), spec))
        result["query_sha256"] = hashlib.sha256(query.encode()).hexdigest()
    elif kind in ("http", "metric"):
        body, content_type = http_get(spec["url"])
        if kind == "metric":
            pattern = re.compile(spec["pattern"], re.MULTILINE)
            matched = pattern.search(body.decode())
            if not matched or not matched.lastindex:
                raise ValueError("metric oracle must match and capture a numeric value")
            result.update(numeric_check(float(matched.group(1)), spec))
        elif "json_path" in spec:
            value = json.loads(body)
            for component in spec["json_path"].split("."):
                if component:
                    value = value[int(component)] if isinstance(value, list) else value[component]
            if "equals" in spec:
                result["passed"] = value == spec["equals"]
            else:
                result.update(numeric_check(len(value) if isinstance(value, (list, dict, str)) else float(value), spec))
        elif "pattern" in spec:
            result["passed"] = re.search(spec["pattern"], body.decode()) is not None
        else:
            raise ValueError("HTTP work oracle requires a JSON assertion or content pattern")
        result.update({"response_sha256": hashlib.sha256(body).hexdigest(), "content_type": content_type})
    elif kind == "file":
        path = (REPO / spec["path"]).resolve()
        if not path.is_relative_to(REPO / "plans"):
            raise ValueError("file work oracle must be inside owned runtime state")
        stat = path.stat()
        result.update(numeric_check(stat.st_size, spec))
        if "max_age_seconds" in spec:
            result["passed"] &= time.time() - stat.st_mtime <= spec["max_age_seconds"]
    elif kind == "command":
        completed = command(spec["command"], timeout=spec.get("timeout", 30), check=False)
        result.update({"passed": completed.returncode == 0, "exit_code": completed.returncode,
                       "output_sha256": hashlib.sha256(completed.stdout).hexdigest()})
    else:
        raise ValueError("unknown work oracle kind")
    return result


def smoke(run: Path) -> dict:
    live = inventory(run)
    manifest = service_manifest()
    records = []
    for service in manifest["services"]:
        if not service.get("required", True):
            continue
        checks = service.get("work", [])
        if isinstance(checks, dict):
            checks = [checks]
        record = {"name": service["name"], "checks": [], "passed": False}
        for spec in checks:
            try:
                record["checks"].append(work_probe(spec))
            except Exception as error:
                record["checks"].append({"passed": False, "error": safe_error(error),
                                         "description": spec.get("description", "")})
        record["passed"] = bool(checks) and all(item["passed"] for item in record["checks"])
        if not checks:
            record["error"] = "required service has no native work oracle"
        records.append(record)
    result = {"at": utc_now(), "passed": live["passed"] and bool(records) and all(item["passed"] for item in records),
              "services": records, "context": phase_context(run)}
    write_json(run / "work-smoke.json", result)
    return result
