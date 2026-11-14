"""Whole-VM resource sampling; cgroup working set and native PSS stay distinct."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import threading
import time

from common import REPO, docker, service_manifest, sql, utc_now, write_json
from context import phase_context
from fixtures import current_targets


MIB = 1024 * 1024


def key_values(path: Path) -> dict[str, int]:
    result = {}
    for line in path.read_text().splitlines():
        parts = line.replace(":", "").split()
        if len(parts) >= 2 and parts[1].isdigit():
            result[parts[0]] = int(parts[1]) * (1024 if len(parts) > 2 and parts[2] == "kB" else 1)
    return result


def host_sample() -> dict:
    memory = key_values(Path("/proc/meminfo"))
    vm = key_values(Path("/proc/vmstat"))
    page_size = os.sysconf("SC_PAGE_SIZE")
    return {"at": utc_now(), "monotonic": time.monotonic(),
            "mem_total_bytes": memory["MemTotal"], "mem_available_bytes": memory["MemAvailable"],
            "swap_used_bytes": memory["SwapTotal"] - memory["SwapFree"],
            "swap_in_bytes_total": vm["pswpin"] * page_size,
            "swap_out_bytes_total": vm["pswpout"] * page_size,
            "disk_free_bytes": shutil.disk_usage(REPO).free,
            "load_average": list(os.getloadavg()),
            "cpu_ticks": [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]}


def native_pss(manifest: dict) -> dict:
    roots = set()
    for service in manifest["services"]:
        if service.get("kind") != "process" or not service.get("pid_file"):
            continue
        marker = (REPO / service["pid_file"]).resolve()
        if not marker.is_relative_to(REPO / "plans"):
            raise ValueError("native PID marker is outside owned plans directory")
        try:
            pid = int(marker.read_text().strip())
            expected = service.get("entrypoint")
            args = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
            if expected and expected not in args:
                raise ValueError(f"native PID ownership mismatch for {service['name']}")
            roots.add(pid)
        except FileNotFoundError:
            continue
    parents = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            status = key_values(entry / "status")
            parents[int(entry.name)] = status["PPid"]
        except (FileNotFoundError, PermissionError, KeyError):
            continue
    selected = set(roots)
    while True:
        descendants = {pid for pid, parent in parents.items() if parent in selected}
        if descendants.issubset(selected):
            break
        selected.update(descendants)
    result = {}
    for pid in selected:
        try:
            result[str(pid)] = key_values(Path(f"/proc/{pid}/smaps_rollup"))["Pss"]
        except FileNotFoundError:
            continue
    return {"processes": result, "total_bytes": sum(result.values())}


def container_samples() -> list[dict]:
    ids = docker(["ps", "-q"]).stdout.decode().split()
    if not ids:
        return []
    inspected = json.loads(docker(["inspect", *ids]).stdout)
    result = []
    for item in inspected:
        pid = item["State"]["Pid"]
        cgroup = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
        unified = next((line.split(":", 2)[2] for line in cgroup if line.startswith("0::")), None)
        if unified is None:
            raise RuntimeError("resource qualification requires cgroup v2 working-set accounting")
        group = (Path("/sys/fs/cgroup") / unified.lstrip("/")).resolve()
        if not group.is_relative_to(Path("/sys/fs/cgroup")):
            raise ValueError("container cgroup path is invalid")
        current = int((group / "memory.current").read_text())
        stat = key_values(group / "memory.stat")
        events = key_values(group / "memory.events")
        io = {"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
        for line in (group / "io.stat").read_text().splitlines():
            for field in line.split()[1:]:
                key, value = field.split("=", 1)
                if key in io:
                    io[key] += int(value)
        result.append({"name": item["Name"].lstrip("/"), "id": item["Id"],
                       "current_bytes": current,
                       "working_bytes": max(0, current - stat.get("inactive_file", 0)),
                       "anon_bytes": stat.get("anon", 0), "file_bytes": stat.get("file", 0),
                       "oom_kills": events.get("oom_kill", 0), "restarts": item["RestartCount"],
                       "oom_killed": item["State"]["OOMKilled"],
                       "started_at": item["State"]["StartedAt"],
                       "health": item["State"].get("Health", {}).get("Status"),
                       "cpu": key_values(group / "cpu.stat"), "io": io,
                       "memory_limit_bytes": item["HostConfig"]["Memory"]})
    return result


def database_samples(targets: dict) -> list[dict]:
    records = []
    for label, container, user in (("ukmesh", "ukmesh-sample-db", "meshcore"),
                                    ("beacon", "ukmesh-companions-beacon-db-1", "beacon_dev")):
        value = sql(container, targets[label], user, """SELECT json_build_object(
          'connections', (SELECT count(*) FROM pg_stat_activity),
          'max_connections', current_setting('max_connections')::integer,
          'active', (SELECT count(*) FROM pg_stat_activity WHERE state='active'),
          'waiting_locks', (SELECT count(*) FROM pg_locks WHERE NOT granted),
          'deadlocks', (SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()),
          'temp_bytes', (SELECT temp_bytes FROM pg_stat_database WHERE datname=current_database()),
          'wal_bytes_position', pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0'),
          'checkpoints_timed', (SELECT checkpoints_timed FROM pg_stat_bgwriter),
          'checkpoints_requested', (SELECT checkpoints_req FROM pg_stat_bgwriter),
          'vacuum_count', (SELECT coalesce(sum(vacuum_count+autovacuum_count),0) FROM pg_stat_user_tables),
          'analyze_count', (SELECT coalesce(sum(analyze_count+autoanalyze_count),0) FROM pg_stat_user_tables),
          'fsync', current_setting('fsync'), 'synchronous_commit', current_setting('synchronous_commit'))""")
        records.append({"name": label, **json.loads(value)})
    return records


def evaluate(samples: list[dict], *, duration: float, warmup: float,
             required_containers: set[str] | None = None) -> dict:
    if not samples:
        return {"passed": False, "reason": "no resource samples"}
    start = samples[0]["host"]["monotonic"]
    steady = [row for row in samples if row["host"]["monotonic"] - start >= warmup]
    if len(steady) < 2:
        return {"passed": False, "reason": "at least two post-warmup samples are required"}
    headroom = sum(row["host"]["mem_available_bytes"] >= 384 * MIB for row in steady) / len(steady)
    memory_peak = max(row["runtime_working_bytes"] for row in samples)
    swap_growth = max(row["host"]["swap_used_bytes"] for row in steady) - steady[0]["host"]["swap_used_bytes"]
    excessive_seconds = 0.0
    sustained_swap = False
    for before, after in zip(steady, steady[1:]):
        elapsed = after["host"]["monotonic"] - before["host"]["monotonic"]
        movement = sum(after["host"][key] - before["host"][key]
                       for key in ("swap_in_bytes_total", "swap_out_bytes_total"))
        excessive_seconds = excessive_seconds + elapsed if elapsed > 0 and movement / elapsed > MIB else 0
        sustained_swap |= excessive_seconds >= 60
    first = {item["name"]: item for item in samples[0]["containers"]}
    required = required_containers if required_containers is not None else set(first)
    complete_coverage = bool(required) and all(required.issubset({item["name"] for item in row["containers"]}) for row in samples)
    healthy_coverage = all(item.get("health") in (None, "healthy") for row in samples
                           for item in row["containers"] if item["name"] in required)
    unstable = set()
    for row in samples:
        unstable.update(set(first) - {item["name"] for item in row["containers"]})
        for item in row["containers"]:
            prior = first.get(item["name"])
            if item["oom_killed"] or item["oom_kills"] > (prior or {}).get("oom_kills", 0):
                unstable.add(item["name"])
            if prior and (item["restarts"] > prior["restarts"] or item["id"] != prior["id"]
                          or item.get("started_at") != prior.get("started_at")):
                unstable.add(item["name"])
    actual_duration = samples[-1]["host"]["monotonic"] - start
    disk_min = min(row["host"]["disk_free_bytes"] for row in samples)
    database_rows = [item for row in samples for item in row.get("databases", [])]
    database_start = {}
    for item in database_rows:
        database_start.setdefault(item["name"], item)
    connection_reserve = bool(database_rows) and all(item["connections"] <= item["max_connections"] - 10 for item in database_rows)
    durable = bool(database_rows) and all(item["fsync"] == "on" and item["synchronous_commit"] == "on" for item in database_rows)
    deadlocks = any(item["deadlocks"] > database_start[item["name"]]["deadlocks"] for item in database_rows)
    checks = {"headroom": headroom >= .95, "runtime_memory": memory_peak <= 3072 * MIB,
              "swap_growth": swap_growth <= 128 * MIB, "swap_churn": not sustained_swap,
              "stable_containers": not unstable, "disk_headroom": disk_min >= 10 * 1024 ** 3,
              "complete_duration": actual_duration >= duration * .98,
              "complete_resident_coverage": complete_coverage,
              "healthy_resident_services": healthy_coverage,
              "database_connection_reserve": connection_reserve,
              "database_durability": durable, "no_new_deadlocks": not deadlocks}
    return {"passed": all(checks.values()), "checks": checks, "sample_count": len(samples),
            "steady_samples": len(steady), "duration_seconds": actual_duration,
            "warmup_seconds": warmup, "headroom_fraction": headroom,
            "runtime_peak_bytes": memory_peak, "swap_growth_bytes": max(0, swap_growth),
            "disk_min_bytes": disk_min, "unstable_containers": sorted(unstable)}


def sample(run: Path, *, seconds: int, interval: float, warmup: int,
           stop_event: threading.Event | None = None) -> dict:
    if not 1 <= seconds <= 86400 or not .5 <= interval <= 60 or warmup < 0 or warmup >= seconds:
        raise ValueError("sample requires 1..86400 seconds, .5..60 interval, and warmup shorter than duration")
    manifest = service_manifest()
    targets = current_targets()
    provenance = phase_context(run)
    output = run / "resources.jsonl"
    if output.exists():
        raise ValueError("resource samples already exist; use a fresh run directory")
    rows = []
    start = time.monotonic()
    next_database_sample = start
    with output.open("x") as stream:
        output.chmod(0o600)
        while True:
            host = host_sample()
            containers = container_samples()
            native = native_pss(manifest)
            row = {"host": host, "containers": containers, "native": native,
                   "runtime_working_bytes": sum(item["working_bytes"] for item in containers) + native["total_bytes"]}
            if time.monotonic() >= next_database_sample:
                row["databases"] = database_samples(targets)
                next_database_sample = time.monotonic() + 60
            rows.append(row)
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            elapsed = time.monotonic() - start
            if elapsed >= seconds or (stop_event and stop_event.is_set()):
                break
            if stop_event:
                stop_event.wait(min(interval, seconds - elapsed))
            else:
                time.sleep(min(interval, seconds - elapsed))
    required = {service.get("container", service["name"]) for service in manifest["services"]
                if service.get("required", True) and service.get("kind", "container") == "container"}
    result = evaluate(rows, duration=seconds, warmup=warmup, required_containers=required)
    result["required_containers"] = sorted(required)
    result["context"] = provenance
    write_json(run / "resources.json", result)
    return result
