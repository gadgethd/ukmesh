"""Shared companion read lifecycle and raw evidence; callers own transport/scope."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
import math
import os
import re
import uuid
from pathlib import Path
import stat
import threading
import time

from common import EVIDENCE, safe_error, utc_now


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


COMPANION_ACTORS = {f"{name}-{index}": name for name in ("corescope", "beacon") for index in (1, 2)}


def companion_summary(records: list[dict]) -> dict:
    return {name: {"requests": len(rows), "failures": sum(not row["passed"] for row in rows),
                   "p95_ms": percentile([row["ms"] for row in rows], .95)}
            for name in ("corescope", "beacon")
            for rows in ([row for row in records if row["name"] == name],)}


class CompanionReads:
    def __init__(self, request, *, cadence_seconds: float = 1) -> None:
        if type(cadence_seconds) not in (int, float) or not math.isfinite(cadence_seconds) or cadence_seconds <= 0:
            raise ValueError('positive finite companion cadence required')
        self.request = request
        self.cadence = cadence_seconds
        self.collector_id = uuid.uuid4().hex
        self.stop = threading.Event()
        self.records: list[dict] = []
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.tasks = []
        self.boundaries: list[dict] = []
        self.attempted = {actor: 0 for actor in COMPANION_ACTORS}
        self.started_at = self.finished_at = self.stop_requested_at = None
        self.origin = self.finished_offset = self.stop_requested_offset = None

    def set_scope(self, scope: str) -> None:
        if self.origin is None:
            raise RuntimeError("companion collector has not started")
        self.boundaries.append({"id": len(self.boundaries), "scope": scope,
                                "at": utc_now(), "offset_seconds": time.monotonic() - self.origin})

    def start(self) -> None:
        if self.origin is not None:
            raise RuntimeError("companion collector cannot be started twice")
        self.started_at, self.origin = utc_now(), time.monotonic()
        self.set_scope("setup")
        for name, url in (("corescope", "http://127.0.0.1:3102/api/packets?limit=20"),
                          ("beacon", "http://127.0.0.1:3103/api/v1/packets?limit=20")):
            for index in (1, 2):
                self.tasks.append(self.executor.submit(self.reader, name, url, f"{name}-{index}"))

    def reader(self, name: str, url: str, actor: str) -> None:
        sequence = 0
        while not self.stop.is_set():
            self.attempted[actor] += 1
            started_at = utc_now()
            start = time.monotonic()
            record = {"name": name, "passed": False}
            try:
                body, _ = self.request(url)
                json.loads(body)
                record["passed"] = True
            except Exception as error:
                record["error"] = safe_error(error)
            # Retain the original HTTP/body/JSON latency boundary. Telemetry
            # serialization and mode attribution happen after this timestamp.
            end = time.monotonic()
            record["ms"] = (end - start) * 1000
            record.update({"actor": actor, "sequence": sequence, "started_at": started_at,
                           "finished_at": utc_now(), "start_offset_seconds": start - self.origin,
                           "end_offset_seconds": end - self.origin})
            self.records.append(record)
            sequence += 1
            self.stop.wait(max(0, self.cadence - (time.monotonic() - start)))

    def finish(self) -> dict:
        if self.stop_requested_at is None:
            self.stop_requested_at = utc_now()
            self.stop_requested_offset = time.monotonic() - self.origin if self.origin is not None else 0
        self.stop.set()
        failures = []
        try:
            for task in self.tasks:
                try:
                    task.result(timeout=15)
                except Exception as error:
                    failures.append(error)
        finally:
            self.executor.shutdown(wait=True)
            self.finished_at = utc_now()
            self.finished_offset = time.monotonic() - self.origin if self.origin is not None else 0
        if failures:
            raise ExceptionGroup("companion reader cleanup failed", failures)
        return companion_summary(self.records)

    def evidence(self) -> dict:
        records = []
        for row in self.records:
            def scope_at(offset):
                return max((item["id"] for item in self.boundaries if item["offset_seconds"] <= offset), default=0)
            records.append({**row, "scope_at_start": scope_at(row["start_offset_seconds"]),
                            "scope_at_finish": scope_at(row["end_offset_seconds"])})
        return {"collector_id": self.collector_id, "started_at": self.started_at, "origin_monotonic": self.origin,
                "stop_requested_at": self.stop_requested_at, "stop_requested_offset_seconds": self.stop_requested_offset,
                "finished_at": self.finished_at, "finished_offset_seconds": self.finished_offset,
                "actors": COMPANION_ACTORS, "cadence_seconds": self.cadence, "attempted": self.attempted,
                "boundaries": self.boundaries, "records": records}


def validate_companion_samples(value: dict, *, cadence_seconds: float = 1, scopes=None) -> dict:
    collector = value["collector"]
    finite = lambda number: type(number) in (int, float) and math.isfinite(number)
    def utc(value):
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("companion UTC evidence must be timezone-aware")
        return parsed
    if not re.fullmatch("[a-f0-9]{32}", collector.get("collector_id", "")):
        raise ValueError("independent companion collector identity required")
    if collector["actors"] != COMPANION_ACTORS or collector["cadence_seconds"] != cadence_seconds \
            or not finite(collector["origin_monotonic"]) or collector["origin_monotonic"] <= 0:
        raise ValueError("independent companion actors or cadence differ")
    stop, finished = collector["stop_requested_offset_seconds"], collector["finished_offset_seconds"]
    if not all(finite(item) for item in (stop, finished)) or not 0 <= stop <= finished \
            or not utc(collector["started_at"]) <= utc(collector["stop_requested_at"]) <= utc(collector["finished_at"]):
        raise ValueError("companion stop/join lifecycle evidence differs")
    boundaries = collector["boundaries"]
    expected_scopes = ['c1-miss', 'c1-warm', 'c8-miss', 'c8-warm'] if scopes is None else scopes
    if not boundaries or [item["id"] for item in boundaries] != list(range(len(boundaries))) \
            or boundaries[0]["scope"] != "setup" or boundaries[-1]["scope"] != "teardown" \
            or [item['scope'] for item in boundaries if item['scope'] not in ('setup', 'between', 'teardown')] != expected_scopes:
        raise ValueError("complete companion mode boundaries are required")
    previous = 0
    for boundary in boundaries:
        offset = boundary["offset_seconds"]
        if not finite(offset) or not previous <= offset <= finished \
                or not utc(collector["started_at"]) <= utc(boundary["at"]) <= utc(collector["finished_at"]):
            raise ValueError("invalid companion mode boundary")
        previous = offset
    records = collector["records"]
    if set(collector["attempted"]) != set(COMPANION_ACTORS) or any(type(count) is not int or count < 1 for count in collector["attempted"].values()):
        raise ValueError("all companion actors must complete their attempted reads")
    for actor, name in COMPANION_ACTORS.items():
        rows = sorted((row for row in records if row["actor"] == actor), key=lambda row: row["sequence"])
        if not rows or any(type(row["sequence"]) is not int for row in rows) or [row["sequence"] for row in rows] != list(range(len(rows))) \
                or collector["attempted"].get(actor) != len(rows) or any(row["name"] != name for row in rows):
            raise ValueError("companion actor samples are missing or duplicated")
        if any(later["start_offset_seconds"] < max(earlier["end_offset_seconds"], earlier["start_offset_seconds"]+cadence_seconds)-1e-6 for earlier, later in zip(rows, rows[1:])):
            raise ValueError("companion actor cadence or serial requests differ")
        for row in rows:
            start, end, elapsed = row["start_offset_seconds"], row["end_offset_seconds"], row["ms"]
            if not all(finite(item) for item in (start, end, elapsed)) or not 0 <= start <= end <= finished \
                    or not math.isclose(elapsed, (end-start)*1000, rel_tol=1e-9, abs_tol=1e-6) \
                    or type(row["passed"]) is not bool \
                    or not utc(collector["started_at"]) <= utc(row["started_at"]) <= utc(row["finished_at"]) <= utc(collector["finished_at"]):
                raise ValueError("invalid companion sample timing/outcome")
            for key, offset in (("scope_at_start", start), ("scope_at_finish", end)):
                if row[key] != max((item["id"] for item in boundaries if item["offset_seconds"] <= offset), default=0):
                    raise ValueError("companion sample scope differs from actual mode boundaries")
    if len(records) != sum(collector["attempted"].values()) or any(row["actor"] not in COMPANION_ACTORS for row in records):
        raise ValueError("unexpected companion observations")
    summary = companion_summary(records)
    if summary != value["summary"]:
        raise ValueError("companion summary differs from all raw samples")
    return summary


def measured_companion_data(record: dict, *, cadence_seconds: float = 1, scopes=None) -> dict:
    pointer = record["companion_evidence"]
    path = Path(pointer["path"])
    if not path.resolve().is_relative_to(EVIDENCE.resolve()) or path.is_symlink():
        raise ValueError("companion raw evidence must stay in the owned run")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 16*1024*1024:
            raise ValueError("bounded regular companion raw evidence required")
        data = stream.read(16*1024*1024+1)
    if len(data) > 16*1024*1024:
        raise ValueError("companion raw evidence exceeds its input budget")
    if hashlib.sha256(data).hexdigest() != pointer["sha256"]:
        raise ValueError("companion raw evidence digest differs")
    value = json.loads(data)
    if value.get("version") != 1 or any(value.get(key) != record.get(key) for key in
            ("scale", "label", "fixture_token", "measured_source", "context", "protocol", "read_window", "diagnostic_only")):
        raise ValueError("companion raw evidence identity differs")
    if value.get("collector", {}).get("collector_id") != pointer.get("collector_id"):
        raise ValueError("companion collector identity differs from its raw evidence")
    summary = validate_companion_samples(value, cadence_seconds=cadence_seconds, scopes=scopes)
    if summary != record["companions"]:
        raise ValueError("reported companion aggregate differs from raw evidence")
    return value


def measured_companions(record: dict, *, cadence_seconds: float = 1) -> dict:
    return measured_companion_data(record, cadence_seconds=cadence_seconds)['summary']
