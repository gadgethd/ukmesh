"""Native-store evidence for the invented, typed MeshCore workload."""

from __future__ import annotations

import json
import csv
import gzip
import hashlib
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import quote
import uuid

from benchmarks import active_fixture, node_tool
from common import read_json, sql, utc_now, write_json
from context import phase_context


def hex_list(values: list[str], width: int) -> str:
    if not values or len(values) > 360 or any(not re.fullmatch(rf"[A-Fa-f0-9]{{{width}}}", value) for value in values):
        raise ValueError("invented traffic identity list is invalid")
    return ",".join("'" + value + "'" for value in values)


def typed_counts(plan: dict, fixture: dict) -> dict:
    databases = {record["label"]: record for record in fixture["databases"]}
    main = databases["ukmesh"]
    beacon = databases["beacon"]
    hashes = hex_list(plan["ukmeshHashes"], 64)
    radio = hex_list(plan["radioHashes"], 16)
    channels = hex_list(plan["channelHashes"], 64)
    observer = plan["statusObserverId"]
    hex_list([observer], 64)
    ukmesh = json.loads(sql(main["container"], main["database"], main["user"], f"""SELECT json_build_object(
        'packets',(SELECT count(*) FROM mesh.packets WHERE packet_hash IN ({hashes})),
        'paths',(SELECT count(*) FROM mesh.packet_paths WHERE packet_hash IN ({hashes})),
        'facts',(SELECT count(*) FROM mesh.multibyte_path_facts WHERE packet_hash IN ({hashes})),
        'ledger',(SELECT count(*) FROM mesh.ingest_observation_ledger WHERE packet_hash IN ({hashes})),
        'tagged_messages',(SELECT count(*) FROM mesh.message_tags WHERE packet_hash IN ({channels})),
        'statuses',(SELECT count(*) FROM core.node_status_samples WHERE node_id='{observer}' AND battery_mv=4100
          AND uptime_secs BETWEEN 3600 AND {3600 + plan['cycles'] - 1}))"""))
    beacon_values = ",".join("decode(" + value + ",'hex')" for value in radio.split(","))
    beacon_counts = json.loads(sql(beacon["container"], beacon["database"], beacon["user"], f"""SELECT json_build_object(
        'packets',(SELECT count(*) FROM packets WHERE packet_hash IN ({beacon_values})),
        'observations',(SELECT count(*) FROM packet_observations WHERE packet_hash IN ({beacon_values})),
        'messages',(SELECT count(*) FROM channel_messages WHERE packet_hash IN ({beacon_values})),
        'statuses',(SELECT count(*) FROM observer_telemetry m JOIN observers o ON o.id=m.observer_id
          WHERE o.public_key=decode('{observer}','hex') AND m.battery_voltage_mv=4100
          AND m.uptime_seconds BETWEEN 3600 AND {3600 + plan['cycles'] - 1}))"""))
    path = Path(fixture["corescope_data_dir"]) / "analyzer.db"
    with sqlite3.connect("file:" + quote(str(path)) + "?mode=ro", uri=True, timeout=15) as reader:
        reader.execute("BEGIN")
        placeholders = ",".join("?" for _ in plan["radioHashes"])
        transmissions = reader.execute(f"SELECT count(*) FROM transmissions WHERE hash IN ({placeholders})", plan["radioHashes"]).fetchone()[0]
        observations = reader.execute(f"SELECT count(*) FROM observations o JOIN transmissions t ON t.id=o.transmission_id "
                                      f"WHERE t.hash IN ({placeholders})", plan["radioHashes"]).fetchone()[0]
        decrypted = reader.execute(f"SELECT count(*) FROM transmissions WHERE hash IN ({placeholders}) AND payload_type=5 "
                                   "AND json_extract(decoded_json,'$.decryptionStatus')='decrypted'", plan["radioHashes"]).fetchone()[0]
        statuses = reader.execute("SELECT count(*) FROM observer_metrics WHERE observer_id IN (?,?) AND battery_mv=4100 "
                                  "AND tx_air_secs BETWEEN 10 AND ? AND rx_air_secs BETWEEN 20 AND ?",
                                  (observer, observer.lower(), 10 + plan["cycles"] - 1, 20 + plan["cycles"] - 1)).fetchone()[0]
    return {"ukmesh": ukmesh, "beacon": beacon_counts,
            "corescope": {"transmissions": transmissions, "observations": observations, "messages": decrypted, "statuses": statuses}}


def verify_typed(plan: dict, fixture: dict, *, timeout: float = 120) -> dict:
    expected = plan["expected"]
    deadline = time.monotonic() + timeout
    while True:
        counts = typed_counts(plan, fixture)
        checks = []
        for application, wanted in expected.items():
            for name, value in wanted.items():
                checks.append({"name": application + ":" + name, "expected": value,
                               "actual": counts[application][name], "passed": counts[application][name] == value})
        for application, name in (("ukmesh", "tagged_messages"), ("corescope", "messages")):
            checks.append({"name": application + ":" + name, "expected": plan["cycles"],
                "actual": counts[application][name], "passed": counts[application][name] == plan["cycles"]})
        if all(check["passed"] for check in checks) or time.monotonic() >= deadline:
            return {"passed": all(check["passed"] for check in checks), "counts": counts, "checks": checks}
        time.sleep(1)


def traffic(run: Path, *, cycles: int = 1, interval_ms: int = 1000, health_code: str | None = None) -> dict:
    fixture = active_fixture()
    from health import create_session, verify_session
    health = create_session(run, fixture, health_code)
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    output = run / ("typed-" + uuid.uuid4().hex[:10])
    args = ["--output", str(output), "--cycles", str(cycles), "--interval-ms", str(interval_ms)]
    args += ["--health-code", health["code"]]
    code = node_tool("fullStackTraffic", args, database, output.with_suffix(".log"),
                     timeout=int(cycles * interval_ms / 1000 + 120))
    plan = read_json(output / "traffic.json")
    result = verify_typed(plan, fixture)
    health_result = verify_session(health)
    result["checks"] += health_result["checks"]
    result["passed"] &= health_result["passed"]
    result.update({"at": utc_now(), "exit_code": code, "passed": code == 0 and result["passed"],
                   "manifest": str(output / "traffic.json"), "context": phase_context(run)})
    write_json(run / "typed-traffic.json", result)
    return result


def ack_radio_hashes(directory: Path) -> list[str]:
    manifest = read_json(directory / "fixture.json")
    if not manifest.get("synthetic") or manifest.get("seed") != "sequential-ack-v1":
        raise ValueError("invented ACK source is required")
    values = []
    with gzip.open(directory / "raw_packets.csv.gz", "rt", newline="") as source:
        for row in csv.DictReader(source):
            raw = bytes.fromhex(row["raw_hex"])
            if raw[0] >> 2 != 3:
                raise ValueError("ACK oracle received another payload type")
            offset = 5 if raw[0] & 3 in (0, 3) else 1
            length = raw[offset]
            payload = raw[offset + 1 + ((length >> 6) + 1) * (length & 63):]
            values.append(hashlib.sha256(bytes([3]) + payload).hexdigest()[:16])
    if len(values) != manifest["count"] or len(set(values)) != len(values):
        raise ValueError("ACK radio identities are not distinct or complete")
    return values


def companion_ack_counts(directory: Path, fixture: dict) -> dict:
    identities = ack_radio_hashes(directory)
    beacon = next(item for item in fixture["databases"] if item["label"] == "beacon")
    result = {"corescope": {"packets": 0, "observations": 0}, "beacon": {"packets": 0, "observations": 0}}
    path = Path(fixture["corescope_data_dir"]) / "analyzer.db"
    with sqlite3.connect("file:" + quote(str(path)) + "?mode=ro", uri=True, timeout=15) as reader:
        reader.execute("BEGIN")
        for index in range(0, len(identities), 500):
            batch = identities[index:index + 500]
            placeholders = ",".join("?" for _ in batch)
            result["corescope"]["packets"] += reader.execute(
                f"SELECT count(*) FROM transmissions WHERE hash IN ({placeholders})", batch).fetchone()[0]
            result["corescope"]["observations"] += reader.execute(
                f"SELECT count(*) FROM observations o JOIN transmissions t ON t.id=o.transmission_id WHERE t.hash IN ({placeholders})", batch).fetchone()[0]
    for index in range(0, len(identities), 2000):
        batch = identities[index:index + 2000]
        values = ",".join("decode('" + value + "','hex')" for value in batch)
        counts = json.loads(sql(beacon["container"], beacon["database"], beacon["user"], f"""SELECT json_build_object(
            'packets',(SELECT count(*) FROM packets WHERE packet_hash IN ({values})),
            'observations',(SELECT count(*) FROM packet_observations WHERE packet_hash IN ({values})))"""))
        for name in counts:
            result["beacon"][name] += counts[name]
    return result


def verify_companion_acks(directory: Path, fixture: dict, *, timeout: float = 120) -> dict:
    count = read_json(directory / "fixture.json")["count"]
    deadline = time.monotonic() + timeout
    while True:
        values = companion_ack_counts(directory, fixture)
        checks = [{"name": application + ":ACK:" + name, "expected": count, "actual": value, "passed": value == count}
                  for application, counts in values.items() for name, value in counts.items()]
        if all(check["passed"] for check in checks) or time.monotonic() >= deadline:
            return {"passed": all(check["passed"] for check in checks), "checks": checks, "counts": values}
        time.sleep(1)
