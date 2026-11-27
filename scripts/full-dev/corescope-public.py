#!/usr/bin/env python3
"""Switch the existing CoreScope pair between owned MQTT and public projections."""
from __future__ import annotations

import argparse
from copy import deepcopy
from contextlib import closing
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent / "qualification"))
from common import REPO, STATE, command, docker, owned_run, read_json, safe_error, utc_now, write_json
from context import phase_context, corescope_application_context
from fixtures import bundle, current_targets

TOKEN = "local-full-public-stream-token-0123456789abcdef"
BACKEND = "http://ukmesh-full-backend-1:3000/internal/ingest/public"
PAIR = ("corescope-ingestor", "corescope")


def active_fixture() -> dict:
    current_targets()
    selected = read_json(STATE / "runtime/selected-fixture.json")
    if not selected.get("token"):
        raise ValueError("public-source qualification requires an owned fixture selection")
    value = bundle(selected["token"])
    normal = Path(value["corescope_data_dir"])
    expected = STATE / "runtime/fixtures" / value["token"] / "corescope"
    if normal != expected or normal.resolve() != expected:
        raise ValueError("CoreScope fixture directory must be canonical and owned")
    return value


def public_config(base: dict, database: str) -> dict:
    if not re.fullmatch(r"lean_(?:fixture|1x|10x|soak)_[a-z0-9_]{1,38}", database):
        raise ValueError("public projection requires an owned fixture database")
    cfg = deepcopy(base)
    cfg.update({"dbPath": "/data/analyzer.db", "publicProjection": True,
                "publicAuthorityURL": BACKEND + "/policy", "publicAuthorityToken": TOKEN,
                "mqttSources": [],
                "postgresSources": [{
                    "name": "ukmesh-public", "dsn": "postgres://corescope_ro:local-full-dev-only@ukmesh-sample-db:5432/" + database + "?sslmode=disable",
                    "startTime": "1970-01-01T00:00:00Z", "statePath": "/data/public-pg.json",
                    "pollIntervalSec": 1, "batchSize": 100, "maxRowsPerSec": 100,
                    "maxEnvelopePastDays": -1,
                }],
                "redisSources": [{
                    "name": "ukmesh-public-live", "url": "", "publicURL": BACKEND + "/entries",
                    "publicToken": TOKEN, "statePath": "/data/public-http.json",
                    "pollIntervalMs": 100, "batchSize": 100, "maxRowsPerSec": 100,
                    "maxEnvelopePastDays": -1,
                }]})
    return cfg


def projection_directory(fixture: dict) -> Path:
    directory = STATE / "runtime/fixtures" / fixture["token"] / "corescope-public"
    if directory.resolve() != directory:
        raise ValueError("projection directory must not resolve through symlinks")
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory


def prepare(fixture: dict) -> Path:
    directory = projection_directory(fixture)
    sqlite = directory / "analyzer.db"
    if sqlite.exists():
        with closing(sqlite3.connect(sqlite.as_uri() + "?mode=ro", uri=True)) as connection:
            installed = connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='public_projection_state'").fetchone()[0]
            populated = connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
            if populated and not installed:
                raise ValueError("public projection refuses an existing non-revocable SQLite database")
    database = next(store["database"] for store in fixture["databases"] if store["label"] == "ukmesh")
    base = read_json(REPO / "scripts/companion-dev/corescope.json")
    write_json(directory / "config.json", public_config(base, database))
    return directory


def compose(fixture: dict, *, public: bool, args: list[str]) -> None:
    invocation = ["docker", "compose", "--project-name", "ukmesh-companions", "--env-file", "/dev/null",
                  "-f", str(REPO / "scripts/companion-dev/compose.yml")]
    if public:
        invocation += ["-f", str(REPO / "scripts/full-dev/corescope-public.yml")]
    invocation += args
    config = projection_directory(fixture) / "config.json" if public else REPO / "scripts/companion-dev/corescope.json"
    invocation = ["bash", str(REPO / "scripts/companion-dev/revisions.sh"),
                  "--component", "corescope", "--core-config", str(config), "--", *invocation]
    environment = ["env", "CORESCOPE_DATA_DIR=" + fixture["corescope_data_dir"],
                   "CORESCOPE_PUBLIC_DIR=" + str(projection_directory(fixture)),
                   "COMPANION_UID=" + str(os.getuid()), "COMPANION_GID=" + str(os.getgid())]
    command([*environment, "sg", "docker", "-c", shlex.join(invocation)], timeout=240)


def switch(public: bool) -> dict:
    fixture = active_fixture()
    if public:
        prepare(fixture)
    for service in PAIR:
        compose(fixture, public=public, args=["up", "-d", "--no-deps", "--force-recreate",
                                            "--wait", "--wait-timeout", "180", service])
    result = {"at": utc_now(), "mode": "public" if public else "mqtt", "fixture_token": fixture["token"],
              "data_dir": str(projection_directory(fixture)) if public else fixture["corescope_data_dir"],
              "passed": True}
    write_json(STATE / "runtime/corescope-mode.json", result)
    return result


def status() -> dict:
    fixture = active_fixture()
    public = str(projection_directory(fixture))
    inspected = json.loads(docker(["inspect", *("ukmesh-companions-" + service + "-1" for service in PAIR)]).stdout)
    directories = []
    for item in inspected:
        if item["Config"]["Labels"].get("com.docker.compose.project") != "ukmesh-companions":
            raise ValueError("CoreScope container ownership differs")
        directories.append(next(mount["Source"] for mount in item["Mounts"] if mount["Destination"] == "/data"))
        if not item["State"]["Running"]:
            raise RuntimeError("CoreScope process is not running")
    if directories == [public, public]:
        mode = "public"
    elif directories == [fixture["corescope_data_dir"]] * 2:
        mode = "mqtt"
    else:
        raise ValueError("CoreScope pair uses mixed or unowned measurement stores")
    return {"passed": True, "mode": mode, "fixture_token": fixture["token"], "data_dir": directories[0]}


def public_variant(fixture: dict, *, ram: bool) -> dict:
    base = read_json(REPO / "scripts/companion-dev/corescope.json")
    database = next(store["database"] for store in fixture["databases"] if store["label"] == "ukmesh")
    config = public_config(base, database)
    return {"mode": "public-ram" if ram else "public-postgresql", "fixture_token": fixture["token"],
            "projection_directory": str(projection_directory(fixture)),
            "core_changed_fields": sorted(key for key in base.keys() | config.keys() if base.get(key) != config.get(key)),
            "backend_changed_fields": ["MQTT_INGEST_MODE", "MQTT_RAM_STREAM_KEY", "MQTT_RAM_CONSUMER_GROUP", "MQTT_RAM_CONSUMER_NAME"] if ram else []}


def qualify_pg(run: Path) -> dict:
    result = {"passed": False, "checks": []}
    attempt = Path(tempfile.mkdtemp(prefix="corescope-public-pg-", dir=run))
    try:
        application = corescope_application_context()
        switch(True)
        settings = STATE / "runtime/corescope-mode.json"
        log = attempt / "proof.log"
        with log.open("xb") as output:
            log.chmod(0o600)
            completed = command(["node", "--max-old-space-size=256", "--import",
                str(REPO / "backend/node_modules/tsx/dist/loader.mjs"),
                str(REPO / "scripts/full-dev/corescope-public-proof.mjs"), str(settings), str(attempt)],
                output_stream=output, timeout=600, check=False)
        if completed.stderr:
            error = log.with_suffix(".stderr")
            error.write_bytes(completed.stderr)
            error.chmod(0o600)
        if (attempt / "corescope-public-pg.json").exists():
            result = read_json(attempt / "corescope-public-pg.json")
        result["variant"] = public_variant(active_fixture(), ram=False)
        result["application"] = application
        if corescope_application_context() != application:
            result["passed"] = False
            raise RuntimeError("CoreScope application changed during the public PostgreSQL proof")
        result["context"] = phase_context(run)
        result["attempt"] = str(attempt)
        if completed.returncode:
            result["passed"] = False
            raise RuntimeError("CoreScope public PostgreSQL proof failed; private diagnostics retained")
    finally:
        try:
            switch(False)
            result["restored_mqtt"] = True
        except Exception:
            result.update({"passed": False, "restored_mqtt": False})
            raise
        finally:
            write_json(run / "corescope-public-pg.json", result)
    return result


def redis(*args: str):
    return json.loads(docker(["exec", "ukmesh-sample-redis", "redis-cli", "--json", *args]).stdout)


def ram_backend(fixture: dict, mode: str, attempt: str) -> None:
    command(["sg", "docker", "-c", shlex.join(["bash",
        str(REPO / "scripts/full-dev/corescope-ram-runtime.sh"), mode, fixture["token"], attempt])], timeout=240)


def drain_stream(key: str, group: str, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if not redis("EXISTS", key):
            return
        consumers = redis("XINFO", "GROUPS", key)
        current = next((value for value in consumers if value["name"] == group), None)
        if current and current["pending"] == 0 and current.get("lag") == 0:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("owned RAM stream did not drain; AOF must remain enabled")
        time.sleep(.2)


def qualify_ram(run: Path) -> dict:
    fixture = active_fixture()
    if fixture["kind"] != "sample":
        raise ValueError("RAM privacy faults require the small owned sample fixture")
    inspected = json.loads(docker(["inspect", "ukmesh-sample-redis", "ukmesh-full-mqtt-ingest-1"]).stdout)
    if inspected[0]["Config"]["Labels"].get("com.docker.compose.project") != "ukmesh-sample":
        raise ValueError("Redis container ownership differs")
    before_env = dict(setting.split("=", 1) for setting in inspected[1]["Config"]["Env"])
    if before_env.get("MQTT_INGEST_MODE") != "durable":
        raise ValueError("RAM qualification must start from the normal durable intake process")
    previous = redis("CONFIG", "GET", "appendonly", "appendfsync", "maxmemory", "maxmemory-policy")
    attempt = Path(tempfile.mkdtemp(prefix="corescope-public-ram-", dir=run))
    write_json(attempt / "redis-before.json", previous)
    identity = uuid.uuid4().hex[:12]
    key = "meshcore:ingest:lean:" + fixture["token"] + ":corescope:" + identity
    group = "lean-corescope-" + fixture["token"] + "-" + identity
    result = {"passed": False, "checks": [], "attempt": str(attempt)}
    result["variant"] = public_variant(fixture, ram=True)
    result["ram_settings"] = {"stream_key": key, "redis_maxmemory": 128 * 1048576,
                              "redis_policy": "noeviction", "redis_appendonly": "yes", "redis_appendfsync": "always"}
    cleanup_errors = []
    try:
        application = corescope_application_context()
        result["application"] = application
        redis("CONFIG", "SET", "maxmemory", str(128 * 1048576), "maxmemory-policy", "noeviction",
              "appendfsync", "always", "appendonly", "yes")
        deadline = time.monotonic() + 60
        while "aof_rewrite_in_progress:1" in docker(["exec", "ukmesh-sample-redis", "redis-cli", "INFO", "persistence"]).stdout.decode():
            if time.monotonic() >= deadline:
                raise RuntimeError("Redis AOF initialization did not complete")
            time.sleep(.2)
        persistence = docker(["exec", "ukmesh-sample-redis", "redis-cli", "INFO", "persistence"]).stdout.decode()
        if "aof_enabled:1" not in persistence or "aof_last_bgrewrite_status:ok" not in persistence:
            raise RuntimeError("Redis AOF durability did not initialize successfully")
        ram_backend(fixture, "ram", identity)
        switch(True)
        log = attempt / "proof.log"
        with log.open("xb") as output:
            log.chmod(0o600)
            completed = command(["node", "--max-old-space-size=256", "--import",
                str(REPO / "backend/node_modules/tsx/dist/loader.mjs"),
                str(REPO / "scripts/full-dev/corescope-ram-proof.mjs"),
                str(STATE / "runtime/corescope-mode.json"), str(attempt), key],
                output_stream=output, timeout=600, check=False)
        if completed.stderr:
            log.with_suffix(".stderr").write_bytes(completed.stderr)
            log.with_suffix(".stderr").chmod(0o600)
        if (attempt / "corescope-public-ram.json").exists():
            result.update(read_json(attempt / "corescope-public-ram.json"))
        if corescope_application_context() != application:
            result["passed"] = False
            raise RuntimeError("CoreScope application changed during the public RAM proof")
        if completed.returncode:
            result["passed"] = False
            raise RuntimeError("CoreScope RAM public tail proof failed; private diagnostics retained")
    finally:
        for name, operation in (("MQTT pair", lambda: switch(False)),
                                ("RAM drain", lambda: drain_stream(key, group))):
            try:
                operation()
            except Exception as error:
                cleanup_errors.append(name + ": " + safe_error(error))
        try:
            ram_backend(fixture, "durable", identity)
            result["restored_durable"] = True
        except Exception as error:
            cleanup_errors.append("durable backend: " + safe_error(error))
        drained = False
        if result.get("restored_durable"):
            try:
                drain_stream(key, group)
                drained = True
                result["final_stream_drained"] = True
            except Exception as error:
                cleanup_errors.append("final RAM drain: " + safe_error(error))
        if drained:
            try:
                redis("CONFIG", "SET", *(item for pair in previous.items() for item in pair))
                result["restored_redis_settings"] = True
            except Exception as error:
                cleanup_errors.append("Redis settings: " + safe_error(error))
        try:
            result["restored_mqtt"] = status()["mode"] == "mqtt"
            if not result["restored_mqtt"]:
                raise RuntimeError("CoreScope did not restore MQTT measurement ownership")
        except Exception as error:
            cleanup_errors.append("pair status: " + safe_error(error))
        result["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            result["passed"] = False
        result["context"] = phase_context(run)
        write_json(run / "corescope-public-ram.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "start", "status", "restart", "restore", "qualify-pg", "qualify-ram"))
    parser.add_argument("--run")
    args = parser.parse_args()
    try:
        if args.action == "prepare":
            fixture = active_fixture()
            prepare(fixture)
            result = {"passed": True, "fixture_token": fixture["token"]}
        elif args.action in ("start", "restart", "restore"):
            result = switch(args.action != "restore")
        elif args.action == "status":
            result = status()
        elif args.action == "qualify-pg":
            result = qualify_pg(owned_run(args.run, create=not args.run))
        else:
            result = qualify_ram(owned_run(args.run, create=not args.run))
        print(json.dumps(result))
        return 0 if result.get("passed") else 1
    except Exception as error:
        print(json.dumps({"passed": False, "error": safe_error(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
