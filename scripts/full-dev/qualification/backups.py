"""Online originals backups, verified by restore into newly owned databases."""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import time
from urllib.parse import quote

from common import DB_NAME, REPO, digest, docker, sql, utc_now, write_json


ORIGINALS = (
    ("ukmesh", "ukmesh-sample-db", "meshcore", "meshcore"),
    ("owner", "ukmesh-sample-db", "meshcore_owner_auth", "meshcore"),
    ("beacon", "ukmesh-companions-beacon-db-1", "beacon_dev", "beacon_dev"),
)
SQLITE = REPO / "plans/companion-dev/runtime/corescope/analyzer.db"


def postgres_backup(run: Path, label: str, container: str,
                    database: str, user: str) -> dict:
    archive = run / f"{label}.dump"
    if archive.exists():
        raise ValueError(f"backup already exists: {label}")
    inspect = docker(["inspect", "--format", "{{.Id}}", container]).stdout.decode().strip()
    fd = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        docker(["exec", container, "pg_dump", "-U", user, "-d", database,
                "-Fc", "--no-owner", "--no-acl"], output_stream=output, timeout=1800)
    verify_db = f"lean_verify_{time.time_ns()}_{label}"
    if not DB_NAME.fullmatch(verify_db):
        raise ValueError("verification database name rejected")
    record = {"kind": "postgres", "container": container, "container_id": inspect,
              "database": database, "archive": archive.name, "sha256": digest(archive),
              "bytes": archive.stat().st_size, "verified": False,
              "verification_database": verify_db}
    created = False
    try:
        docker(["exec", container, "createdb", "-U", user, "-T", "template0", verify_db])
        created = True
        has_timescale = sql(container, database, user,
                            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')") == "t"
        if has_timescale:
            sql(container, verify_db, user,
                "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();")
        with archive.open("rb") as source:
            docker(["exec", "-i", container, "pg_restore", "-U", user, "-d", verify_db,
                    "--no-owner", "--no-acl", "--exit-on-error"], input_stream=source, timeout=1800)
        if has_timescale:
            sql(container, verify_db, user, "SELECT timescaledb_post_restore();")
        record["restored_table_count"] = int(sql(container, verify_db, user,
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')"))
        record["verified"] = True
    finally:
        if created:
            # This name was generated and created above; never accept an external drop target.
            docker(["exec", container, "dropdb", "-U", user, verify_db])
    return record


def sqlite_backup(run: Path, source: Path = SQLITE) -> dict:
    target = run / "corescope.sqlite"
    if target.exists():
        raise ValueError("CoreScope backup already exists")
    if not source.is_file():
        raise ValueError("CoreScope original SQLite database is missing")
    uri = "file:" + quote(str(source.resolve())) + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as reader:
        with sqlite3.connect(target) as writer:
            reader.backup(writer, pages=256, sleep=0.05)
            integrity = [row[0] for row in writer.execute("PRAGMA integrity_check")]
            foreign_keys = writer.execute("PRAGMA foreign_key_check").fetchall()
            table_count = writer.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
    target.chmod(0o600)
    return {"kind": "sqlite", "archive": target.name, "sha256": digest(target),
            "bytes": target.stat().st_size, "verified": integrity == ["ok"] and not foreign_keys,
            "integrity_ok": integrity == ["ok"], "foreign_key_violations": len(foreign_keys),
            "restored_table_count": table_count}


def backup_originals(run: Path) -> dict:
    records = []
    try:
        for label, container, database, user in ORIGINALS:
            records.append(postgres_backup(run, label, container, database, user))
            write_json(run / "backups.json", {"started_at": utc_now(), "passed": False, "stores": records})
        records.append(sqlite_backup(run))
    finally:
        result = {"completed_at": utc_now(), "passed": len(records) == 4 and all(r["verified"] for r in records),
                  "stores": records}
        write_json(run / "backups.json", result)
    return result
