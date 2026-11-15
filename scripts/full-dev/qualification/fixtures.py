#!/usr/bin/env python3
"""Create owned fixture bundles and atomically select their local database targets."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import uuid

from backups import ORIGINALS, SQLITE
from common import DB_NAME, REPO, STATE, digest, docker, owned_run, read_json, safe_error, sql, utc_now, write_json


FIXTURES = STATE / "runtime/fixtures"
TARGETS = STATE / "runtime/targets.env"
TOKEN = re.compile(r"[a-z0-9_]{1,38}\Z")


def cluster_identity(container: str, user: str) -> str:
    return sql(container, "postgres", user, "SELECT system_identifier::text FROM pg_control_system()")


def restore_archive(container: str, user: str, database: str, archive: Path,
                    *, timescale: bool) -> None:
    if timescale:
        sql(container, database, user,
            "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();")
    with archive.open("rb") as source:
        docker(["exec", "-i", container, "pg_restore", "-U", user, "-d", database,
                "--no-owner", "--no-acl", "--exit-on-error"], input_stream=source, timeout=3600)
    if timescale:
        sql(container, database, user, "SELECT timescaledb_post_restore();")


def verify_backups(run: Path) -> dict:
    manifest = read_json(run / "backups.json")
    if not manifest.get("passed") or len(manifest.get("stores", [])) != 4:
        raise ValueError("four verified original backups are required before fixture creation")
    for store in manifest["stores"]:
        path = (run / store["archive"]).resolve()
        if not path.is_relative_to(run.resolve()) or digest(path) != store["sha256"]:
            raise ValueError("backup archive ownership or checksum failed")
    return manifest


from target_safety import managed_publisher


@managed_publisher
def create_bundle(kind: str, backup_run: Path) -> dict:
    if kind not in ("sample", "1x", "10x"):
        raise ValueError("fixture kind must be sample, 1x or 10x")
    backup = verify_backups(backup_run)
    token = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]
    path = FIXTURES / token
    path.mkdir(parents=True, mode=0o700)
    result = {"token": token, "kind": kind, "created_at": utc_now(),
              "backup_run": str(backup_run), "ready": False, "databases": []}
    write_json(path / "manifest.json", result)
    stores = {item.get("database", "sqlite"): item for item in backup["stores"]}
    source = backup_run / stores["meshcore"]["archive"]
    source_container_started = False
    try:
        if kind != "sample":
            source_state = json.loads(docker(["inspect", "ukmesh-lookup-db"]).stdout)[0]
            if not source_state["State"]["Running"]:
                docker(["start", "ukmesh-lookup-db"])
                source_container_started = True
            for attempt in range(120):
                ready = docker(["exec", "ukmesh-lookup-db", "pg_isready", "-h", "127.0.0.1",
                                "-U", "meshcore", "-d", "postgres"], check=False)
                if ready.returncode == 0:
                    break
                import time
                time.sleep(1)
            else:
                raise RuntimeError("retained lookup source did not become ready")
            source = path / "lookup-source.dump"
            with source.open("xb") as output:
                source.chmod(0o600)
                docker(["exec", "ukmesh-lookup-db", "pg_dump", "-U", "meshcore",
                        "-d", "lookup_" + kind, "-Fc", "--no-owner", "--no-acl"],
                       output_stream=output, timeout=3600)
            result["lookup_source_sha256"] = digest(source)
        for label, container, original, user in ORIGINALS:
            prefix = ("fixture" if kind == "sample" else kind) if label == "ukmesh" else label
            name = f"lean_{prefix}_{token}"
            if not DB_NAME.fullmatch(name):
                raise ValueError("generated fixture database name rejected")
            identity = cluster_identity(container, user)
            docker(["exec", container, "createdb", "-U", user, "-T", "template0", name])
            sql(container, "postgres", user,
                f'COMMENT ON DATABASE "{name}" IS \'ukmesh-owned-fixture:{token}\'')
            record = {"label": label, "container": container, "database": name,
                      "user": user, "cluster": identity, "restored": False}
            result["databases"].append(record)
            write_json(path / "manifest.json", result)
            archive = source if label == "ukmesh" else backup_run / stores[original]["archive"]
            timescale = label == "ukmesh"
            restore_archive(container, user, name, archive, timescale=timescale)
            record["restored"] = True
            write_json(path / "manifest.json", result)
        corescope = path / "corescope"
        corescope.mkdir(mode=0o700)
        shutil.copyfile(backup_run / stores["sqlite"]["archive"], corescope / "analyzer.db")
        (corescope / "analyzer.db").chmod(0o600)
        result["corescope_data_dir"] = str(corescope)
        result["ready"] = True
        write_json(path / "manifest.json", result)
    finally:
        if source_container_started:
            docker(["stop", "ukmesh-lookup-db"], timeout=180)
    return result


def bundle(token: str) -> dict:
    if not TOKEN.fullmatch(token):
        raise ValueError("invalid fixture token")
    path = FIXTURES / token / "manifest.json"
    value = read_json(path)
    if value.get("token") != token or not value.get("ready") or len(value.get("databases", [])) != 3:
        raise ValueError("fixture bundle is incomplete")
    verify_backups(owned_run(value["backup_run"]))
    for database in value["databases"]:
        name = database["database"]
        if not DB_NAME.fullmatch(name) or not name.endswith("_" + token):
            raise ValueError("fixture database ownership failed")
        if cluster_identity(database["container"], database["user"]) != database["cluster"]:
            raise ValueError("fixture database cluster identity changed")
        comment = sql(database["container"], "postgres", database["user"],
                      "SELECT shobj_description(oid,'pg_database') FROM pg_database "
                      f"WHERE datname='{name}'")
        if comment != "ukmesh-owned-fixture:" + token:
            raise ValueError("fixture database ownership marker missing")
    corescope = Path(value["corescope_data_dir"]).resolve()
    if corescope != (FIXTURES / token / "corescope").resolve() or not (corescope / "analyzer.db").is_file():
        raise ValueError("CoreScope fixture directory ownership failed")
    return value


def write_targets(values: dict[str, str]) -> None:
    allowed = {"UKMESH_DATABASE", "OWNER_DATABASE", "BEACON_DATABASE", "CORESCOPE_DATA_DIR"}
    if set(values) != allowed:
        raise ValueError("target settings must contain exactly four allowlisted keys")
    if any("\n" in value or "\r" in value or "\0" in value for value in values.values()):
        raise ValueError("target settings must be single-line literal values")
    TARGETS.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".targets-", dir=TARGETS.parent)
    try:
        with os.fdopen(fd, "w") as output:
            for name in sorted(values):
                # environment.sh reads literal data with IFS='=', never shell code.
                output.write(name + "=" + values[name] + "\n")
            output.flush();os.fsync(output.fileno())
        os.replace(temporary, TARGETS)
        directory=os.open(TARGETS.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def selection_files():
    """Literal file state, intentionally readable during an owned torn selection."""
    parsed={}
    if TARGETS.exists():
        for line in TARGETS.read_text().splitlines():
            if not line or line.startswith('#'):continue
            key,separator,value=line.partition('=')
            if not separator or key in parsed:raise ValueError('malformed target settings')
            parsed[key]=value
    else:parsed=original_values()
    from target_safety import target_values
    target_values(parsed)
    selected=STATE/'runtime/selected-fixture.json'
    value=read_json(selected) if selected.exists() else {'targets':'original'}
    marker={'targets':value.get('targets')}
    if marker['targets']!='original':marker['token']=value.get('token')
    return {'values':parsed,'selection':marker}


def original_values():
    return {'UKMESH_DATABASE':'meshcore','OWNER_DATABASE':'meshcore_owner_auth',
            'BEACON_DATABASE':'beacon_dev','CORESCOPE_DATA_DIR':str(SQLITE.parent)}


def selection_values():
    targets=current_targets()  # The authoritative owner validates selected DBs and directory.
    state=selection_files()['values']
    return state


def publish_selection(values,result):
    from target_safety import select,write_intent
    marker={'targets':result['targets']}
    if marker['targets']!='original':marker['token']=result['token']
    def commit():
        write_targets(values)
        write_intent(STATE/'runtime/selected-fixture.json',result)
    proof=select(values,selection_values,commit,selection_reader=selection_files,requested_selection=marker)
    return {**result,'protection':proof}


def switch(token: str) -> dict:
    value=bundle(token)
    databases={item['label']:item['database'] for item in value['databases']}
    values={'UKMESH_DATABASE':databases['ukmesh'],'OWNER_DATABASE':databases['owner'],
            'BEACON_DATABASE':databases['beacon'],'CORESCOPE_DATA_DIR':value['corescope_data_dir']}
    result={'at':utc_now(),'token':token,'targets':databases,'services_restarted':False,
            'note':'run dev-all start to apply selected targets'}
    return publish_selection(values,result)


def restore_originals() -> dict:
    # Existing selected databases remain retained. The pending intent owns torn-file recovery.
    result={'at':utc_now(),'targets':'original','services_restarted':False,
            'note':'run dev-all start to apply original targets'}
    return publish_selection(original_values(),result)


def current_targets() -> dict[str, str]:
    defaults = {"ukmesh": "meshcore", "owner": "meshcore_owner_auth", "beacon": "beacon_dev"}
    selected = STATE / "runtime/selected-fixture.json"
    if not selected.exists():
        if TARGETS.exists():
            raise ValueError("target settings exist without a fixture ownership selection")
        return defaults
    value = read_json(selected)
    if value.get("targets") == "original":
        expected = defaults
        corescope = str(SQLITE.parent)
    else:
        active = bundle(value["token"])
        expected = {item["label"]: item["database"] for item in active["databases"]}
        corescope = active["corescope_data_dir"]
    parsed = {}
    for line in TARGETS.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        key, separator, raw = line.partition("=")
        if not separator or key in parsed or "\0" in raw or "\r" in raw:
            raise ValueError("fixture target settings are malformed")
        parsed[key] = raw
    settings = {"UKMESH_DATABASE": expected["ukmesh"], "OWNER_DATABASE": expected["owner"],
                "BEACON_DATABASE": expected["beacon"], "CORESCOPE_DATA_DIR": corescope}
    if parsed != settings:
        raise ValueError("active target settings differ from the owned selection")
    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "switch", "restore", "status"))
    parser.add_argument("name", nargs="?")
    parser.add_argument("--run")
    args = parser.parse_args()
    try:
        if args.action == "create":
            result = create_bundle(args.name or "sample", owned_run(args.run))
        elif args.action == "switch":
            result = switch(args.name or "")
        elif args.action == "restore":
            result = restore_originals()
        else:
            result = read_json(STATE / "runtime/selected-fixture.json")
        print(json.dumps(result))
        return 0
    except Exception as error:
        print(json.dumps({"passed": False, "error": safe_error(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
