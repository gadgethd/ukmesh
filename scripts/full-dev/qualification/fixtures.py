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


def fixture_maintenance_phases(records):
    """Distribute owned internal cleanup across its unchanged six-hour cycle."""
    ordered = sorted(records, key=lambda row: row['database'])
    if len({row['database'] for row in ordered}) != len(ordered):
        raise ValueError('duplicate fixture maintenance database')
    for row in ordered:
        token = row.get('token', '')
        prefixes = ('fixture', '1x', '10x') if row.get('label') == 'ukmesh' else ('owner',) if row.get('label') == 'owner' else ()
        if not TOKEN.fullmatch(token) or not DB_NAME.fullmatch(row['database']) \
                or row['database'] not in {f'lean_{prefix}_{token}' for prefix in prefixes}:
            raise ValueError('maintenance requires an owned fixture main/owner database')
    phases = [(index + 1) * 21600 // (len(ordered) + 1) for index in range(len(ordered))]
    if any(not 0 < phase < 21600 for phase in phases) or len(set(phases)) != len(phases):
        raise ValueError('fixture maintenance phases exhausted')
    return list(zip(ordered, phases))


def reschedule_fixture_maintenance(record, phase_seconds):
    """Change only verified fixture job timing; never original stores or policy."""
    from target_lease import target_lease
    from target_safety import publication_lease
    fixture_maintenance_phases([record])
    if type(phase_seconds) is not int or not 0 < phase_seconds < 21600:
        raise ValueError('a nonzero six-hour fixture phase is required')
    with target_lease(exclusive=True), publication_lease():
        inspect = """SELECT json_build_object(
          'database',current_database(), 'cluster',(SELECT system_identifier::text FROM pg_control_system()),
          'marker',shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database'),
          'job',(SELECT to_jsonb(j) FROM timescaledb_information.jobs j WHERE job_id=3),
          'next_start',(SELECT next_start::text FROM timescaledb_information.job_stats WHERE job_id=3),
          'phase_seconds',(SELECT mod(extract(epoch FROM initial_start-TIMESTAMPTZ '2000-01-01T00:00:00Z'),21600)::int
            FROM timescaledb_information.jobs WHERE job_id=3),
          'next_aligned',(SELECT s.next_start>NOW() AND date_bin(INTERVAL '6 hours',s.next_start,j.initial_start)=s.next_start
            FROM timescaledb_information.jobs j JOIN timescaledb_information.job_stats s USING(job_id) WHERE j.job_id=3))"""
        def execute(query):
            return sql(record['container'], record['database'], record['user'], query, timeout=15)
        before = json.loads(execute(inspect))
        job = before.get('job')
        if before.get('database') != record['database'] or before.get('cluster') != record['cluster'] \
                or before.get('marker') != 'ukmesh-owned-fixture:' + record['token']:
            raise ValueError('fixture maintenance store identity differs')
        if not isinstance(job, dict) or any(job.get(key) != value for key, value in {
                'job_id': 3, 'proc_schema': '_timescaledb_functions',
                'proc_name': 'policy_job_stat_history_retention', 'scheduled': True,
                'fixed_schedule': True, 'schedule_interval': '06:00:00', 'max_runtime': '01:00:00'}.items()):
            raise ValueError('fixture internal history policy differs')
        if before.get('phase_seconds') == phase_seconds and before.get('next_aligned') is True \
                and before.get('next_start') not in ('infinity', '-infinity', None):
            return {'database': record['database'], 'phase_seconds': phase_seconds, 'changed': False,
                    'before': before, 'after': before}
        quote = lambda value: "'" + value.replace("'", "''") + "'"
        expected = quote(json.dumps(job))
        marker = quote('ukmesh-owned-fixture:' + record['token'])
        name, cluster = quote(record['database']), quote(record['cluster'])
        mutation = f"""BEGIN; SET LOCAL statement_timeout='5s';
        DO $fixture_maintenance$
        DECLARE old jsonb; fresh jsonb;
          anchor timestamptz := TIMESTAMPTZ '2000-01-01T00:00:00Z' + make_interval(secs=>{phase_seconds});
        BEGIN
          IF current_database()<>{name} OR (SELECT system_identifier::text FROM pg_control_system())<>{cluster}
            OR shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database') IS DISTINCT FROM {marker}
            THEN RAISE EXCEPTION 'fixture maintenance store identity changed'; END IF;
          SELECT to_jsonb(j) INTO old FROM timescaledb_information.jobs j WHERE job_id=3;
          IF old IS DISTINCT FROM {expected}::jsonb THEN RAISE EXCEPTION 'fixture history job changed'; END IF;
          PERFORM alter_job(3, initial_start=>anchor,
            next_start=>date_bin(INTERVAL '6 hours',NOW(),anchor)+INTERVAL '6 hours');
          SELECT to_jsonb(j) INTO fresh FROM timescaledb_information.jobs j WHERE job_id=3;
          IF (old-'initial_start'-'next_start') IS DISTINCT FROM (fresh-'initial_start'-'next_start')
            OR NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs j
              JOIN timescaledb_information.job_stats s USING(job_id) WHERE j.job_id=3
              AND j.initial_start=anchor AND s.next_start>NOW()
              AND date_bin(INTERVAL '6 hours',s.next_start,anchor)=s.next_start)
            THEN RAISE EXCEPTION 'fixture history policy preservation failed'; END IF;
        END; $fixture_maintenance$;
        {inspect}; COMMIT;"""
        after = json.loads(execute(mutation))
        protected = lambda value: {key: item for key, item in value.items() if key not in ('initial_start', 'next_start')}
        if protected(after['job']) != protected(job) or after['phase_seconds'] != phase_seconds \
                or after['next_aligned'] is not True:
            raise ValueError('fixture history timing readback differs')
        return {'database': record['database'], 'phase_seconds': phase_seconds, 'changed': True,
                'before': before, 'after': after}


def stagger_fixture_maintenance():
    from target_lease import target_lease
    from target_safety import publication_lease
    with target_lease(exclusive=True), publication_lease():
        records = []
        for path in sorted(FIXTURES.glob('*/manifest.json')):
            manifest = read_json(path)
            if manifest.get('ready') is not True:
                continue
            owned = bundle(manifest['token'])
            records.extend(dict(row, token=owned['token']) for row in owned['databases']
                           if row['label'] in ('ukmesh', 'owner'))
        jobs = [reschedule_fixture_maintenance(row, phase) for row, phase in fixture_maintenance_phases(records)]
        return {'passed': True, 'jobs': jobs, 'original_databases_touched': False, 'interval_seconds': 21600}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "switch", "restore", "status", "stagger-maintenance"))
    parser.add_argument("name", nargs="?")
    parser.add_argument("--run")
    args = parser.parse_args()
    try:
        if args.action == "create":
            result = create_bundle(args.name or "sample", owned_run(args.run))
            result['fixture_maintenance'] = stagger_fixture_maintenance()
        elif args.action == "switch":
            result = switch(args.name or "")
        elif args.action == "restore":
            result = restore_originals()
        elif args.action == "stagger-maintenance":
            result = stagger_fixture_maintenance()
        else:
            result = read_json(STATE / "runtime/selected-fixture.json")
        print(json.dumps(result))
        return 0
    except Exception as error:
        print(json.dumps({"passed": False, "error": safe_error(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
