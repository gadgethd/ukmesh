"""Authenticated permission probes against an explicitly owned fixture bundle."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from common import STATE, docker, read_json, sql, utc_now, write_json
from context import phase_context
from fixtures import bundle, current_targets


FIXTURE_PASSWORD = "local-full-dev-only"


def application_sessions(stores: dict) -> list[dict]:
    main,owner,beacon=(stores[key] for key in ('ukmesh','owner','beacon'))
    sessions=json.loads(sql(main['container'],main['database'],main['user'],
        "SELECT coalesce(json_agg(json_build_object('database',datname,'role',usename,'application',application_name)),'[]') "
        "FROM pg_stat_activity WHERE backend_type='client backend' AND datname IN ('"+main['database']+"','"+owner['database']+"') "
        "AND (application_name LIKE 'meshcore-%' OR application_name='ukmesh-tagger-worker')"))
    checks=[]
    for row in sessions:
        application=row['application']
        role={'meshcore-backend':'meshcore_api','meshcore-backend-ingest':'meshcore_ingest',
              'meshcore-backend-owner-auth':'meshcore_owner','meshcore-owner-auth':'meshcore_owner'}.get(application,'meshcore_worker')
        database=owner['database'] if role=='meshcore_owner' else main['database']
        checks.append({'description':'actual application session '+application,'role':row['role'],
                       'passed':row['role']==role and row['database']==database})
    observed={row['role'] for row in sessions}
    checks.append({'description':'actual API, owner and worker sessions observed',
                   'passed':{'meshcore_api','meshcore_owner','meshcore_worker'}.issubset(observed)})
    beacon_roles=json.loads(sql(beacon['container'],beacon['database'],beacon['user'],
        "SELECT coalesce(json_agg(DISTINCT usename),'[]') FROM pg_stat_activity WHERE backend_type='client backend' "
        "AND datname=current_database() AND pid<>pg_backend_pid()"))
    checks.append({'description':'actual Beacon runtime and maintenance sessions observed','roles':beacon_roles,
                   'passed':{'beacon_runtime','beacon_maint'}.issubset(beacon_roles) and 'beacon_dev' not in beacon_roles})
    return checks


def session(container: str, database: str, role: str, statement: str,
            *, host: str, password: str = FIXTURE_PASSWORD, capture_metadata: bool = False) -> dict:
    result = docker(["exec", "-i", "-e", "PGPASSWORD=" + password, container,
                     "psql", "-X", "-q", "-A", "-t", "-h", host, "-U", role,
                     "-d", database, "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate"],
                    data=("SET statement_timeout='10s'; BEGIN; " + statement + "; ROLLBACK;\n").encode(),
                    timeout=15, check=False)
    states = re.findall(rb"(?:ERROR|FATAL):\s+([0-9A-Z]{5})\b", result.stderr)
    evidence = {"exit_code": result.returncode,
            "authentication_rejected": result.returncode != 0 and b"password authentication failed" in result.stderr,
            "database_access_denied": result.returncode != 0 and b"permission denied for database" in result.stderr,
            "sqlstate": next((state.decode() for state in states if state in (b"42501", b"28P01", b"28000")),
                             states[0].decode() if states else None),
            "query_sha256": hashlib.sha256(statement.encode()).hexdigest()}
    if capture_metadata:
        try:
            evidence['metadata'] = json.loads(result.stdout)
        except (ValueError, UnicodeDecodeError):
            evidence['metadata'] = None
            evidence['metadata_error'] = 'authenticated permission metadata missing or malformed'
    return evidence


def reader_metadata_valid(metadata: object, *, role: str, database: str, schema: str) -> bool:
    if not isinstance(metadata, dict) or any(metadata.get(k) != v for k, v in
        {'current_user': role, 'session_user': role, 'database': database}.items()):
        return False
    relations = metadata.get('relations')
    if not isinstance(relations, list) or len(relations) != 2:
        return False
    seen = {}
    for relation in relations:
        if not isinstance(relation, dict):
            return False
        oid = relation.get('oid')
        if type(oid) is not int or not 0 < oid < 2**32:
            return False
        if not isinstance(relation.get('schema'), str) or not isinstance(relation.get('name'), str):
            return False
        key = (relation['schema'], relation['name'])
        expected_kinds = {(schema, 'packets'): ('v',), ('mesh', 'packets'): ('r', 'p')}
        if key in seen or relation.get('relkind') not in expected_kinds.get(key, ()):
            return False
        if relation.get('table_insert') is not False or relation.get('any_column_insert') is not False:
            return False
        seen[key] = oid
    return set(seen) == {(schema, 'packets'), ('mesh', 'packets')} and len(set(seen.values())) == 2


def reader_write_checks(container: str, database: str, role: str, schema: str, *, host: str) -> list[dict]:
    # A whole-row INSERT targets the derived payload column and can fail before
    # PostgreSQL checks privileges. DEFAULT VALUES reaches the permission gate.
    statement = f"""SELECT json_build_object('current_user',current_user,'session_user',session_user,
      'database',current_database(),'relations',(
        SELECT json_agg(json_build_object('oid',c.oid::bigint,'schema',n.nspname,'name',c.relname,
          'relkind',c.relkind,'table_insert',has_table_privilege(current_user,c.oid,'INSERT'),
          'any_column_insert',has_any_column_privilege(current_user,c.oid,'INSERT')))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE (n.nspname,c.relname) IN (('{schema}','packets'),('mesh','packets'))));
      INSERT INTO {schema}.packets DEFAULT VALUES"""
    evidence = session(container, database, role, statement, host=host, capture_metadata=True)
    valid = reader_metadata_valid(evidence.get('metadata'), role=role, database=database, schema=schema)
    return [{'description': 'reader has no effective public or raw packet INSERT privileges',
             'role': role, 'passed': valid, 'metadata': evidence.get('metadata'),
             'query_sha256': evidence['query_sha256']},
            {'description': 'reader cannot insert public packets', 'role': role, 'expected_allowed': False,
             'passed': valid and evidence['exit_code'] != 0 and evidence['sqlstate'] == '42501', **evidence}]


def access(run: Path, *, reader_role: str = "corescope_ro",
           reader_schema: str = "corescope_public") -> dict:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", reader_role) or not re.fullmatch(r"[a-z_][a-z0-9_]*", reader_schema):
        raise ValueError("reader role/schema identifiers are invalid")
    selected = read_json(STATE / "runtime/selected-fixture.json")
    fixture = bundle(selected["token"])
    current_targets()
    from probes import inspected_containers
    from targets import runtime_targets
    runtime_targets(inspected_containers())
    stores = {record["label"]: record for record in fixture["databases"]}
    hosts = {}
    for store in stores.values():
        if store["container"] not in hosts:
            inspected = json.loads(docker(["inspect", store["container"]]).stdout)[0]
            addresses = [network["IPAddress"] for network in inspected["NetworkSettings"]["Networks"].values()
                         if network.get("IPAddress")]
            if not addresses:
                raise RuntimeError("database container has no bridge address for authenticated probes")
            hosts[store["container"]] = addresses[0]
    main = stores["ukmesh"]
    owner = stores["owner"]
    beacon = stores["beacon"]
    specs = [
        ("api reads nodes", "meshcore_api", main, "SELECT node_id FROM core.nodes LIMIT 0", True),
        ("api reads packets", "meshcore_api", main, "SELECT packet_hash FROM mesh.packets LIMIT 0", True),
        ("api cannot insert packets", "meshcore_api", main,
         "INSERT INTO mesh.packets SELECT * FROM mesh.packets WHERE false", False),
        ("api cannot read raw privacy policy", "meshcore_api", main, "SELECT * FROM ops.node_privacy LIMIT 0", False),
        ("api cannot create core tables", "meshcore_api", main,
         "CREATE TABLE core.qualification_forbidden_ddl(id integer)", False),
        ("ingest can insert packets", "meshcore_ingest", main,
         "INSERT INTO mesh.packets SELECT * FROM mesh.packets WHERE false", True),
        ("ingest cannot create core tables", "meshcore_ingest", main,
         "CREATE TABLE core.qualification_forbidden_ddl(id integer)", False),
        ("worker can write health snapshots", "meshcore_worker", main,
         "INSERT INTO ops.worker_health_snapshots SELECT * FROM ops.worker_health_snapshots WHERE false", True),
        ("worker cannot create core tables", "meshcore_worker", main,
         "CREATE TABLE core.qualification_forbidden_ddl(id integer)", False),
        ("owner can read accounts", "meshcore_owner", owner, "SELECT * FROM core.owner_accounts LIMIT 0", True),
        ("owner can insert accounts", "meshcore_owner", owner,
         "INSERT INTO core.owner_accounts SELECT * FROM core.owner_accounts WHERE false", True),
        ("owner cannot create account tables", "meshcore_owner", owner,
         "CREATE TABLE core.qualification_forbidden_ddl(id integer)", False),
        ("owner cannot connect measurement DB", "meshcore_owner", main, "SELECT 1", False),
        ("api cannot connect owner DB", "meshcore_api", owner, "SELECT 1", False),
        ("migrator can create core tables", "meshcore_migrate", main,
         "CREATE TABLE core.qualification_allowed_ddl(id integer)", True),
        ("reader can read public packets", reader_role, main,
         f"SELECT * FROM {reader_schema}.packets LIMIT 0", True),
        ("reader cannot read raw packets", reader_role, main, "SELECT * FROM mesh.packets LIMIT 0", False),
        ("reader cannot read raw privacy policy", reader_role, main, "SELECT * FROM ops.node_privacy LIMIT 0", False),
        ("worker can create temporary staging tables", "meshcore_worker", main,
         "CREATE TEMP TABLE qualification_allowed_stage(id integer)", True),
        ("Beacon runtime can read packets", "beacon_runtime", beacon, "SELECT * FROM public.packets LIMIT 0", True),
        ("Beacon runtime can insert observations", "beacon_runtime", beacon,
         "INSERT INTO public.packet_observations SELECT * FROM public.packet_observations WHERE false", True),
        ("Beacon runtime cannot create persistent tables", "beacon_runtime", beacon,
         "CREATE TABLE public.qualification_forbidden_ddl(id integer)", False),
        ("Beacon runtime cannot create temporary tables", "beacon_runtime", beacon,
         "CREATE TEMP TABLE qualification_forbidden_stage(id integer)", False),
        ("Beacon runtime cannot alter maintenance tables", "beacon_runtime", beacon,
         "ALTER TABLE public.packet_sites ADD COLUMN qualification_denied integer", False),
        ("Beacon maint can read packets", "beacon_maint", beacon, "SELECT * FROM public.packets LIMIT 0", True),
        ("Beacon maint can create temporary staging tables", "beacon_maint", beacon,
         "CREATE TEMP TABLE qualification_allowed_stage(id integer)", True),
        ("Beacon maint cannot create persistent tables", "beacon_maint", beacon,
         "CREATE TABLE public.qualification_forbidden_ddl(id integer)", False),
    ]
    for role in ("meshcore_api", "meshcore_ingest", "meshcore_worker", "meshcore_owner", reader_role):
        specs.append((role + " cannot assume administrator", role,
                      owner if role == "meshcore_owner" else main, "SET ROLE meshcore", False))
        if role != "meshcore_worker":
            specs.append((role + " cannot create temporary tables", role,
                          owner if role == "meshcore_owner" else main,
                          "CREATE TEMP TABLE qualification_forbidden_stage(id integer)", False))
    for role in ("beacon_runtime", "beacon_maint"):
        specs.append((role + " cannot assume administrator", role, beacon, "SET ROLE beacon_dev", False))
    records = []
    for role, store in [(role, owner if role == "meshcore_owner" else main)
                        for role in ("meshcore_api", "meshcore_ingest", "meshcore_worker", "meshcore_owner",
                                     "meshcore_migrate", reader_role)] + [(role, beacon) for role in ("beacon_runtime", "beacon_maint")]:
        evidence = session(store["container"], store["database"], role, "SELECT 1",
                           host=hosts[store["container"]], password="deliberately-wrong-qualification-password")
        records.append({"description": role + " rejects wrong password", "role": role,
                        "passed": evidence["authentication_rejected"], **evidence})
    for description, role, store, statement, allowed in specs:
        evidence = session(store["container"], store["database"], role, statement,
                           host=hosts[store["container"]])
        passed = evidence["exit_code"] == 0 if allowed else (
            evidence["sqlstate"] == "42501" or evidence["database_access_denied"])
        records.append({"description": description, "role": role, "expected_allowed": allowed,
                        "passed": passed, **evidence})
    records += reader_write_checks(main['container'], main['database'], reader_role, reader_schema,
                                   host=hosts[main['container']])
    records+=application_sessions(stores)
    result = {"at": utc_now(), "fixture_token": fixture["token"], "passed": all(row["passed"] for row in records),
              "authentication": "TCP password authentication; statements roll back", "checks": records,
              "context": phase_context(run)}
    write_json(run / "access.json", result)
    return result
