"""Qualification capabilities and transient application-role environments.

Credentials come from the selected running consumer, never from a second
password definition. This module performs no IO and never mutates its inputs.
"""
from urllib.parse import unquote, urlsplit, urlunsplit

from consumer_refs import describe_consumer

READ_TOOLS = frozenset({"benchmarkLookups", "benchmarkMatchedDemand", "benchmarkOperations"})
ADMIN_TOOLS = frozenset({"generateSampleDataset", "loadSampleDataset", "prepareSampleDataset",
    "soakDatabaseLookups", "checkLookupParity", "checkPathContext", "fullStackTraffic", "dataLifecycle"})
URL_KEYS = ("DATABASE_URL", "MESHCORE_HTTP_DATABASE_URL", "MESHCORE_WORKER_DATABASE_URL",
            "MESHCORE_INGEST_DATABASE_URL", "OWNER_DATABASE_URL")
PROFILE_KEYS = ("DATABASE_POOL_MAX", "DATABASE_STATEMENT_TIMEOUT_MS", "DATABASE_CONNECTION_TIMEOUT_MS",
    "DATABASE_APPLICATION_NAME", "OWNER_DATABASE_POOL_MAX", "OWNER_DATABASE_STATEMENT_TIMEOUT_MS",
    "OWNER_DATABASE_APPLICATION_NAME")


def tool_category(tool):
    if tool in READ_TOOLS:
        return "application-read"
    if tool in ADMIN_TOOLS:
        return "administrative-fixture-oracle"
    raise ValueError("unknown qualification tool capability")


def selected_stores(fixture, database):
    rows = fixture.get("databases", [])
    if fixture.get("ready") is not True or fixture.get("kind") not in ("sample", "1x", "10x") \
            or len(rows) != 3 or {r.get("label") for r in rows} != {"ukmesh", "owner", "beacon"}:
        raise ValueError("complete owned fixture required")
    stores = {r["label"]: r for r in rows}
    if stores["ukmesh"]["database"] != database:
        raise ValueError("tool does not target the selected owned main store")
    if stores["ukmesh"]["container"] != "ukmesh-sample-db" \
            or stores["owner"]["container"] != "ukmesh-sample-db":
        raise ValueError("unexpected selected database owner")
    return stores


def environment_values(item):
    values = {}
    for value in item.get("Config", {}).get("Env", []):
        key, separator, setting = value.partition("=")
        if not separator or key in values:
            raise ValueError("ambiguous service environment")
        values[key] = setting
    return values


def loopback_port(item):
    if item.get("Name") != "/ukmesh-sample-db" or item.get("State", {}).get("Running") is not True \
            or item.get("Config", {}).get("Labels", {}).get("com.docker.compose.project") != "ukmesh-sample":
        raise ValueError("actual database container identity differs")
    ports = item.get("NetworkSettings", {}).get("Ports", {}).get("5432/tcp", [])
    if len(ports) != 1 or ports[0].get("HostIp") != "127.0.0.1":
        raise ValueError("one actual loopback database port required")
    value = ports[0].get("HostPort", "")
    if not value.isdecimal() or not 0 < int(value) < 65536:
        raise ValueError("actual loopback database port is invalid")
    return value


def local_endpoint(value, port):
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in ("postgres", "postgresql") or parsed.hostname != "ukmesh-sample-db" \
                or parsed.port != 5432 or parsed.fragment or not parsed.username:
            raise ValueError()
        # Preserve encoded credentials and query options; only the already
        # validated service endpoint changes from bridge to published loopback.
        credentials = parsed.netloc.rsplit("@", 1)[0]
        return urlunsplit(parsed._replace(netloc=credentials + "@127.0.0.1:" + port))
    except (ValueError, TypeError):
        raise ValueError("invalid selected service database endpoint") from None


def read_environment(fixture, database, containers):
    stores = selected_stores(fixture, database)
    if fixture["kind"] not in ("1x", "10x"):
        raise ValueError("read qualification requires a retained lookup fixture")
    backend = containers.get("ukmesh-full-backend-1", {})
    if backend.get("Name") != "/ukmesh-full-backend-1" \
            or backend.get("Config", {}).get("Labels", {}).get("com.docker.compose.service") != "backend" \
            or backend.get("State", {}).get("Health", {}).get("Status") != "healthy":
        raise ValueError("actual healthy backend consumer required")
    try:
        consumer = describe_consumer("ukmesh-full-backend-1", backend, identity=True)
    except (ValueError, TypeError, KeyError):
        raise ValueError("actual backend role or endpoint differs") from None
    if consumer["running"] is not True or consumer["refs"] != {
            "ukmesh": stores["ukmesh"]["database"], "owner": stores["owner"]["database"]}:
        raise ValueError("actual backend does not use the selected stores")
    actual = environment_values(backend)
    if actual.get("DATABASE_ROLE_ENFORCEMENT", "").strip().lower() not in ("true", "1", "yes", "on"):
        raise ValueError("actual application role enforcement must be enabled")
    try:
        if int(actual["DATABASE_POOL_MAX"]) != 8 or int(actual["OWNER_DATABASE_POOL_MAX"]) != 2:
            raise ValueError()
    except (KeyError, ValueError):
        raise ValueError("actual qualification HTTP/owner pool bounds differ") from None
    port = loopback_port(containers.get("ukmesh-sample-db", {}))
    try:
        values = {key: local_endpoint(actual[key], port) for key in URL_KEYS}
    except KeyError:
        raise ValueError("actual backend explicit role bundle is incomplete") from None
    values.update({key: actual[key] for key in PROFILE_KEYS if key in actual})
    values.update(DATABASE_ROLE_ENFORCEMENT="true", PUBLIC_FEATURE_REGION_SCOPING_ENABLED="true")
    return values


def evidence(environment, category, database):
    """A configuration receipt, explicitly not proof of authenticated sessions."""
    roles = {}
    for key in URL_KEYS:
        if key in environment:
            parsed = urlsplit(environment[key])
            roles[key] = {"role": unquote(parsed.username or ""),
                          "database": unquote(parsed.path).removeprefix("/")}
    return {"version": 1, "category": category, "database": database,
            "scope": "configuration-only; native session identity still required",
            "roles": roles, "pool_settings": {key: environment[key] for key in PROFILE_KEYS if key in environment}}


def tool_environment(tool, database, fixture, *, containers=None, admin_endpoint=None):
    category = tool_category(tool)
    selected_stores(fixture, database)
    if category == "application-read":
        if admin_endpoint is not None:
            raise ValueError("administrator capability cannot be supplied to a read benchmark")
        values = read_environment(fixture, database, containers or {})
    else:
        try:
            parsed = urlsplit(admin_endpoint or "")
            if parsed.scheme not in ("postgres", "postgresql") or parsed.hostname != "127.0.0.1" \
                    or unquote(parsed.path).removeprefix("/") != database or parsed.fragment:
                raise ValueError()
        except ValueError:
            raise ValueError("explicit owned administrative endpoint required") from None
        values = {"DATABASE_URL": admin_endpoint, "DATABASE_POOL_MAX": "8",
                  "DATABASE_STATEMENT_TIMEOUT_MS": "30000", "PUBLIC_FEATURE_REGION_SCOPING_ENABLED": "true"}
    return values, evidence(values, category, database)
