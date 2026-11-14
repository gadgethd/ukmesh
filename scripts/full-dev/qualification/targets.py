"""Verify selected stores are the stores used by every actual consumer."""
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from common import STATE, read_json
from fixtures import SQLITE, bundle, current_targets


def database_setting(key: str) -> bool:
    return 'DATABASE_URL' in key or key.endswith('_DSN') or key=='DATA_SOURCE_NAME'


def validate_consumers(containers: dict, targets: dict, data_directory: Path) -> list[dict]:
    checks = []
    core_mounts = []
    for name, item in containers.items():
        if not name.startswith(("ukmesh-full-", "ukmesh-companions-")):
            continue
        environment = dict(value.split("=", 1) for value in item["Config"].get("Env", []))
        required = {
            "ukmesh-full-backend-1": {"DATABASE_URL", "MESHCORE_HTTP_DATABASE_URL", "MESHCORE_INGEST_DATABASE_URL", "MESHCORE_WORKER_DATABASE_URL", "OWNER_DATABASE_URL"},
            "ukmesh-full-db-migrate-1": {"DATABASE_URL", "MESHCORE_MIGRATE_DATABASE_URL"},
            "ukmesh-full-postgres-exporter-1": {"DATA_SOURCE_NAME"},
            "ukmesh-companions-beacon-1": {"POSTGRES_DSN", "BEACON_MAINTENANCE_DSN"},
            "ukmesh-companions-beacon-migrate-1": {"POSTGRES_DSN"},
        }.get(name, {"DATABASE_URL", "MESHCORE_WORKER_DATABASE_URL"}
              if name.endswith("-worker-1") or name == "ukmesh-full-synthetic-monitor-1" else set())
        if any(not environment.get(key) for key in required):
            raise ValueError(f"{name} lacks an explicit required database session target")
        for key, value in environment.items():
            if not value or not database_setting(key):
                continue
            parsed = urlsplit(value)
            if parsed.scheme not in ("postgres", "postgresql"):
                raise ValueError(f"{name} {key} is not a PostgreSQL session target")
            target = "beacon" if name.startswith("ukmesh-companions-beacon") else ("owner" if key == "OWNER_DATABASE_URL" else "ukmesh")
            hosts = {"beacon-db", "ukmesh-companions-beacon-db-1"} if target == "beacon" else {"ukmesh-sample-db"}
            if name == "ukmesh-companions-beacon-migrate-1":
                role = "beacon_dev"
            elif target == "beacon":
                role = "beacon_maint" if key == "BEACON_MAINTENANCE_DSN" else "beacon_runtime"
            elif target == "owner":
                role = "meshcore_owner"
            elif name == "ukmesh-full-db-migrate-1":
                role = "meshcore"
            elif name == "ukmesh-full-postgres-exporter-1":
                role = "meshcore_monitor"
            elif name == "ukmesh-full-backend-1":
                role = {"MESHCORE_INGEST_DATABASE_URL": "meshcore_ingest", "MESHCORE_WORKER_DATABASE_URL": "meshcore_worker"}.get(key, "meshcore_api")
            else:
                role = "meshcore_worker"
            options=parse_qs(parsed.query,keep_blank_values=True)
            if options not in ({},{'sslmode':['disable']}) or parsed.hostname not in hosts or parsed.port != 5432 or unquote(parsed.path) != "/" + targets[target] \
                    or unquote(parsed.username or "") != role or parsed.fragment:
                raise ValueError(f"{name} {key} differs from its selected store or intended session role")
            checks.append({"consumer": name, "setting": key, "target": target, "role": role})
        if name in ("ukmesh-companions-corescope-1", "ukmesh-companions-corescope-ingestor-1"):
            mounts = [Path(mount["Source"]).resolve() for mount in item["Mounts"]
                      if mount["Type"] == "bind" and mount["Destination"] == "/data"]
            if mounts != [data_directory.resolve()]:
                raise ValueError(f"{name} does not mount the selected CoreScope store")
            core_mounts.append(name)
    if len(core_mounts) != 2:
        raise ValueError("both CoreScope consumers must use the selected data directory")
    if not checks:
        raise ValueError("runtime has no PostgreSQL consumer target evidence")
    return checks


def runtime_targets(containers: dict, *, allow_public_variant: bool = False) -> list[dict]:
    targets = current_targets()
    selected_path = STATE / "runtime/selected-fixture.json"
    selected = read_json(selected_path) if selected_path.exists() else {"targets": "original"}
    directory = SQLITE.parent
    if selected.get("targets") != "original":
        fixture = bundle(selected["token"])
        directory = Path(fixture["corescope_data_dir"])
        mode_path = STATE / "runtime/corescope-mode.json"
        mode = read_json(mode_path) if mode_path.exists() else {}
        if mode.get("mode") == "public":
            if not allow_public_variant or mode.get("fixture_token") != fixture["token"]:
                raise ValueError("this workload requires the selected default MQTT data ownership")
            expected = directory.parent / "corescope-public"
            if Path(mode.get("data_dir", "")).resolve() != expected.resolve():
                raise ValueError("CoreScope public variant directory differs from its owned fixture")
            directory = expected
    return validate_consumers(containers, targets, directory)
