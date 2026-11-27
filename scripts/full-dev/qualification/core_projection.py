"""Close the two actual Core public-source proofs against their exact app scope."""

from __future__ import annotations

import json
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit

from common import REPO, STATE, digest, docker, read_json, safe_error, utc_now, write_json
from context import corescope_application_context, phase_context
from fixtures import bundle


PG_NAMES = {"public-postgresql-backfill-and-follow", "cursor-restart-and-observation-deduplication",
            "stale-authority-fails-closed", "privacy-revocation-prunes-copied-and-derived-data",
            "public-restoration-replays-once"}
RAM_NAMES = {"public-http-tail-before-postgresql-commit", "private-only-page-advances-cursor",
             "postgresql-handoff-deduplicates-and-excludes-private"}
CORE_FIELDS = {"dbPath", "mqttSources", "postgresSources", "redisSources", "publicAuthorityURL",
               "publicAuthorityToken", "publicProjection"}
RAM_FIELDS = {"MQTT_INGEST_MODE", "MQTT_RAM_STREAM_KEY", "MQTT_RAM_CONSUMER_GROUP", "MQTT_RAM_CONSUMER_NAME"}


def validate_receipt(value: dict, *, ram: bool, application: dict, run: Path) -> None:
    if value.get("passed") is not True or value.get("application") != application:
        raise ValueError("Core public-source proof failed or its application code/policy changed")
    checks = value.get("checks", [])
    if not checks or any(check.get("passed") is not True or check.get("skipped") for check in checks):
        raise ValueError("Core native checks are failed or incomplete")
    named = {check["name"]: check for check in checks}
    if len(named) != len(checks) or not (RAM_NAMES if ram else PG_NAMES).issubset(named):
        raise ValueError("required Core native behaviors were not proved")
    variant = value.get("variant", {})
    if variant.get("mode") != ("public-ram" if ram else "public-postgresql"):
        raise ValueError("Core proof variant is missing or incorrect")
    token = value.get("fixture_token")
    if not token or variant.get("fixture_token") != token:
        raise ValueError("Core proof fixture identities differ")
    fixture = bundle(token)
    if fixture["kind"] != "sample":
        raise ValueError("privacy fault proof requires its dedicated small owned fixture")
    directory = Path(variant.get("projection_directory", "")).resolve()
    if directory != (STATE / "runtime/fixtures" / token / "corescope-public").resolve():
        raise ValueError("Core projection storage is outside its owned fixture")
    base = read_json(REPO / "scripts/companion-dev/corescope.json")
    public = read_json(directory / "config.json")
    actual = {key for key in base.keys() | public.keys() if base.get(key) != public.get(key)}
    if not actual.issubset(CORE_FIELDS) or set(variant.get("core_changed_fields", [])) != actual:
        raise ValueError("Core variant changed undeclared configuration")
    if set(variant.get("backend_changed_fields", [])) != (RAM_FIELDS if ram else set()):
        raise ValueError("Core variant changed undeclared backend settings")
    database = next(item["database"] for item in fixture["databases"] if item["label"] == "ukmesh")
    sources = public.get("postgresSources", [])
    live = public.get("redisSources", [])
    if not public.get("publicProjection") or public.get("mqttSources") or len(sources) != 1 or len(live) != 1:
        raise ValueError("Core public projection mixed source ownership")
    parsed = urlsplit(sources[0].get("dsn", ""))
    if parsed.hostname != "ukmesh-sample-db" or parsed.port != 5432 or parsed.username != "corescope_ro" \
            or parsed.path != "/" + database or parse_qs(parsed.query) != {"sslmode": ["disable"]}:
        raise ValueError("Core PostgreSQL source is outside its restricted local fixture")
    authority = "http://ukmesh-full-backend-1:3000/internal/ingest/public"
    if public.get("publicAuthorityURL") != authority + "/policy" or live[0].get("publicURL") != authority + "/entries" \
            or live[0].get("url") or live[0].get("privacyDsn") or sources[0].get("requirePublicVisibility") is False \
            or live[0].get("requirePublicVisibility") is False:
        raise ValueError("Core source bypassed its filtered public authority")
    if not value.get("restored_mqtt"):
        raise ValueError("Core measurement ownership was not restored to MQTT")
    attempt = Path(value.get("attempt", "")).resolve()
    if not attempt.is_relative_to(run.resolve()):
        raise ValueError("Core proof attempt is outside this evidence run")
    if ram:
        if not all(value.get(key) is True for key in ("restored_durable", "restored_redis_settings", "final_stream_drained")) \
                or value.get("cleanup_errors"):
            raise ValueError("RAM proof did not safely drain and restore normal settings")
        settings = value.get("ram_settings", {})
        stream = r"meshcore:ingest:lean:" + re.escape(token) + r":corescope:[a-f0-9]{12}"
        if not re.fullmatch(stream, settings.get("stream_key", "")) or settings.get("redis_maxmemory") != 134217728 \
                or settings.get("redis_policy") != "noeviction" or settings.get("redis_appendonly") != "yes" \
                or settings.get("redis_appendfsync") != "always":
            raise ValueError("RAM proof lacked its bounded, durable owned stream")
        before = named["public-http-tail-before-postgresql-commit"]
        private = named["private-only-page-advances-cursor"]
        handoff = named["postgresql-handoff-deduplicates-and-excludes-private"]
        if before.get("publicCopies") != 80 or before.get("sqlRows") != 0 \
                or private.get("publicEntries") != 80 or private.get("privateEntries") != 20 or private.get("cursorAdvanced") is not True \
                or handoff.get("committed") != 100 or handoff.get("publicCopies") != 80 \
                or handoff.get("privateCopies") != 0 or handoff.get("duplicateIdentities") != 0:
            raise ValueError("RAM public/private counts, cursor or handoff evidence differs")
    else:
        for name in ("public-postgresql-backfill-and-follow", "public-restoration-replays-once"):
            if any(named[name].get(key) != expected for key, expected in
                   (("journal", 2), ("observations", 2), ("duplicateIdentities", 0))):
                raise ValueError("Core PostgreSQL copy or restoration counts differ")
        revoked = named["privacy-revocation-prunes-copied-and-derived-data"]
        if any(revoked.get(key) != 0 for key in ("journal", "observations", "duplicateIdentities")):
            raise ValueError("revoked Core data remains copied or derived")
        cursor = named["cursor-restart-and-observation-deduplication"]
        if int(cursor.get("lastSeqBefore", 0)) <= 0 or int(cursor.get("lastSeqAfter", 0)) < int(cursor["lastSeqBefore"]):
            raise ValueError("Core PostgreSQL cursor continuity was not proved")


def core_projection(run: Path) -> dict:
    application = {}
    checks = []
    receipts = {}
    ram_receipt = None
    for ram, filename in ((False, "corescope-public-pg.json"), (True, "corescope-public-ram.json")):
        try:
            path = run / filename
            receipt = read_json(path)
            application = corescope_application_context(receipt.get("fixture_token"))
            validate_receipt(receipt, ram=ram, application=application, run=run)
            checks.append({"name": filename, "passed": True})
            receipts[filename] = digest(path)
            if ram:
                ram_receipt = receipt
        except Exception as error:
            checks.append({"name": filename, "passed": False, "error": safe_error(error)})
    api, intake, corescope = json.loads(docker(["inspect", "ukmesh-full-backend-1", "ukmesh-full-mqtt-ingest-1",
                                              "ukmesh-companions-corescope-ingestor-1"]).stdout)
    durable = all(dict(value.split("=", 1) for value in service["Config"]["Env"]).get("MQTT_INGEST_MODE", "durable")
                  == "durable" for service in (api, intake))
    mode = read_json(STATE / "runtime/corescope-mode.json")
    config_file = next((Path(mount["Source"]) for mount in corescope["Mounts"]
                        if mount["Type"] == "bind" and mount["Destination"] == "/config/config.json"), None)
    configuration = read_json(config_file) if config_file else {}
    base = read_json(REPO / "scripts/companion-dev/corescope.json")
    checks.append({"name": "default MQTT and durable ownership restored", "passed":
                   mode.get("mode") == "mqtt" and durable
                   and not configuration.get("publicProjection") and configuration.get("mqttSources") == base.get("mqttSources")
                   and not configuration.get("postgresSources") and not configuration.get("redisSources")})
    if ram_receipt:
        previous = read_json(Path(ram_receipt["attempt"]) / "redis-before.json")
        actual = json.loads(docker(["exec", "ukmesh-sample-redis", "redis-cli", "--json", "CONFIG", "GET",
                                   "appendonly", "appendfsync", "maxmemory", "maxmemory-policy"]).stdout)
        checks.append({"name": "normal Redis settings actually restored", "passed": actual == previous})
    result = {"at": utc_now(), "passed": all(check["passed"] for check in checks), "checks": checks,
              "receipts": receipts, "application": application, "context": phase_context(run),
              "source_scope": "CoreScope source/binaries, API and ingest images, installed reader policy and exact Core proof sources"}
    write_json(run / "core-projection.json", result)
    return result
