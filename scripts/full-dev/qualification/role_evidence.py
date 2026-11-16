"""Admit only native, authenticated application-role read evidence.

Expected role policy remains owned by consumer_refs; this validates redacted
session metadata, never creates a connection or accepts a credential fallback.
"""
from urllib.parse import quote
from consumer_refs import describe_consumer


def validate_database_identity(value, context, *, operations=False):
    try:
        if type(value["version"]) is not int or value["version"] != 1 or value["scope"] != "application-role-readonly":
            raise ValueError()
        targets = context["targets"]
        expected = value["expected"]
        if set(expected) != {"http", "worker", "ingest", "owner"}:
            raise ValueError()
        settings = {"http": "MESHCORE_HTTP_DATABASE_URL", "worker": "MESHCORE_WORKER_DATABASE_URL",
                    "ingest": "MESHCORE_INGEST_DATABASE_URL", "owner": "OWNER_DATABASE_URL"}
        # Use the existing consumer's role/ref policy, with metadata-only URLs.
        # These contain no passwords and are never used to connect.
        environment = {}
        for role, setting in settings.items():
            row = expected[role]
            database = targets["owner" if role == "owner" else "ukmesh"]
            if not database or row["database"] != database:
                raise ValueError()
            environment[setting] = "postgresql://" + quote(row["user"], safe="") + "@ukmesh-sample-db:5432/" + quote(database, safe="")
        environment["DATABASE_URL"] = environment["MESHCORE_HTTP_DATABASE_URL"]
        consumer = describe_consumer("ukmesh-full-backend-1", {"Config": {"Env": [k+"="+v for k,v in environment.items()]}})
        if consumer["refs"] != {"ukmesh": targets["ukmesh"], "owner": targets["owner"]}:
            raise ValueError()
        used = {"http", "worker"} if operations else {"http"}
        for phase in ("setup", "after"):
            if set(value[phase]) != used:
                raise ValueError()
            for role in used:
                session = value[phase][role]
                if session["current_user"] != expected[role]["user"] or session["session_user"] != expected[role]["user"] \
                        or session["database"] != expected[role]["database"] \
                        or type(session["backend_pid"]) is not int or session["backend_pid"] <= 0 \
                        or session["transaction_read_only"] != "on" or session["default_transaction_read_only"] != "on" \
                        or session["superuser"] is not False or session["bypass_rls"] is not False:
                    raise ValueError()
        if set(value["profiles"]) != {"http", "worker", "ingest"} \
                or any(type(value["profiles"][name]["max"]) is not int or value["profiles"][name]["max"] != maximum
                       for name, maximum in (("http", 8), ("worker", 2), ("ingest", 2))) \
                or type(value["ownerPool"]["configuredMax"]) is not int or value["ownerPool"]["configuredMax"] != 2 \
                or value["ownerPool"]["queriedByBenchmark"] is not False:
            raise ValueError()
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("native authenticated application-role read-only evidence is missing or differs") from None
