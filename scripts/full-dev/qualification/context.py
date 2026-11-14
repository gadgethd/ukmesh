"""Bind evidence to actual source, runtime configuration and fixture ownership."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from common import REPO, command, digest, docker, service_manifest
from fixtures import current_targets


def repository_contents(directory: Path) -> str:
    listing = command(["git", "-C", str(directory), "ls-files", "--cached", "--others", "--exclude-standard", "-z"]).stdout.split(b"\0")
    value = hashlib.sha256()
    for name in sorted(set(listing)):
        if name and (directory / os.fsdecode(name)).is_file() and not (directory / os.fsdecode(name)).is_symlink():
            value.update(name + b"\0")
            value.update(bytes.fromhex(digest(directory / os.fsdecode(name))))
    return value.hexdigest()


def corescope_application_context(policy_fixture_token: str | None = None) -> dict:
    """Scope Core adapter evidence to its actual app code, binaries and policy."""
    names = ["ukmesh-companions-corescope-1", "ukmesh-companions-corescope-ingestor-1", "ukmesh-full-backend-1"]
    inspected = {item["Name"].lstrip("/"): item for item in json.loads(docker(["inspect", *names]).stdout)}
    binaries = {}
    for name in names[:2]:
        item = inspected[name]
        executable = item["Config"]["Entrypoint"][0]
        mounts = sorted(item["Mounts"], key=lambda value: len(value["Destination"]), reverse=True)
        for mount in mounts:
            if mount["Type"] == "bind" and executable.startswith(mount["Destination"].rstrip("/") + "/"):
                file = Path(mount["Source"]) / executable.removeprefix(mount["Destination"].rstrip("/") + "/")
                binaries[name] = digest(file)
                break
        else:
            raise ValueError("CoreScope executable is not backed by its owned runtime binary")
    from common import sql
    if policy_fixture_token:
        from fixtures import bundle
        policy_database = next(item["database"] for item in bundle(policy_fixture_token)["databases"] if item["label"] == "ukmesh")
    else:
        policy_database = current_targets()["ukmesh"]
    definitions = sql("ukmesh-sample-db", policy_database, "meshcore", """SELECT coalesce(string_agg(body,E'\\n' ORDER BY name),'') FROM (
      SELECT 'view:'||c.relname AS name, pg_get_viewdef(c.oid,true) AS body FROM pg_class c
      JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='corescope_public' AND c.relkind='v'
      UNION ALL SELECT 'function:'||p.proname||':'||pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid)
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='corescope_public') definitions""")
    if not definitions:
        raise ValueError("installed CoreScope public reader policy is missing")
    proof = hashlib.sha256()
    for relative in ("scripts/full-dev/corescope-public.py", "scripts/full-dev/corescope-public.sh",
                     "scripts/full-dev/corescope-public.yml", "scripts/full-dev/corescope-ram-runtime.sh",
                     "scripts/full-dev/runtime.sh", "scripts/full-dev/environment.sh",
                     "docker-compose.yml", "scripts/full-dev/compose.yml", "scripts/companion-dev/compose.yml",
                     "scripts/full-dev/corescope-public-proof.mjs", "scripts/full-dev/corescope-ram-proof.mjs",
                     "scripts/full-dev/corescope-proof-utils.mjs", "backend/src/tools/generateSampleDataset.ts",
                     "backend/src/tools/replaySampleMqtt.ts", "backend/src/tools/ownedLookupFixture.ts"):
        proof.update(relative.encode() + b"\0")
        proof.update(bytes.fromhex(digest(REPO / relative)))
    return {"version": 1,
        "corescope_revision": command(["git", "-C", str(REPO / "third_party/corescope"), "rev-parse", "HEAD"]).stdout.decode().strip(),
        "corescope_source_sha256": repository_contents(REPO / "third_party/corescope"),
        "corescope_binaries": binaries,
        "backend_image_id": inspected["ukmesh-full-backend-1"]["Image"],
        "reader_policy_source_sha256": digest(REPO / "scripts/full-dev/corescope-reader.sql"),
        "reader_policy_installed_sha256": hashlib.sha256(definitions.encode()).hexdigest(),
        "base_configuration_sha256": digest(REPO / "scripts/companion-dev/corescope.json"),
        "proof_code_sha256": proof.hexdigest()}

def source_state() -> dict[str, str]:
    tracked = command(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"]).stdout.split(b"\0")
    checksum = hashlib.sha256()
    for relative in sorted(set(tracked)):
        if not relative:
            continue
        path = REPO / os.fsdecode(relative)
        if path.is_file() and not path.is_symlink():
            checksum.update(relative + b"\0")
            checksum.update(bytes.fromhex(digest(path)))
    submodules = hashlib.sha256(command(["git", "submodule", "status"]).stdout)
    configured = command(["git", "config", "--file", ".gitmodules", "--get-regexp", "path"]).stdout.decode().splitlines()
    for line in sorted(configured):
        relative = line.split(None, 1)[1]
        source = REPO / relative
        listing = command(["git", "-C", str(source), "ls-files", "--cached", "--others", "--exclude-standard", "-z"]).stdout.split(b"\0")
        for name in sorted(set(listing)):
            if not name:
                continue
            path = source / os.fsdecode(name)
            if path.is_file() and not path.is_symlink():
                submodules.update(relative.encode() + b"/" + name + b"\0")
                submodules.update(bytes.fromhex(digest(path)))
    return {"revision": command(["git", "rev-parse", "HEAD"]).stdout.decode().strip(),
            "root_content_sha256": checksum.hexdigest(),
            "submodules_sha256": submodules.hexdigest()}


def running_profile(targets: dict) -> dict:
    manifest = service_manifest()
    names = [service.get("container", service["name"]) for service in manifest["services"]
             if service.get("kind", "container") in ("container", "oneshot") and service.get("required", True)]
    if not names:
        raise ValueError("qualification profile has no required services")
    inspected_result = docker(["inspect", *names], check=False)
    if not inspected_result.stdout:
        raise RuntimeError("runtime inspection returned no service evidence")
    inspected = json.loads(inspected_result.stdout)
    from targets import runtime_targets
    runtime_targets({item["Name"].lstrip("/"): item for item in inspected}, allow_public_variant=True)
    present = {item["Name"].lstrip("/") for item in inspected}
    if inspected_result.returncode and present == set(names):
        raise RuntimeError("runtime inspection failed despite a complete service list")
    values = {}
    directory_targets = ("/config", "/etc/nginx", "/etc/prometheus", "/etc/grafana/provisioning",
                         "/etc/alloy", "/etc/loki", "/etc/alertmanager", "/etc/anubis",
                         "/bin/companions", "/usr/share/nginx/html", "/public")
    mutable_targets = ("/data/", "/var/lib/", "/var/log/", "/app/data/", "/health-volumes/", "/cache/")
    from urllib.parse import urlsplit, urlunsplit
    target_names = set(targets.values()) | {"meshcore", "meshcore_owner_auth", "beacon_dev"}
    for item in inspected:
        configuration = hashlib.sha256()
        environment = []
        for setting in item["Config"].get("Env", []):
            key, _, value = setting.partition("=")
            if "DATABASE_URL" in key or key.endswith("_DSN"):
                parsed = urlsplit(value)
                if parsed.path.lstrip("/") in target_names:
                    value = urlunsplit((parsed.scheme, parsed.netloc, "/selected-fixture", parsed.query, parsed.fragment))
            elif key in ("UKMESH_DATABASE", "OWNER_DATABASE", "BEACON_DATABASE"):
                value = "selected-fixture" if value in target_names else value
            environment.append(key + "=" + value)
        configuration.update(json.dumps({"env": sorted(environment), "command": item["Config"].get("Cmd"),
                                         "entrypoint": item["Config"].get("Entrypoint")}, sort_keys=True).encode())
        for mount in sorted(item.get("Mounts", []), key=lambda value: value["Destination"]):
            if mount["Type"] != "bind" or mount["Destination"].startswith(mutable_targets):
                continue
            source = Path(mount["Source"])
            files = [source] if source.is_file() else (
                sorted(source.rglob("*")) if mount["Destination"].startswith(directory_targets) else [])
            for path in files:
                if path.is_file() and not path.is_symlink():
                    relative = str(path.relative_to(source)) if source.is_dir() else "."
                    configuration.update((mount["Destination"] + "/" + relative).encode() + b"\0")
                    configuration.update(bytes.fromhex(digest(path)))
        host = item["HostConfig"]
        values[item["Name"].lstrip("/")] = {"image_id": item["Image"],
            "memory": host["Memory"], "memory_swap": host["MemorySwap"],
            "nano_cpus": host["NanoCpus"], "cpu_set": host["CpusetCpus"],
            "shared_memory": host["ShmSize"], "configuration_sha256": configuration.hexdigest()}
    for name in sorted(set(names) - present):
        values[name] = {"missing": True}
    return values


def phase_context(run: Path) -> dict:
    profile = hashlib.sha256()
    for relative in ("docker-compose.yml", "scripts/full-dev/compose.yml", "scripts/full-dev/environment.sh",
                     "scripts/full-dev/services.json", "scripts/companion-dev/compose.yml"):
        path = REPO / relative
        profile.update(relative.encode() + b"\0")
        profile.update(bytes.fromhex(digest(path)))
    targets = current_targets()
    runtime = running_profile(targets)
    profile.update(json.dumps(runtime, sort_keys=True).encode())
    return {"run_id": run.name, "source": source_state(), "profile_sha256": profile.hexdigest(),
            "targets": targets, "runtime": runtime}

