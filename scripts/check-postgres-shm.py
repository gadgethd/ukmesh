#!/usr/bin/env python3
"""Inventory Compose/Actions services and enforce the repo's 1 GiB PG shm floor.

Only reads Git-visible YAML; never invokes Docker, expands variables, reads .env,
or contacts a database. Custom PostgreSQL images must set x-postgres: true.
"""

import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys

import yaml

MIN_SHM = 1024**3
PG_IMAGE = re.compile(
    r"(?:^|/)(?:postgres(?:ql)?(?:-[\w.]+)?|timescaledb(?:-[\w.]+)?|postgis|pgvector)"
    r"(?=[:@/}]|$)", re.I,
)


class ComposeLoader(yaml.SafeLoader):
    """Understand Compose tags without executing or resolving any host config."""


def compose_tag(loader, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    # !reset always restores the default, regardless of the supplied value.
    if node.tag == "!reset":
        return None
    return loader.construct_scalar(node)


for tag in ("!reset", "!override"):
    ComposeLoader.add_constructor(tag, compose_tag)


def size_bytes(value):
    """Docker byte values, including numeric byte counts; no interpolation."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    match = re.fullmatch(r"(\d+)(b|[kmg]b?)?", str(value).strip(), re.I)
    if not match:
        return None
    unit = (match[2] or "b")[0].lower()
    return int(match[1]) * 1024 ** "bkmg".index(unit)


def postgres(service):
    image = str(service.get("image", ""))
    # Exporters are clients, including when the image name contains postgres.
    candidates = [image, *re.findall(r"\$\{\w+(?::-|-)([^{}]+)\}", image)]
    image_match = any(PG_IMAGE.search(candidate) and "postgres-exporter" not in candidate
                      for candidate in candidates)
    command = service.get("command", [])
    entrypoint = service.get("entrypoint", [])
    words = list(command) if isinstance(command, list) else str(command).split()
    words += entrypoint if isinstance(entrypoint, list) else str(entrypoint).split()
    environment = service.get("environment") or service.get("env") or {}
    keys = environment if isinstance(environment, dict) else (
        str(item).split("=", 1)[0] for item in environment
    )
    server_env = any(key in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "PGDATA")
                     for key in keys)
    return bool(service.get("x-postgres") is True or image_match or server_env or any(
        Path(str(word)).name in ("postgres", "postmaster") for word in words
    ))


def git_yaml_paths(root):
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root, check=True, capture_output=True,
    )
    return sorted({
        Path(p) for p in result.stdout.decode().split("\0")
        if re.search(r"\.ya?ml(?:\.example)?$", p, re.I)
    })


def load_services(root):
    rows = []
    compose_files = []
    for path in git_yaml_paths(root):
        # A tracked deletion must not silently remove an inventory entry.
        document = yaml.load((root / path).read_text(), Loader=ComposeLoader) or {}
        if not isinstance(document, dict):
            continue
        if "include" in document:
            raise ValueError(f"{path}: Compose include needs explicit guard support")
        services = document.get("services")
        if isinstance(services, dict):
            compose_files.append(str(path))
            for name, service in services.items():
                if not isinstance(service, dict):
                    raise ValueError(f"{path}:{name}: expected a service mapping")
                if "extends" in service:
                    raise ValueError(f"{path}:{name}: extends needs explicit guard support")
                rows.append((str(path), str(name), "compose", service))
        if ".github/workflows" in path.as_posix():
            for job_name, job in (document.get("jobs") or {}).items():
                for name, service in (job.get("services") or {}).items():
                    rows.append((str(path), f"{job_name}/{name}", "actions", service))
    return rows, compose_files


def option_value(words, option):
    values = []
    for index, word in enumerate(words):
        if word.startswith(option + "="):
            values.append(word.split("=", 1)[1])
        elif word == option:
            values.append(words[index + 1] if index + 1 < len(words) else None)
    if len(values) > 1:
        raise ValueError(f"duplicate {option} options")
    return values[0] if values else None


def audit(root):
    services, compose_files = load_services(root)
    # Overlays can omit image/command; associate names only within a directory.
    pg_names = {
        (str(Path(path).parent), name)
        for path, name, kind, service in services if kind == "compose" and postgres(service)
    }
    inventory, errors = [], []
    for path, name, kind, service in services:
        direct_pg = postgres(service)
        pg_overlay = kind == "compose" and not direct_pg and (
            str(Path(path).parent), name
        ) in pg_names
        pg = direct_pg or pg_overlay
        options = shlex.split(service.get("options", "")) if kind == "actions" else []
        shm = option_value(options, "--shm-size") if kind == "actions" else service.get("shm_size")
        entry = {
            "source": path, "service": name, "kind": kind,
            "postgres": pg, "shm_size": shm,
            "volumes": service.get("volumes", []),
            "tmpfs": service.get("tmpfs", []),
        }
        problems = []
        if pg:
            if pg_overlay and "shm_size" not in service:
                entry["status"] = "unknown"
                entry["reason"] = "overlay only; inherits shm_size from its chosen base"
            elif size_bytes(shm) is None or size_bytes(shm) < MIN_SHM:
                problems.append("requires explicit literal shm_size >= 1g (1073741824 bytes)")
            else:
                entry["status"] = "safe"
                entry["reason"] = "meets repo floor; workload capacity still needs monitoring"
            ipc = option_value(options, "--ipc") if kind == "actions" else service.get("ipc")
            if ipc not in (None, "private", "shareable"):
                problems.append("shared/disabled IPC bypasses the container shm_size")
            mounts = json.dumps(entry["volumes"]) + json.dumps(entry["tmpfs"])
            if "/dev/shm" in mounts or any("/dev/shm" in word for word in options):
                problems.append("/dev/shm mount masks shm_size; needs explicit policy review")
        else:
            entry["status"] = "exempt"
            entry["reason"] = "not PostgreSQL; see audit for compute-service review"
        if problems:
            entry["status"] = "at risk"
            entry["reason"] = "; ".join(problems)
            errors.append(f"{path}:{name}: {entry['reason']}")
        inventory.append(entry)
    if not any(row["postgres"] and row["status"] != "unknown" for row in inventory):
        errors.append("no PostgreSQL definitions found; inventory unexpectedly empty")
    return {"minimum_bytes": MIN_SHM, "compose_files": compose_files,
            "inventory": inventory, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--json", action="store_true", help="emit the complete inventory as JSON")
    args = parser.parse_args()
    try:
        report = audit(args.root)
    except (OSError, ValueError, yaml.YAMLError, subprocess.CalledProcessError) as exc:
        print(f"PostgreSQL shm check ERROR: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("SOURCE | SERVICE | CLASS | SHM_SIZE | VOLUME MOUNTS | TMPFS")
        for row in report["inventory"]:
            print(" | ".join(str(value) for value in (
                row["source"], row["service"], row["status"],
                row["shm_size"] if row["shm_size"] is not None else "unset",
                json.dumps(row["volumes"]), json.dumps(row["tmpfs"]),
            )))
        pg_count = sum(row["postgres"] for row in report["inventory"])
        print(f"Inventory: {len(report['compose_files'])} Compose files, "
              f"{len(report['inventory'])} service entries, {pg_count} PostgreSQL entries.")
        for error in report["errors"]:
            print(f"FAIL: {error}")
        print("PostgreSQL shm check: " + ("FAIL" if report["errors"] else "PASS"))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
