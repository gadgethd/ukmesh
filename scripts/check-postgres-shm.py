#!/usr/bin/env python3
"""Inventory Compose/Actions services and enforce the repo's 1 GiB PG shm floor.

Only reads Git-visible YAML; never invokes Docker, expands variables, reads .env,
or contacts a database. Custom PostgreSQL images must set x-postgres: true.
"""

import argparse
import json
import os
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


class OverrideList(list):
    pass


class OverrideMapping(dict):
    pass


def compose_tag(loader, node):
    if node.tag == "!reset":
        return None
    if isinstance(node, yaml.MappingNode):
        return OverrideMapping(loader.construct_mapping(node))
    if isinstance(node, yaml.SequenceNode):
        return OverrideList(loader.construct_sequence(node))
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


def merge_environment(parent, child):
    def entries(value):
        if isinstance(value, list):
            if not all(isinstance(entry, str) for entry in value):
                raise ValueError("extends environment list entries must be strings")
            return {key: item if separator else None for key, separator, item in
                    (entry.partition("=") for entry in value)}
        if not isinstance(value, dict):
            raise ValueError("extends environment must be a mapping or list")
        return value

    if child is None or isinstance(child, (OverrideList, OverrideMapping)):
        return child
    return {**entries(parent or {}), **entries(child)}


def volume_target(mount):
    if isinstance(mount, dict):
        target = mount.get("target")
    elif isinstance(mount, str):
        parts = mount.split(":")
        target = parts[0] if len(parts) == 1 else parts[1]
    else:
        target = None
    if not isinstance(target, str) or not target.startswith("/"):
        raise ValueError("extends volumes require a literal Linux container target")
    return target


def merge_policy(parent, child):
    """Apply extends rules for the values used by the shared-memory audit."""
    policy = {**parent, **child}
    if "environment" in child:
        policy["environment"] = merge_environment(parent.get("environment"), child["environment"])
    for key in ("volumes", "tmpfs"):
        value = child.get(key)
        if value is None or isinstance(value, OverrideList) or key not in parent:
            continue
        inherited = parent[key] or []
        if key == "volumes":
            if not isinstance(inherited, list) or not isinstance(value, list):
                raise ValueError("extends volumes must be lists")
            policy[key] = list({volume_target(mount): mount for mount in [*inherited, *value]}.values())
        else:
            parent_mounts = [inherited] if isinstance(inherited, str) else inherited
            child_mounts = [value] if isinstance(value, str) else value
            if not isinstance(parent_mounts, list) or not isinstance(child_mounts, list):
                raise ValueError("extends tmpfs must be a string or list")
            policy[key] = [*parent_mounts, *child_mounts]
    return policy


def service_policy(documents, main_path, name):
    chain, seen = [], set()
    path = main_path
    while True:
        key = (path, name)
        if key in seen:
            raise ValueError(f"{main_path}: circular extends at {path}:{name}")
        seen.add(key)
        services = documents.get(path, {}).get("services")
        service = services.get(name) if isinstance(services, dict) else None
        if not isinstance(service, dict):
            raise ValueError(f"{main_path}: missing Git-visible service {path}:{name}")
        if "extends" not in service:
            break
        reference = service["extends"]
        if (not isinstance(reference, dict) or set(reference) - {"file", "service"}
                or not isinstance(reference.get("service"), str) or not reference["service"]
                or "$" in reference["service"]):
            raise ValueError(f"{path}:{name}: extends requires a literal service mapping")
        chain.append({key: value for key, value in service.items() if key != "extends"})
        if "file" in reference:
            filename = reference["file"]
            if not isinstance(filename, str) or not filename or "$" in filename or Path(filename).is_absolute():
                raise ValueError(f"{path}:{name}: extends file must be a relative Git-visible path")
            # Nested references are relative to the file declaring them.
            path = Path(os.path.normpath(path.parent / filename))
        name = reference["service"]
    for override in reversed(chain):
        service = merge_policy(service, override)
    return service


def load_services(root):
    rows, compose_files, documents = [], [], {}
    root = root.resolve()
    for path in git_yaml_paths(root):
        # A tracked deletion must not silently remove an inventory entry.
        source = root / path
        if source.resolve() != source or not source.is_file():
            raise ValueError(f"{path}: Git-visible YAML must be a regular file without symlinks")
        document = yaml.load(source.read_text(), Loader=ComposeLoader) or {}
        if not isinstance(document, dict):
            continue
        if "include" in document:
            raise ValueError(f"{path}: Compose include needs explicit guard support")
        documents[path] = document
    for path, document in documents.items():
        services = document.get("services")
        if isinstance(services, dict):
            compose_files.append(str(path))
            for name, service in services.items():
                if not isinstance(service, dict):
                    raise ValueError(f"{path}:{name}: expected a service mapping")
                rows.append((str(path), str(name), "compose", service_policy(documents, path, name)))
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
