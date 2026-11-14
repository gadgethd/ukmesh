"""Local ownership, subprocess and evidence boundaries for qualification."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from typing import Any, BinaryIO


REPO = Path(__file__).resolve().parents[3]
STATE = REPO / "plans/lean-runtime"
EVIDENCE = STATE / "qualification"
MANIFEST = REPO / "scripts/full-dev/services.json"
DB_NAME = re.compile(r"lean_(?:verify|fixture|1x|10x|soak|owner|beacon)_[a-z0-9_]{1,38}\Z")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def clean_environment() -> dict[str, str]:
    return {"PATH": os.environ.get("PATH", os.defpath),
            "HOME": os.environ.get("HOME", "/tmp"), "LANG": "C.UTF-8",
            "DOCKER_HOST": "unix:///var/run/docker.sock"}


def command(args: list[str], *, data: bytes | None = None,
            timeout: float = 120, check: bool = True,
            input_stream: BinaryIO | None = None,
            output_stream: BinaryIO | None = None,
            merge_stderr: bool = False) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(args, input=data, stdin=input_stream,
                          stdout=output_stream if output_stream is not None else subprocess.PIPE,
                          stderr=subprocess.STDOUT if merge_stderr else subprocess.PIPE, env=clean_environment(),
                          cwd=REPO, timeout=timeout, check=check)


def docker(args: list[str], *, data: bytes | None = None,
           timeout: float = 120, check: bool = True,
           input_stream: BinaryIO | None = None,
           output_stream: BinaryIO | None = None,
           merge_stderr: bool = False) -> subprocess.CompletedProcess[bytes]:
    # sg receives shell code, so quote every argument, including SQL and paths.
    return command(["sg", "docker", "-c", shlex.join(["docker", "--host", "unix:///var/run/docker.sock", *args])],
                   data=data, timeout=timeout, check=check,
                   input_stream=input_stream, output_stream=output_stream, merge_stderr=merge_stderr)


def sql(container: str, database: str, user: str, query: str,
        *, check: bool = True, timeout: float = 120) -> str:
    result = docker(["exec", "-i", container, "psql", "-X", "-q", "-A", "-t",
                     "-v", "ON_ERROR_STOP=1", "-U", user, "-d", database],
                    data=query.encode(), check=check, timeout=timeout)
    if not check and result.returncode:
        raise RuntimeError(f"SQL operation failed (exit {result.returncode}); query and credentials withheld")
    return result.stdout.decode().strip()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".evidence-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def new_run() -> Path:
    EVIDENCE.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(tempfile.mkdtemp(prefix=stamp + "-", dir=EVIDENCE))
    (EVIDENCE / "latest").unlink(missing_ok=True)
    (EVIDENCE / "latest").symlink_to(path.name)
    return path


def owned_run(value: str | None, *, create: bool = False) -> Path:
    if value is None:
        if create or not (EVIDENCE / "latest").exists():
            return new_run()
        path = (EVIDENCE / "latest").resolve()
    else:
        path = Path(value).resolve()
    if not path.is_relative_to(EVIDENCE.resolve()) or path == EVIDENCE.resolve():
        raise ValueError("evidence directory must be under plans/lean-runtime/qualification")
    if not path.is_dir():
        raise ValueError("evidence directory does not exist")
    return path


def service_manifest() -> dict[str, Any]:
    value = read_json(MANIFEST)
    if isinstance(value, list):
        value = {"services": value}
    names = [service["name"] for service in value["services"]]
    if len(names) != len(set(names)):
        raise ValueError("service manifest contains duplicate names")
    return value


def safe_error(error: Exception) -> str:
    # CalledProcessError text can contain passwords in arguments or row values.
    if isinstance(error, subprocess.CalledProcessError):
        return f"subprocess failed with exit {error.returncode}; private details withheld"
    if isinstance(error, subprocess.TimeoutExpired):
        return f"subprocess exceeded {error.timeout}s"
    return str(error)
