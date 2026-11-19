"""Own the bounded HTTP helper used by a real published-stack soak."""
import json
import os
import re
import subprocess
from pathlib import Path
import time
import tempfile
import uuid

from common import REPO, docker, read_json, write_json


def start_reader(run: Path) -> Path:
    directory = run / "http-reader"
    directory.mkdir(mode=0o700)
    backend = json.loads(docker(["inspect", "ukmesh-full-backend-1"]).stdout)[0]
    if not backend['State']['Running'] or backend['Config'].get('Labels',{}).get('com.docker.compose.project')!='ukmesh-full' \
            or backend['Config']['Labels'].get('com.docker.compose.service')!='backend':
        raise ValueError('HTTP reader requires the running owned backend namespace')
    name = "ukmesh-owned-http-reader-" + uuid.uuid4().hex[:12]
    # sockaddr_un is only108 bytes; the repository's owned evidence path is longer.
    alias=Path(tempfile.gettempdir())/name
    owner = directory / 'owner.json'
    write_json(owner, {'name': name, 'started': False})
    alias.symlink_to(directory.resolve(),target_is_directory=True)
    created=False
    try:
        docker(["run", "-d", "--name", name, "--label", 'ukmesh.owned-http-reader='+name,
                '--label','ukmesh.http-reader.alias='+str(alias),
                '--label','ukmesh.http-reader.directory='+str(directory.resolve()),
                "--network", "container:"+backend['Id'],
                "--memory", "64m", "--memory-swap", "64m", "--cpus", "0.25", "--pids-limit", "64",
                "--user", f"{os.getuid()}:{os.getgid()}", "--read-only", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges", "--entrypoint", "node",
                "--mount", f"type=bind,src={directory},dst=/reader",
                "--mount", f"type=bind,src={REPO / 'scripts/full-dev/qualification/http-reader.mjs'},dst=/reader-source.mjs,readonly",
                backend["Image"], "--max-old-space-size=24", "/reader-source.mjs", "/reader/reader.sock"])
        created=True
        write_json(owner, {'name': name, 'started': True})
        socket = directory / "reader.sock"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if socket.is_socket():
                return alias / 'reader.sock'
            time.sleep(.1)
        raise RuntimeError("bounded native HTTP reader did not become ready")
    except BaseException:
        try:
            recover_reader(run)
        except (OSError, ValueError, subprocess.SubprocessError):
            if created:
                raise
            if alias.is_symlink() and alias.resolve() == directory:
                alias.unlink(missing_ok=True)
        raise


def recover_reader(run: Path, *, require_alias: bool = False) -> None:
    """Clean up the exact run's detached helper after the workload settles."""
    directory = run.resolve() / 'http-reader'
    if directory.is_symlink():
        raise ValueError('HTTP reader recovery evidence is outside the owned run')
    owner = directory / 'owner.json'
    if not owner.exists():
        return
    if not owner.is_file() or owner.is_symlink():
        raise ValueError('HTTP reader recovery evidence is outside the owned run')
    receipt = read_json(owner)
    name = receipt['name']
    if not re.fullmatch(r'ukmesh-owned-http-reader-[0-9a-f]{12}', name):
        raise ValueError('invalid owned HTTP reader name')
    alias = Path(tempfile.gettempdir()) / name
    deadline = time.monotonic() + (0 if receipt.get('started') else 5)
    while True:
        containers = docker(['ps', '-aq', '--filter', 'name=^/' + name + '$'], timeout=5).stdout.decode().split()
        if containers or time.monotonic() >= deadline:
            break
        time.sleep(.2)
    owned_alias = alias.is_symlink() and alias.resolve() == directory
    if containers:
        item = json.loads(docker(['inspect', containers[0]], timeout=5).stdout)[0]
        labels = item['Config'].get('Labels', {})
        mounts = [mount['Source'] for mount in item['Mounts'] if mount['Destination'] == '/reader']
        if (len(containers) != 1 or not item['Id'].startswith(containers[0]) or item['Name'] != '/' + name
                or labels.get('ukmesh.owned-http-reader') != name
                or labels.get('ukmesh.http-reader.alias') != str(alias)
                or labels.get('ukmesh.http-reader.directory') != str(directory)
                or mounts != [str(directory)]):
            raise ValueError('HTTP reader recovery ownership differs')
        # Remove by the verified immutable identity, even if the short
        # socket alias has already disappeared.
        docker(['rm', '-f', item['Id']], timeout=30)
        if require_alias and not owned_alias:
            raise ValueError('owned HTTP reader was removed; short socket alias ownership differs')
    if owned_alias and alias.is_symlink() and alias.resolve() == directory:
        alias.unlink(missing_ok=True)
