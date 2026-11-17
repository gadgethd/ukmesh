"""Own the bounded HTTP helper used by a real published-stack soak."""
import json
import os
from pathlib import Path
import time
import tempfile
import uuid

from common import REPO, docker


def start_reader(run: Path) -> tuple[str, Path]:
    directory = run / "http-reader"
    directory.mkdir(mode=0o700)
    backend = json.loads(docker(["inspect", "ukmesh-full-backend-1"]).stdout)[0]
    if not backend['State']['Running'] or backend['Config'].get('Labels',{}).get('com.docker.compose.project')!='ukmesh-full' \
            or backend['Config']['Labels'].get('com.docker.compose.service')!='backend':
        raise ValueError('HTTP reader requires the running owned backend namespace')
    name = "ukmesh-owned-http-reader-" + uuid.uuid4().hex[:12]
    # sockaddr_un is only108 bytes; the repository's owned evidence path is longer.
    alias=Path(tempfile.gettempdir())/name
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
        socket = directory / "reader.sock"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if socket.is_socket():
                return name, alias/'reader.sock'
            time.sleep(.1)
        raise RuntimeError("bounded native HTTP reader did not become ready")
    except Exception:
        if created:
            stop_reader(name)
        else:
            alias.unlink()
        raise


def stop_reader(name: str) -> None:
    if not name.startswith("ukmesh-owned-http-reader-"):
        raise ValueError("unowned HTTP reader container")
    item=json.loads(docker(['inspect',name]).stdout)[0]
    if item['Name']!='/'+name or item['Config'].get('Labels',{}).get('ukmesh.owned-http-reader')!=name:
        raise ValueError('HTTP reader deletion ownership marker differs')
    labels=item['Config']['Labels']
    alias=Path(labels.get('ukmesh.http-reader.alias',''))
    directory=Path(labels.get('ukmesh.http-reader.directory',''))
    owned_alias=alias==Path(tempfile.gettempdir())/name and alias.is_symlink() and alias.resolve()==directory.resolve()
    docker(["rm", "-f", name], timeout=30)
    if not owned_alias:
        raise ValueError('owned HTTP reader was removed; short socket alias ownership differs')
    alias.unlink()
