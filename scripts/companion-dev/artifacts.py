#!/usr/bin/env python3
"""Publish a complete web build and its revision, retaining crash recovery."""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat


def paths(state: Path) -> tuple[Path, Path, Path]:
    values = tuple(state / name for name in ('beacon-web', 'beacon-web.next', 'beacon-web.previous'))
    if any(value.is_symlink() for value in values):
        raise ValueError('Static artifact directories must not be symbolic links')
    return values


def static_entries(directory: Path) -> dict[Path, int]:
    """Validate the complete owned tree before reading or changing its entries."""
    root_mode = directory.lstat().st_mode
    if stat.S_ISLNK(root_mode):
        raise ValueError('Static artifact directories must not be symbolic links')
    if not stat.S_ISDIR(root_mode):
        raise ValueError('A static artifact must be a directory')
    entries = {directory: 0o755}

    def fail_walk(error: OSError) -> None:
        raise error

    for current, directories, files in os.walk(directory, followlinks=False, onerror=fail_walk):
        for name in directories + files:
            entry = Path(current) / name
            mode = entry.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise ValueError('Static artifact entries must not be symbolic links')
            if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise ValueError('Static artifacts require regular files and directories')
            entries[entry] = 0o755 if stat.S_ISDIR(mode) else 0o644
    return entries


def public_static_permissions(directory: Path) -> None:
    """The artifact owns public modes; private state and caller umask do not."""
    for entry, mode in static_entries(directory).items():
        entry.chmod(mode, follow_symlinks=False)


def static_fingerprint(directory: Path) -> str:
    digest = hashlib.sha256(b'ukmesh-static-publication-v1\0')
    for entry, mode in sorted(static_entries(directory).items(), key=lambda item: item[0].relative_to(directory).as_posix()):
        name = os.fsencode(entry.relative_to(directory))
        digest.update(b'D' if mode == 0o755 else b'F')
        digest.update(len(name).to_bytes(8, 'big'))
        digest.update(name)
        if mode == 0o644:
            digest.update(entry.stat().st_size.to_bytes(8, 'big'))
            with entry.open('rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(chunk)
    return digest.hexdigest()


def web_publication(state: Path) -> str:
    active = state / 'beacon-web'
    if not active.exists():
        return 'unknown'
    publication = static_fingerprint(active)
    return publication if (active / 'index.html').is_file() else 'unknown'


def recover_web(state: Path) -> None:
    active, _, previous = paths(state)
    if active.exists():
        public_static_permissions(active)
    if previous.exists():
        if active.exists():
            shutil.rmtree(previous)
        else:
            public_static_permissions(previous)
            os.replace(previous, active)


def prepare_web(state: Path) -> None:
    recover_web(state)
    _, staged, _ = paths(state)
    if staged.exists():
        shutil.rmtree(staged)
    staged.mkdir(mode=0o700)


def web_revision(state: Path) -> str:
    active = state / 'beacon-web'
    if active.is_symlink() or not (active / 'index.html').is_file():
        return 'unknown'
    marker = active / '.ukmesh-source-revision'
    # Preserve the previous launcher's cache until its first complete rebuild.
    if not marker.exists():
        marker = state / 'web-source'
    revision = marker.read_text().strip() if marker.exists() and not marker.is_symlink() else ''
    return revision if re.fullmatch('[a-f0-9]{64}', revision) else 'unknown'


def publish_web(state: Path, revision: str) -> None:
    if not re.fullmatch('[a-f0-9]{64}', revision):
        raise ValueError('A complete source revision is required')
    recover_web(state)
    active, staged, previous = paths(state)
    if not (staged / 'index.html').is_file():
        raise ValueError('Beacon static build is incomplete')
    public_static_permissions(staged)
    marker = staged / '.ukmesh-source-revision'
    marker.write_text(revision + '\n')
    marker.chmod(0o644, follow_symlinks=False)
    if active.exists() and web_publication(state) == static_fingerprint(staged):
        # Existing bind mounts retain this inode when a rebuild changes no bytes.
        shutil.rmtree(staged)
        return
    if active.exists():
        os.replace(active, previous)
    try:
        os.replace(staged, active)
    except BaseException:
        if previous.exists() and not active.exists():
            os.replace(previous, active)
        raise
    if previous.exists():
        shutil.rmtree(previous)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('recover-web', 'prepare-web', 'publish-web', 'web-revision', 'web-publication'))
    parser.add_argument('revision', nargs='?')
    args = parser.parse_args()
    state = Path(__file__).resolve().parents[2] / 'plans/companion-dev/runtime'
    if args.action == 'publish-web':
        publish_web(state, args.revision or '')
    elif args.action == 'web-revision':
        print(web_revision(state))
    elif args.action == 'web-publication':
        print(web_publication(state))
    else:
        {'recover-web': recover_web, 'prepare-web': prepare_web}[args.action](state)


if __name__ == '__main__':
    main()
