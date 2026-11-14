#!/usr/bin/env python3
"""Publish a complete web build and its revision, retaining crash recovery."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil


def paths(state: Path) -> tuple[Path, Path, Path]:
    values = tuple(state / name for name in ('beacon-web', 'beacon-web.next', 'beacon-web.previous'))
    if any(value.is_symlink() for value in values):
        raise ValueError('Static artifact directories must not be symbolic links')
    return values


def recover_web(state: Path) -> None:
    active, _, previous = paths(state)
    if previous.exists():
        if active.exists():
            shutil.rmtree(previous)
        else:
            os.replace(previous, active)


def prepare_web(state: Path) -> None:
    recover_web(state)
    _, staged, _ = paths(state)
    if staged.exists():
        shutil.rmtree(staged)
    staged.mkdir()


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
    (staged / '.ukmesh-source-revision').write_text(revision + '\n')
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
    parser.add_argument('action', choices=('recover-web', 'prepare-web', 'publish-web', 'web-revision'))
    parser.add_argument('revision', nargs='?')
    args = parser.parse_args()
    state = Path(__file__).resolve().parents[2] / 'plans/companion-dev/runtime'
    if args.action == 'publish-web':
        publish_web(state, args.revision or '')
    elif args.action == 'web-revision':
        print(web_revision(state))
    else:
        {'recover-web': recover_web, 'prepare-web': prepare_web}[args.action](state)


if __name__ == '__main__':
    main()
