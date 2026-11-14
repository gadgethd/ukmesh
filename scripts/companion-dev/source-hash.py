#!/usr/bin/env python3
"""Fingerprint build inputs, including new source files, without dependency caches."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import yaml

PROJECTS = {'corescope': 'corescope', 'beacon': 'beacon-server', 'web': 'beacon-web'}


def fingerprint(root: Path, mode: str) -> str:
    root = root.resolve()
    digest = hashlib.sha256()
    tools = root / 'scripts/companion-dev'
    builder = tools / ('web-build.sh' if mode == 'web' else f'build-{mode}.sh')
    digest.update(builder.read_bytes())
    if mode != 'web':
        digest.update((tools / 'build.sh').read_bytes())
    configuration = yaml.safe_load((tools / 'compose.yml').read_text())
    recipe = configuration['services']['web-build' if mode == 'web' else 'build']
    # Compiler image/flags and frontend build variables are build inputs too;
    # runtime-only configuration must not rebuild an unrelated component.
    digest.update(json.dumps({key: recipe.get(key) for key in
        ('image', 'entrypoint', 'working_dir', 'environment')}, sort_keys=True).encode())
    project = PROJECTS[mode]
    checkout = root / 'third_party' / project
    digest.update(subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD']))
    paths = subprocess.check_output(['git', '-C', str(checkout), 'ls-files', '-z', '--cached', '--others', '--exclude-standard']).split(b'\0')
    for raw in sorted(set(paths)):
        if not raw:
            continue
        name = os.fsdecode(raw)
        path = checkout / name
        digest.update(project.encode() + b'\0' + raw + b'\0')
        if path.is_symlink():
            if not path.resolve().is_relative_to(checkout):
                raise ValueError('build input is outside its repository')
            digest.update(b'symlink\0' + os.fsencode(os.readlink(path)) + b'\0')
        if path.is_file():
            if not path.resolve().is_relative_to(checkout):
                raise ValueError('build input is outside its repository')
            with path.open('rb') as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    digest.update(chunk)
        else:
            digest.update(b'[deleted]')
    return digest.hexdigest()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=PROJECTS)
    arguments = parser.parse_args()
    print(fingerprint(Path(__file__).resolve().parents[2], arguments.mode))
