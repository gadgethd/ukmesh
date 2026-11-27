#!/usr/bin/env python3
"""Activate a prepared DEV UI release without restarting data services."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'qualification'))
from common import REPO, STATE, docker, read_json, write_json
from target_safety import require_originals
from target_lease import validate_fd, ENV

ARTIFACTS = REPO / 'plans/ui-overhaul/releases'
RUNTIME = STATE / 'runtime/ui-release'
ACTIVE = RUNTIME / 'active.json'

# Project, Compose service, container, release directory, mount, loopback UI.
SERVICES = (
    ('main', 'app-ukmesh', 'ukmesh-full-app-ukmesh-1', 'app', '/usr/share/nginx/html', 'http://127.0.0.1:3003/'),
    ('main', 'website-ukmesh', 'ukmesh-full-website-ukmesh-1', 'website', '/usr/share/nginx/html', 'http://127.0.0.1:5173/'),
    ('main', 'website-dev', 'ukmesh-full-website-dev-1', 'website-dev', '/usr/share/nginx/html', 'http://127.0.0.1:3005/'),
    ('main', 'mesh-health-check', 'ukmesh-full-mesh-health-check-1', 'health', '/app/public', 'http://127.0.0.1:3090/app'),
    ('companions', 'corescope', 'ukmesh-companions-corescope-1', 'corescope', '/public', 'http://127.0.0.1:3102/'),
    ('companions', 'beacon-web', 'ukmesh-companions-beacon-web-1', 'beacon-web', '/usr/share/nginx/html', 'http://127.0.0.1:5175/'),
    ('lan', 'site', 'ukmesh-lan-site', 'hub', '/srv', None),
)

COMPOSE = {
    'main': ('ukmesh-full', REPO / 'docker-compose.yml', HERE.parent / 'compose.yml'),
    'companions': ('ukmesh-companions', REPO / 'scripts/companion-dev/compose.yml'),
    'lan': ('ukmesh-lan', HERE.parent / 'lan/compose.yml', STATE / 'runtime/lan/compose.json'),
}


def release_path(name):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}', name):
        raise ValueError('invalid DEV UI release ID')
    path = ARTIFACTS / name
    if not path.is_dir() or path.resolve() != path:
        raise ValueError(f'DEV UI release does not exist: {name}')
    return path


def active():
    return read_json(ACTIVE) if ACTIVE.is_file() else {'current': None, 'previous': None}


def override_path(name, group):
    return release_path(name) / f'{group}.compose.json'


def fingerprint(directory):
    digest = hashlib.sha256()
    for file in sorted(directory.rglob('*')):
        if file.is_symlink():
            raise ValueError(f'UI release may not contain symlinks: {file}')
        if file.is_file():
            digest.update(str(file.relative_to(directory)).encode() + b'\0')
            digest.update(hashlib.sha256(file.read_bytes()).digest())
    return digest.hexdigest()


def inspection(names):
    result = docker(['inspect', *names])
    return {row['Name'].lstrip('/'): row for row in json.loads(result.stdout)}


def seal(name):
    root = release_path(name)
    if (root / 'receipt.json').exists():
        raise ValueError('this UI release is already sealed')
    docker_state = inspection([entry[2] for entry in SERVICES])
    overrides = {group: {'services': {}} for group in COMPOSE}
    receipt = {}
    for group, service, container, asset, target, _ in SERVICES:
        directory = root / asset
        if not (directory / 'index.html').is_file():
            raise ValueError(f'missing HTML in {asset}')
        image = docker_state[container]['Image']
        overrides[group]['services'][service] = {
            'image': image,
            'volumes': [f'{directory}:{target}:ro'],
            'labels': {'ukmesh.ui.release': name},
        }
        receipt[asset] = {'image': image, 'sha256': fingerprint(directory)}
    for group, override in overrides.items():
        write_json(override_path(name, group), override)
    write_json(root / 'receipt.json', receipt)


def verify(name):
    root = release_path(name)
    receipt = read_json(root / 'receipt.json')
    for group, service, _, key, target, _ in SERVICES:
        identity = receipt[key]
        override = read_json(override_path(name, group))['services'][service]
        expected = {'image': identity['image'], 'volumes': [f'{root / key}:{target}:ro'],
                    'labels': {'ukmesh.ui.release': name}}
        if override != expected or fingerprint(root / key) != identity['sha256']:
            raise ValueError(f'DEV UI release no longer matches its receipt: {key}')
        docker(['image', 'inspect', identity['image']])


def switch(name, entry):
    group, service, container, asset, target, url = entry
    project, *files = COMPOSE[group]
    command = ['docker', '--host', 'unix:///var/run/docker.sock', 'compose', '--project-name', project, '--env-file', '/dev/null']
    for file in [*files, override_path(name, group)]:
        command += ['-f', str(file)]
    subprocess.run([*command, 'up', '-d', '--no-deps', '--no-build', '--pull', 'never',
                    '--wait', '--wait-timeout', '45', service], cwd=REPO, timeout=95, check=True)
    row = inspection([container])[container]
    if not any(mount['Source'] == str(release_path(name) / asset) and mount['Destination'] == target
               and not mount['RW'] for mount in row['Mounts']):
        raise ValueError(f'{service} is not serving the selected release')
    if group == 'lan':
        address = read_json(STATE / 'runtime/lan/active.json')['address']
        url = f'http://{address}:8080/'
    with urlopen(url, timeout=8) as response:
        if response.status != 200 or b'<' not in response.read(8192):
            raise ValueError(f'{service} did not serve HTML')


def apply(name, previous):
    verify(name)
    verify(previous)
    require_originals()
    attempted = []
    try:
        for entry in SERVICES:
            attempted.append(entry)
            switch(name, entry)
        write_json(ACTIVE, {'current': name, 'previous': previous})
    except Exception as failure:
        errors = []
        for entry in reversed(attempted):
            try:
                switch(previous, entry)
            except Exception as error:
                errors.append(f'{entry[1]}: {error}')
        if errors:
            raise RuntimeError(f'UI apply failed and rollback needs attention: {errors}') from failure
        raise RuntimeError(f'UI apply failed, prior release restored: {failure}') from failure


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='action', required=True)
    for command in ('seal', 'apply'):
        sub.add_parser(command).add_argument('name')
    sub.add_parser('status')
    sub.add_parser('rollback')
    sub.add_parser('file').add_argument('group', choices=COMPOSE)
    args = parser.parse_args()
    state = active()
    if args.action == 'file':
        if state['current']:
            print(override_path(state['current'], args.group))
        return
    if args.action == 'status':
        print(json.dumps(state))
        return
    validate_fd(int(os.environ[ENV]))
    if args.action == 'seal':
        seal(args.name)
        return
    current = state['current'] or 'classic'
    if args.action == 'rollback':
        if not state['previous']:
            raise ValueError('no previous DEV UI release is recorded')
        requested = state['previous']
    else:
        requested = args.name
    if requested != current:
        apply(requested, current)
    print(json.dumps(active()))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(f'DEV UI release: {error}')
