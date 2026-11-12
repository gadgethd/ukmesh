#!/usr/bin/env python3
"""Validate semantic release coverage and worker scrape ownership without running services."""
import argparse
import json
import re
import shlex
from pathlib import Path
import yaml


def release_components(document):
    jobs = document['jobs']
    result = {}
    for job in ('image', 'sign'):
        entries = jobs[job]['strategy']['matrix']['include']
        names = [entry['component'] for entry in entries]
        if not names or len(names) != len(set(names)):
            raise ValueError(f'{job} must have unique, nonempty components')
        result[job] = set(names)
    if result['image'] != result['sign']:
        raise ValueError('release build/sign components differ')
    return result['image']


def worker_scrapes(compose, prometheus):
    services = compose['services']
    jobs = [job for job in prometheus['scrape_configs'] if job.get('job_name') == 'meshcore-workers']
    if len(jobs) != 1:
        raise ValueError('one meshcore-workers scrape job is required')
    targets, roles = set(), set()
    for config in jobs[0]['static_configs']:
        role = config.get('labels', {}).get('role')
        if not role or role in roles:
            raise ValueError('worker scrape roles must be nonempty and unique')
        roles.add(role)
        for target in config['targets']:
            service, port = target.rsplit(':', 1)
            if service not in services or not port.isdigit() or not 1 <= int(port) <= 65535:
                raise ValueError(f'worker scrape target is not a Compose service: {target}')
            if target in targets:
                raise ValueError(f'duplicate worker scrape target: {target}')
            targets.add(target)
    if not targets:
        raise ValueError('worker scrape inventory is empty')
    return targets



def link_queue_defaults(contract, compose, example):
    """Expose the shared definition link without changing either runtime parser."""
    documented = {}
    for line in example.splitlines():
        match = re.fullmatch(r"\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)\s*=\s*(.*?)\s*", line)
        if match:
            documented.setdefault(match[1], []).append(match[2])
    limits = contract['limits']
    for spec in limits.values():
        name, expected = spec['env'], spec['default']
        for service in ('backend', 'link-worker'):
            environment = compose['services'][service].get('environment', {})
            if isinstance(environment, list):
                environment = dict(entry.split('=', 1) if '=' in entry else (entry, None)
                                   for entry in environment)
            value = environment.get(name)
            expression = re.fullmatch(r"\$\{([A-Z_][A-Z0-9_]*):-([+-]?[0-9]+)\}", str(value).strip())
            if not expression or expression[1] != name or int(expression[2]) != expected:
                raise ValueError(f'{service}.{name} must default from {name} to {expected}')
        values = documented.get(name, [])
        tokens = shlex.split(values[0], comments=True) if len(values) == 1 else []
        if (len(tokens) != 1 or re.fullmatch(r'[+-]?[0-9]+', tokens[0]) is None
                or int(tokens[0]) != expected):
            raise ValueError(f'.env.example must document {name}={expected} exactly once')
    return {spec['env'] for spec in limits.values()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', type=Path)
    parser.add_argument('--monitoring', action='store_true')
    parser.add_argument('--queue-defaults', action='store_true')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    load = lambda path: yaml.safe_load(path.read_text())
    if args.release:
        names = release_components(load(args.release))
        print(f'Release build and signing components match ({len(names)} images).')
    if args.monitoring:
        targets = worker_scrapes(load(args.root / 'docker-compose.yml'), load(args.root / 'logging/prometheus.yml'))
        print(f'Worker scrape ownership verified ({len(targets)} targets).')
    if args.queue_defaults:
        names = link_queue_defaults(
            json.loads((args.root / 'contracts/link-v3.json').read_text()),
            load(args.root / 'docker-compose.yml'),
            (args.root / '.env.example').read_text(),
        )
        print(f'Link queue defaults verified ({len(names)} limits across backend, link-worker and .env.example).')
    if not args.release and not args.monitoring and not args.queue_defaults:
        parser.error('select --release, --monitoring or --queue-defaults')


if __name__ == '__main__':
    main()
