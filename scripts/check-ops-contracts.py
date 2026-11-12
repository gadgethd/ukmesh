#!/usr/bin/env python3
"""Validate semantic release coverage and worker scrape ownership without running services."""
import argparse
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', type=Path)
    parser.add_argument('--monitoring', action='store_true')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    load = lambda path: yaml.safe_load(path.read_text())
    if args.release:
        names = release_components(load(args.release))
        print(f'Release build and signing components match ({len(names)} images).')
    if args.monitoring:
        targets = worker_scrapes(load(args.root / 'docker-compose.yml'), load(args.root / 'logging/prometheus.yml'))
        print(f'Worker scrape ownership verified ({len(targets)} targets).')
    if not args.release and not args.monitoring:
        parser.error('select --release or --monitoring')


if __name__ == '__main__':
    main()
