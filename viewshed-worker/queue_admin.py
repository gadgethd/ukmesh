#!/usr/bin/env python3
"""Bounded operator tooling for the durable link-v3 queue."""

import argparse
import json
import os
import sys

import redis

import link_queue_v3
import viewshed_queue_v2


def client():
    return redis.Redis.from_url(
        os.environ.get('REDIS_URL', 'redis://redis:6379'),
        password=os.environ.get('REDIS_PASSWORD') or None,
        decode_responses=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest='command', required=True)
    audit = subcommands.add_parser('audit', help='report exact queue counter invariants')
    audit.add_argument('--repair', action='store_true', help='atomically replace drifted counters')
    requeue = subcommands.add_parser('requeue-dead', help='requeue one retained dead job')
    requeue.add_argument('job_id')
    purge = subcommands.add_parser('purge-dead', help='permanently purge one retained dead job')
    purge.add_argument('job_id')
    viewshed_requeue = subcommands.add_parser(
        'requeue-coverage-dead',
        help='requeue one retained dead coverage job',
    )
    viewshed_requeue.add_argument('job_id')
    viewshed_purge = subcommands.add_parser(
        'purge-coverage-dead',
        help='permanently purge one retained dead coverage job',
    )
    viewshed_purge.add_argument('job_id')
    args = parser.parse_args()

    queue = client()
    try:
        if args.command == 'audit':
            report = link_queue_v3.audit_invariants(queue, apply=args.repair)
            print(json.dumps(report, sort_keys=True))
            return 0 if report['consistent'] or args.repair else 2
        if args.command == 'requeue-dead':
            result = link_queue_v3.requeue_dead(queue, args.job_id)
            print(json.dumps({'job_id': args.job_id, 'result': result}, sort_keys=True))
            return 0 if result == 'requeued' else 2
        if args.command == 'purge-dead':
            purged = link_queue_v3.purge_dead(queue, args.job_id)
            print(json.dumps({'job_id': args.job_id, 'purged': purged}, sort_keys=True))
            return 0 if purged else 2
        if args.command == 'requeue-coverage-dead':
            result = viewshed_queue_v2.requeue_dead(
                queue,
                args.job_id,
                viewshed_queue_v2.MAX_JOBS,
                viewshed_queue_v2.MAX_BYTES,
            )
            print(json.dumps({'job_id': args.job_id, 'result': result}, sort_keys=True))
            return 0 if result == 'requeued' else 2
        if args.command == 'purge-coverage-dead':
            purged = viewshed_queue_v2.purge_dead(queue, args.job_id)
            print(json.dumps({'job_id': args.job_id, 'purged': purged}, sort_keys=True))
            return 0 if purged else 2
    finally:
        queue.close()
    return 2


if __name__ == '__main__':
    sys.exit(main())
