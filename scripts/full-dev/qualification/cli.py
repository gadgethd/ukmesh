#!/usr/bin/env python3
"""Full-stack qualification CLI. Missing evidence never becomes a pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from backups import backup_originals
from common import owned_run, safe_error, utc_now, write_json


def main() -> int:
    from report import ACCEPTANCE_PROFILES
    parser = argparse.ArgumentParser(description=__doc__, epilog=
        "all refreshes service inventory and validates the complete collected run; "
        "explicit stages own traffic, fixture preparation and timed measurements.")
    parser.add_argument("action", choices=("inventory", "backup", "smoke", "access", "reads", "operations", "parity", "core-projection", "traffic", "soak", "recovery", "sample", "report", "all"))
    parser.add_argument("--run", help="existing evidence directory under plans/lean-runtime/qualification")
    parser.add_argument("--seconds", type=int, default=2400)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--warmup", type=int, default=120)
    parser.add_argument("--reader-role", default="corescope_ro")
    parser.add_argument("--reader-schema", default="corescope_public")
    parser.add_argument("--label", choices=("baseline", "candidate"), default="candidate")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--paired", action="store_true", help="run the fixed matched reads or synchronized operations")
    parser.add_argument("--count", type=int, default=72000)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--traffic-interval-ms", type=int, default=1000)
    parser.add_argument("--health-code")
    parser.add_argument("--native-only", action="store_true", help="qualify paired native reads separately from fixed-demand stress")
    parser.add_argument("--reuse-prepared", action="store_true", help="reuse an already converged owned soak fixture")
    parser.add_argument("--acceptance-profile", choices=ACCEPTANCE_PROFILES, default="full-stack",
                        help="mqtt-first requires matched native reads; fixed-demand and mixed-read latency remain advisory")
    args = parser.parse_args()
    if args.native_only and (args.action != "reads" or not args.paired):
        parser.error("--native-only requires reads --paired")
    if args.reuse_prepared and args.action != "soak":
        parser.error("--reuse-prepared requires soak")
    run = owned_run(args.run, create=args.action == "backup" and not args.run)
    try:
        if args.action == "backup":
            result = backup_originals(run)
        elif args.action in ("inventory", "smoke"):
            from probes import inventory, smoke
            result = inventory(run) if args.action == "inventory" else smoke(run)
        elif args.action == "sample":
            from resources import sample
            result = sample(run, seconds=args.seconds, interval=args.interval, warmup=args.warmup)
        elif args.action == "access":
            from access import access
            result = access(run, reader_role=args.reader_role, reader_schema=args.reader_schema)
        elif args.action == "reads":
            from benchmarks import reads, reads_pair
            from common import REPO
            if args.paired and args.source_root:
                raise ValueError('paired reads select sealed/current sources explicitly')
            if args.acceptance_profile == 'mqtt-first' and not args.paired:
                raise ValueError('mqtt-first reads require --paired')
            result = (reads_pair(run, native_only=args.native_only or args.acceptance_profile == 'mqtt-first') if args.paired else
                      reads(run, label=args.label, source_root=args.source_root or REPO / "backend/src"))
        elif args.action == "operations":
            from operations import operations,operations_pair
            if args.paired and args.source_root:
                raise ValueError('paired operations select both sealed/current source roots explicitly')
            result = operations_pair(run) if args.paired else operations(run, label=args.label, source_root=args.source_root)
        elif args.action == "parity":
            from benchmarks import parity
            if not args.source_root:
                raise ValueError("--source-root must identify the frozen baseline backend source")
            result = parity(run, source_root=args.source_root)
        elif args.action == "core-projection":
            from core_projection import core_projection
            result = core_projection(run)
        elif args.action == "traffic":
            from traffic import traffic
            result = traffic(run, cycles=args.cycles, interval_ms=args.traffic_interval_ms, health_code=args.health_code)
        elif args.action == "soak":
            from soak import soak
            result = soak(run, count=args.count, health_code=args.health_code, reuse_prepared=args.reuse_prepared,
                          acceptance_profile=args.acceptance_profile)
        elif args.action == "recovery":
            from recovery import recovery
            result = recovery(run)
        elif args.action == "report":
            from report import report
            result = report(run, acceptance_profile=args.acceptance_profile)
        else:
            from probes import inventory
            from report import report
            inventory(run)
            result = report(run, acceptance_profile=args.acceptance_profile)
        print(json.dumps({"action": args.action, "run": str(run), "passed": result["passed"]}))
        return 0 if result["passed"] else 1
    except Exception as error:
        failure = {"action": args.action, "at": utc_now(), "passed": False, "error": safe_error(error)}
        write_json(run / f"{args.action}-failure.json", failure)
        print(json.dumps({**failure, "run": str(run)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
