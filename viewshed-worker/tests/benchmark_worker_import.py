#!/usr/bin/env python3
"""Fresh-process import/RSS measurements in the real GDAL image; no connections."""

import argparse
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time


def probe():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    started = time.perf_counter()
    import worker_runtime  # noqa: F401 — measure the actual process import graph
    from osgeo import gdal

    elapsed = time.perf_counter() - started
    status = dict(
        line.strip().split(":", 1)
        for line in Path("/proc/self/status").read_text().splitlines()
        if ":" in line
    )
    return {
        "import_seconds": elapsed,
        "rss_kib": int(status["VmRSS"].split()[0]),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "python": sys.version,
        "gdal": gdal.VersionInfo(),
        "mode": os.environ.get("WORKER_MODE", "all"),
        "loaded_modules": {
            name: name in sys.modules
            for name in ("osgeo.gdal", "shapely", "scipy.ndimage", "scipy.spatial")
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, choices=range(1, 11), default=3)
    parser.add_argument("--mode", choices=("link", "viewshed", "all"), default="link")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        print(json.dumps(probe()))
        return

    def fresh_process():
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child"],
            env={**os.environ, "WORKER_MODE": args.mode},
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        return json.loads(result.stdout)

    warmup = fresh_process()
    results = [fresh_process() for _ in range(args.runs)]
    print(
        json.dumps(
            {
                "measurement": "worker import only; one discarded warm-up, fresh Python process per run",
                "warmup": warmup,
                "results": results,
                "median_import_seconds": statistics.median(
                    r["import_seconds"] for r in results
                ),
                "median_rss_kib": statistics.median(r["rss_kib"] for r in results),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
