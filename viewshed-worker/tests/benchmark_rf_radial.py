#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import resource
import statistics
import sys
import time
from unittest import mock

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import worker


def percentile(values, percentile_value):
    return float(np.percentile(np.asarray(values, dtype=np.float64), percentile_value))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rays', type=int, default=360)
    parser.add_argument('--steps', type=int, default=1000)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    rays = max(12, min(360, args.rays))
    steps = max(100, min(1000, args.steps))
    repeats = max(1, min(10, args.repeats))
    wall_budget_s = float(os.environ.get('RF_RADIAL_KERNEL_BUDGET_S', '5'))
    rss_budget_mb = float(os.environ.get('RF_WORKER_RSS_BUDGET_MB', '1024'))

    # The flat synthetic raster isolates the maximum-size RF kernel from tile
    # downloads, GDAL VRT construction, and database/Redis latency.
    elev = np.full((1024, 1024), 120.0, dtype=np.float32)
    gt = (-2.0, 0.003, 0.0, 53.0, 0.0, -0.003)
    search_radius_m = steps * 100.0
    durations_s = []
    boundaries = None
    radius_m = 0.0

    with (
        mock.patch.object(worker, 'RF_N_RAYS', rays),
        mock.patch.object(worker, 'RF_RADIAL_STEP_M', 100.0),
        mock.patch.object(worker, 'RF_MIN_RADIUS_M', search_radius_m),
        mock.patch.object(worker, 'MAX_RADIUS_M', search_radius_m),
        mock.patch.object(worker, 'RF_RADIUS_MULTIPLIER', 1.0),
        mock.patch.object(
            worker,
            'support_penalty_db',
            side_effect=lambda _node_id, lats, _lons: np.zeros(
                len(lats),
                dtype=np.float32,
            ),
        ),
    ):
        for _ in range(repeats):
            started = time.perf_counter()
            boundaries, radius_m = worker.resolve_rf_radial_boundaries(
                'synthetic-benchmark-node',
                51.5,
                -0.5,
                elev,
                gt,
                140.0,
                search_radius_m,
                deadline_monotonic=time.monotonic() + max(30.0, wall_budget_s * 3),
            )
            durations_s.append(time.perf_counter() - started)

    assert boundaries is not None
    expected_points = rays + 1
    points_per_band = {key: len(value) for key, value in boundaries.items()}
    if any(count != expected_points for count in points_per_band.values()):
        raise SystemExit(
            f'RF radial benchmark returned non-closed boundaries: {points_per_band}'
        )

    p50_s = statistics.median(durations_s)
    p95_s = percentile(durations_s, 95)
    rss_mb = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    print(json.dumps({
        'p50_seconds': round(p50_s, 3),
        'p95_seconds': round(p95_s, 3),
        'peak_rss_mb': round(rss_mb, 3),
        'points_per_band': points_per_band,
        'ray_batch_size': worker.RF_PREFIX_RAY_BATCH,
        'rays': rays,
        'repeats': repeats,
        'reported_radius_m': radius_m,
        'rss_budget_mb': rss_budget_mb,
        'steps_per_ray': steps,
        'wall_budget_s': wall_budget_s,
    }, sort_keys=True))

    if p95_s > wall_budget_s:
        raise SystemExit(
            f'RF radial p95 {p95_s:.3f}s exceeded {wall_budget_s:.3f}s'
        )
    if rss_mb > rss_budget_mb:
        raise SystemExit(
            f'RF radial benchmark RSS {rss_mb:.1f}MiB exceeded {rss_budget_mb:.1f}MiB'
        )


if __name__ == '__main__':
    main()
