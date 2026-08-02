#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import resource
import statistics
import sys
import time

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from rf.loss import compute_path_loss_from_profile, compute_prefix_path_losses


def percentile(values, percentile_value):
    return float(np.percentile(np.asarray(values, dtype=np.float64), percentile_value))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rays', type=int, default=24)
    parser.add_argument('--steps', type=int, default=1000)
    args = parser.parse_args()
    rays = max(4, min(360, args.rays))
    steps = max(100, min(1000, args.steps))
    p95_budget_ms = float(os.environ.get('RF_PREFIX_P95_BUDGET_MS', '250'))
    rss_budget_mb = float(os.environ.get('RF_WORKER_RSS_BUDGET_MB', '1024'))

    rng = np.random.default_rng(20260729)
    distances = np.arange(1, steps + 1, dtype=np.float64) * 100.0
    profiles = []
    for ray in range(rays):
        base = 120 + 45 * np.sin(distances / (1800 + ray * 17))
        noise = rng.integers(-8, 9, size=steps)
        profiles.append((base + noise).astype(np.float64))

    profiles_array = np.asarray(profiles, dtype=np.float64)
    ray_batch_size = max(1, min(64, int(os.environ.get('RF_PREFIX_RAY_BATCH', '8'))))
    durations_ms = []
    batched_result = None
    for _ in range(5):
        started = time.perf_counter()
        batched_result = compute_prefix_path_losses(
            distances,
            profiles_array,
            140.0,
            profiles_array + 2.0,
            endpoint_batch_size=64,
            ray_batch_size=ray_batch_size,
        )
        durations_ms.append((time.perf_counter() - started) * 1000 / rays)
    assert batched_result is not None
    results = batched_result.path_loss_db

    legacy_durations_ms = []
    max_parity_error_db = 0.0
    for ray_index in range(min(3, rays)):
        heights = profiles[ray_index]
        started = time.perf_counter()
        expected = np.asarray([
            compute_path_loss_from_profile(
                distances[:index + 1],
                heights[:index + 1],
                140.0,
                heights[index] + 2.0,
                include_profile=False,
            ).path_loss_db
            for index in range(steps)
        ])
        legacy_durations_ms.append((time.perf_counter() - started) * 1000)
        finite = np.isfinite(expected) & np.isfinite(results[ray_index])
        if np.any(finite):
            max_parity_error_db = max(
                max_parity_error_db,
                float(np.max(np.abs(expected[finite] - results[ray_index][finite]))),
            )
        if not np.array_equal(np.isinf(expected), np.isinf(results[ray_index])):
            raise SystemExit('RF benchmark parity failed: infinity positions differ')

    p50_ms = statistics.median(durations_ms)
    p95_ms = percentile(durations_ms, 95)
    rss_mb = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    comparable_vector_ms = statistics.median(durations_ms)
    legacy_p50_ms = statistics.median(legacy_durations_ms)
    report = {
        'rays': rays,
        'ray_batch_size': ray_batch_size,
        'steps_per_ray': steps,
        'batched_p50_ms_total': round(p50_ms * rays, 3),
        'vectorized_p50_ms_per_ray': round(p50_ms, 3),
        'vectorized_p95_ms_per_ray': round(p95_ms, 3),
        'legacy_p50_ms_per_ray': round(legacy_p50_ms, 3),
        'median_speedup': round(legacy_p50_ms / max(0.001, comparable_vector_ms), 2),
        'peak_rss_mb': round(rss_mb, 3),
        'max_parity_error_db': max_parity_error_db,
        'p95_budget_ms': p95_budget_ms,
        'rss_budget_mb': rss_budget_mb,
    }
    print(json.dumps(report, sort_keys=True))

    if max_parity_error_db > 1e-9:
        raise SystemExit('RF prefix parity exceeded 1e-9 dB')
    if p95_ms > p95_budget_ms:
        raise SystemExit(
            f'RF prefix p95 {p95_ms:.1f}ms exceeded {p95_budget_ms:.1f}ms'
        )
    if rss_mb > rss_budget_mb:
        raise SystemExit(
            f'RF benchmark RSS {rss_mb:.1f}MiB exceeded {rss_budget_mb:.1f}MiB'
        )


if __name__ == '__main__':
    main()
