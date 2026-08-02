#!/usr/bin/env python3
"""Fenced, no-persistence RF coverage rehearsal for real node coordinates.

Input is tab-separated on stdin:

    N<TAB>node_id<TAB>lat<TAB>lon
    L<TAB>node_a_id<TAB>node_b_id<TAB>a_lat<TAB>a_lon<TAB>b_lat<TAB>b_lon

The script reconstructs the worker's read-only support context, calculates
coverage sequentially, validates and immediately discards each geometry, and
prints one aggregate JSON report. It never connects to PostgreSQL or Redis.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
import resource
import statistics
import sys
import time
from dataclasses import dataclass
from typing import TextIO

import numpy as np
from shapely.geometry import shape
from shapely.ops import unary_union

import worker


EXPECTED_BANDS = frozenset(('green', 'amber', 'red'))


@dataclass(frozen=True)
class NodeInput:
    node_id: str
    lat: float
    lon: float


@dataclass(frozen=True)
class LinkInput:
    node_a_id: str
    node_b_id: str
    a_lat: float
    a_lon: float
    b_lat: float
    b_lon: float


def parse_input(stream: TextIO) -> tuple[list[NodeInput], list[LinkInput]]:
    nodes: list[NodeInput] = []
    links: list[LinkInput] = []
    seen_node_ids: set[str] = set()
    for line_number, raw_line in enumerate(stream, start=1):
        line = raw_line.rstrip('\n')
        if not line:
            continue
        fields = line.split('\t')
        kind = fields[0]
        try:
            if kind == 'N' and len(fields) == 4:
                node_id = fields[1].strip()
                lat = float(fields[2])
                lon = float(fields[3])
                if (
                    not node_id
                    or len(node_id) > 128
                    or node_id in seen_node_ids
                    or not worker.is_viewshed_eligible_coordinate(lat, lon)
                ):
                    raise ValueError('invalid or duplicate node row')
                seen_node_ids.add(node_id)
                nodes.append(NodeInput(node_id, lat, lon))
            elif kind == 'L' and len(fields) == 7:
                coordinates = tuple(float(value) for value in fields[3:])
                if (
                    not fields[1]
                    or not fields[2]
                    or not all(math.isfinite(value) for value in coordinates)
                ):
                    raise ValueError('invalid link row')
                links.append(LinkInput(fields[1], fields[2], *coordinates))
            else:
                raise ValueError('unknown row type or field count')
        except (TypeError, ValueError) as exc:
            raise ValueError(f'invalid dry-run input at line {line_number}') from exc
    if not nodes:
        raise ValueError('dry-run input contains no nodes')
    return nodes, links


def install_support_context(nodes: list[NodeInput], links: list[LinkInput]) -> None:
    node_ids = [node.node_id for node in nodes]
    xy = worker.project_xy_km(
        [node.lat for node in nodes],
        [node.lon for node in nodes],
    )
    worker.SUPPORT_CONTEXT['tree'] = worker.cKDTree(xy)
    worker.SUPPORT_CONTEXT['node_ids'] = node_ids
    worker.SUPPORT_CONTEXT['node_index_by_id'] = {
        node_id: index for index, node_id in enumerate(node_ids)
    }

    max_link_km_by_node: dict[str, float] = {}
    for link in links:
        cos_mid = math.cos(math.radians((link.a_lat + link.b_lat) / 2.0))
        distance_km = math.sqrt(
            ((link.a_lat - link.b_lat) * 111.32) ** 2
            + ((link.a_lon - link.b_lon) * 111.32 * cos_mid) ** 2
        )
        if distance_km <= 0:
            continue
        max_link_km_by_node[link.node_a_id] = max(
            max_link_km_by_node.get(link.node_a_id, 0.0),
            distance_km,
        )
        max_link_km_by_node[link.node_b_id] = max(
            max_link_km_by_node.get(link.node_b_id, 0.0),
            distance_km,
        )
    worker.SUPPORT_CONTEXT['max_link_km_by_node'] = max_link_km_by_node
    worker.SUPPORT_CONTEXT['updated_at'] = time.time()


def validate_result(result: worker.Computed) -> list[str]:
    issues: list[str] = []
    if not math.isfinite(result.radius_m) or not 0 < result.radius_m <= worker.MAX_RADIUS_M:
        issues.append('radius')
    if set(result.strength_geoms) != EXPECTED_BANDS:
        issues.append('bands')
        return issues

    try:
        outer = shape(result.geom)
        bands = {name: shape(result.strength_geoms[name]) for name in EXPECTED_BANDS}
    except Exception:
        issues.append('geometry_parse')
        return issues

    geometries = [outer, *bands.values()]
    if any(
        geometry.is_empty
        or not geometry.is_valid
        or geometry.geom_type not in ('Polygon', 'MultiPolygon')
        or not math.isfinite(geometry.area)
        or geometry.area <= 0
        for geometry in geometries
    ):
        issues.append('geometry_validity')
        return issues

    names = tuple(sorted(EXPECTED_BANDS))
    overlap_area = sum(
        bands[names[left]].intersection(bands[names[right]]).area
        for left in range(len(names))
        for right in range(left + 1, len(names))
    )
    outer_area = max(outer.area, 1e-12)
    if overlap_area / outer_area > 1e-6:
        issues.append('band_overlap')

    reconstructed = unary_union(tuple(bands.values()))
    if outer.symmetric_difference(reconstructed).area / outer_area > 0.005:
        issues.append('band_reconstruction')
    return issues


def percentile(values: list[float], value: float) -> float | None:
    if not values:
        return None
    return float(np.percentile(np.asarray(values, dtype=np.float64), value))


def rounded(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(value, digits)


def select_jobs(nodes: list[NodeInput], max_jobs: int | None) -> list[NodeInput]:
    if max_jobs is None or max_jobs >= len(nodes):
        return nodes
    count = max(1, max_jobs)
    indices = np.linspace(0, len(nodes) - 1, count, dtype=np.int64)
    return [nodes[int(index)] for index in indices]


def report_is_complete(report: dict, *, accept_permanent: bool = False) -> bool:
    outcomes = report['outcomes']
    accepted = {'computed'}
    if accept_permanent:
        accepted.add('permanent')
    return (
        not report['validation_issues']
        and set(outcomes).issubset(accepted)
        and sum(outcomes.values()) == report['input_nodes']
    )


def run(
    support_nodes: list[NodeInput],
    links: list[LinkInput],
    progress_every: int,
    *,
    max_jobs: int | None = None,
) -> dict:
    nodes = select_jobs(support_nodes, max_jobs)
    # Aggregate-only output avoids exposing node identifiers or coordinates in
    # rehearsal logs while still retaining progress and failure categories.
    worker.log.disabled = True
    install_support_context(support_nodes, links)
    durations: list[float] = []
    geometry_bytes: list[float] = []
    radii_m: list[float] = []
    stage_totals: collections.Counter[str] = collections.Counter()
    outcomes: collections.Counter[str] = collections.Counter()
    validation_issues: collections.Counter[str] = collections.Counter()
    aggregate_hash = hashlib.sha256()
    started_all = time.perf_counter()

    for index, node in enumerate(nodes, start=1):
        started = time.perf_counter()
        try:
            result = worker.calculate_viewshed(
                node.node_id,
                node.lat,
                node.lon,
                deadline_monotonic=time.monotonic()
                + worker.COVERAGE_JOB_DEADLINE_SECONDS,
            )
            issues = validate_result(result)
            if issues:
                outcomes['invalid'] += 1
                validation_issues.update(issues)
            else:
                outcomes['computed'] += 1
            encoded = json.dumps(
                {
                    'geom': result.geom,
                    'strength_geoms': result.strength_geoms,
                },
                sort_keys=True,
                separators=(',', ':'),
            ).encode()
            aggregate_hash.update(node.node_id.encode())
            aggregate_hash.update(b'\0')
            aggregate_hash.update(encoded)
            durations.append(time.perf_counter() - started)
            geometry_bytes.append(float(len(encoded)))
            radii_m.append(float(result.radius_m))
            for stage, seconds in result.stage_seconds.items():
                stage_totals[stage] += float(seconds)
        except worker.PermanentOutOfScope:
            outcomes['permanent'] += 1
        except Exception as exc:
            outcomes[f'error:{type(exc).__name__}'] += 1

        if index % progress_every == 0 or index == len(nodes):
            print(json.dumps({
                'event': 'progress',
                'processed': index,
                'total': len(nodes),
                'elapsed_seconds': round(time.perf_counter() - started_all, 1),
                'outcomes': dict(sorted(outcomes.items())),
                'peak_rss_mb': round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0,
                    1,
                ),
            }, sort_keys=True), file=sys.stderr, flush=True)

    successful = len(durations)
    elapsed = time.perf_counter() - started_all
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        'model': worker.COVERAGE_MODEL,
        'model_version': worker.COVERAGE_MODEL_VERSION,
        'support_nodes': len(support_nodes),
        'input_nodes': len(nodes),
        'input_viable_links': len(links),
        'support_capped_nodes': len(worker.SUPPORT_CONTEXT['max_link_km_by_node']),
        'outcomes': dict(sorted(outcomes.items())),
        'validation_issues': dict(sorted(validation_issues.items())),
        'elapsed_seconds': round(elapsed, 3),
        'throughput_nodes_per_second': rounded(len(nodes) / elapsed),
        'duration_seconds': {
            'mean': rounded(statistics.mean(durations) if durations else None),
            'p50': rounded(percentile(durations, 50)),
            'p95': rounded(percentile(durations, 95)),
            'p99': rounded(percentile(durations, 99)),
            'max': rounded(max(durations) if durations else None),
        },
        'geometry_bytes': {
            'mean': rounded(statistics.mean(geometry_bytes) if geometry_bytes else None, 1),
            'p95': rounded(percentile(geometry_bytes, 95), 1),
            'max': rounded(max(geometry_bytes) if geometry_bytes else None, 1),
        },
        'radius_km': {
            'mean': rounded(statistics.mean(radii_m) / 1000.0 if radii_m else None),
            'p95': rounded(percentile(radii_m, 95) / 1000.0 if radii_m else None),
            'max': rounded(max(radii_m) / 1000.0 if radii_m else None),
        },
        'mean_stage_seconds': {
            stage: round(seconds / successful, 4)
            for stage, seconds in sorted(stage_totals.items())
        } if successful else {},
        'peak_rss_mb': round(usage.ru_maxrss / 1024.0, 3),
        'cpu_user_seconds': round(usage.ru_utime, 3),
        'cpu_system_seconds': round(usage.ru_stime, 3),
        'aggregate_geometry_sha256': aggregate_hash.hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--expected-nodes', type=int)
    parser.add_argument('--max-jobs', type=int)
    parser.add_argument('--progress-every', type=int, default=50)
    parser.add_argument(
        '--accept-permanent',
        action='store_true',
        help='accept deterministic out-of-land-mask outcomes as complete',
    )
    parser.add_argument('--allow-incomplete', action='store_true')
    args = parser.parse_args()
    nodes, links = parse_input(sys.stdin)
    if args.expected_nodes is not None and len(nodes) != args.expected_nodes:
        raise SystemExit(
            f'expected {args.expected_nodes} nodes but received {len(nodes)}'
        )
    report = run(
        nodes,
        links,
        max(1, args.progress_every),
        max_jobs=args.max_jobs,
    )
    print(json.dumps(report, sort_keys=True), flush=True)
    if not args.allow_incomplete and not report_is_complete(
        report,
        accept_permanent=args.accept_permanent,
    ):
        raise SystemExit(2)


if __name__ == '__main__':
    main()
