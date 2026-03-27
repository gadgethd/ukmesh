"""
MeshCore Analytics — Viewshed Worker
Consumes jobs from Redis, downloads SRTM1 tiles, computes a raycasting
viewshed, clips to the UK mainland, stores the result polygon in
node_coverage, then notifies the frontend.
"""

import argparse
import json
import logging
import math
import multiprocessing
import os
import subprocess
import tempfile
import time
import datetime as dt
from pathlib import Path
from typing import Optional

import numpy as np
import psycopg2
from scipy.ndimage import minimum_filter as _min_filter
from scipy.spatial import cKDTree
import redis
from osgeo import gdal
from shapely.geometry import mapping, Polygon as ShapelyPolygon
from rf.config import (
    ANTENNA_HEIGHT_M,
    CALIBRATION_EXTRA_MARGIN_DB,
    CALIBRATION_MAX_THRESHOLD_BOOST_DB,
    CALIBRATION_MIN_LINKS,
    CALIBRATION_MIN_OBSERVED_COUNT,
    CALIBRATION_PERCENTILE,
    CALIBRATION_REFRESH_S,
    COVERAGE_TARGET_HEIGHT_M,
    DEFAULT_USABLE_PATH_LOSS_DB,
    FREQ_MHZ,
    K_FACTOR,
    LAMBDA_M,
    LINK_LOS_MAX_V,
    PROFILE_STEP_M,
    RADIO_NEIGHBOR_SYNC,
    RF_CALIBRATION,
    R_EARTH_M,
    current_signal_thresholds_db,
    current_usable_path_loss_db,
    radio_snr_band,
)
from rf.loss import compute_path_loss, compute_path_loss_from_profile
from rf.terrain import (
    build_link_vrt,
    download_tile,
    load_uk_mainland,
    radio_horizon_m,
    sample_elevation,
    tiles_for_radius,
)

gdal.UseExceptions()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SRTM_DIR     = Path(os.environ.get('SRTM_DIR', '/data/srtm'))
REDIS_URL    = os.environ.get('REDIS_URL', 'redis://redis:6379')
DATABASE_URL = os.environ.get('DATABASE_URL')
RADIO_BOT_URL = os.environ.get('RADIO_BOT_URL', 'http://meshcore-radio-bot:3011')
WORKER_MODE  = os.environ.get('WORKER_MODE', 'all').lower()
COVERAGE_MODEL = os.environ.get('COVERAGE_MODEL', 'rf_radial_100m').lower()
DB_APPLICATION_NAME = os.environ.get(
    'DATABASE_APPLICATION_NAME',
    f'meshcore-{WORKER_MODE if WORKER_MODE in ("viewshed", "link") else "viewshed-worker"}',
)

JOB_QUEUE      = 'meshcore:viewshed_jobs'
JOB_PENDING_SET = 'meshcore:viewshed_pending'
LINK_JOB_QUEUE = 'meshcore:link_jobs'
LIVE_CHANNEL   = 'meshcore:live'

COVERAGE_MODEL_VERSION = int(os.environ.get(
    'COVERAGE_MODEL_VERSION',
    '5' if COVERAGE_MODEL == 'rf_radial_100m' else '2',
))
MIN_LINK_OBSERVATIONS = 5  # must match backend db/index.ts
PREFIX_AMBIGUITY_RADIUS_KM = 45.0  # only penalize same-prefix ambiguity when nodes are realistically in range
MAX_RADIUS_M     = 100_000  # absolute cap on viewshed radius (m)
SIMPLIFY_DEG     = 0.001    # Douglas-Peucker tolerance (~100 m)
N_RAYS           = 720      # number of radial rays cast from the observer
STEP_M           = 50.0     # ray step size in metres
ANGLE_EPS        = 1e-9     # numerical tolerance for horizon comparisons
RF_RADIAL_STEP_M = 100.0    # radial search precision for RF coverage mode
RF_N_RAYS        = 360      # 1-degree azimuth resolution keeps RF mode tractable
RF_RADIUS_MULTIPLIER = 1.35 # search beyond geometric horizon to allow limited diffraction gain
RF_MIN_RADIUS_M  = 20_000   # avoid under-searching low-elevation repeaters
RF_SOURCE_LINK_RADIUS_MULTIPLIER = float(os.environ.get('RF_SOURCE_LINK_RADIUS_MULTIPLIER', '1.25'))
DEFAULT_PHYSICAL_LINK_RADIUS_KM = float(os.environ.get('DEFAULT_PHYSICAL_LINK_RADIUS_KM', '60'))
MIN_PHYSICAL_LINK_RADIUS_KM = float(os.environ.get('MIN_PHYSICAL_LINK_RADIUS_KM', '20'))
MAX_PHYSICAL_LINK_RADIUS_KM = float(os.environ.get('MAX_PHYSICAL_LINK_RADIUS_KM', '100'))
SUPPORT_REFRESH_S = int(os.environ.get('COVERAGE_SUPPORT_REFRESH_S', '900'))
SUPPORT_NEARBY_REPEATER_KM = float(os.environ.get('COVERAGE_SUPPORT_NEARBY_REPEATER_KM', '12'))
SUPPORT_PENALTY_PER_KM_DB = float(os.environ.get('COVERAGE_SUPPORT_PENALTY_PER_KM_DB', '0.6'))
SUPPORT_MAX_PENALTY_DB = float(os.environ.get('COVERAGE_SUPPORT_MAX_PENALTY_DB', '14'))
SUPPORT_PROJECTION_LAT = float(os.environ.get('COVERAGE_SUPPORT_PROJECTION_LAT', '54.0'))

RADIO_NEIGHBOR_REFRESH_S = int(os.environ.get('RADIO_NEIGHBOR_REFRESH_S', '300'))
RADIO_NEIGHBOR_MAX_AGE_HOURS = float(os.environ.get('RADIO_NEIGHBOR_MAX_AGE_HOURS', '72'))

# Radio horizon parameters
# K-factor, wavelength, RF calibration state, and path-loss thresholds now
# live in rf.config so the RF model can be reused outside the worker loop.

SUPPORT_CONTEXT = {
    'tree': None,
    'node_ids': [],
    'node_index_by_id': {},
    'max_link_km_by_node': {},
    'updated_at': 0.0,
}

UK_LAT_MIN = 49.5
UK_LAT_MAX = 61.5
UK_LON_MIN = -8.5
UK_LON_MAX = 2.5


def is_viewshed_eligible_coordinate(lat: float, lon: float) -> bool:
    if not math.isfinite(lat) or not math.isfinite(lon):
        return False
    if abs(lat) < 1e-9 and abs(lon) < 1e-9:
        return False
    return UK_LAT_MIN <= lat <= UK_LAT_MAX and UK_LON_MIN <= lon <= UK_LON_MAX

def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    if values.size < 1:
        raise ValueError('No values provided')
    order = np.argsort(values)
    v = values[order]
    w = weights[order]
    cumulative = np.cumsum(w)
    target = float(np.clip(q, 0.0, 1.0)) * cumulative[-1]
    idx = int(np.searchsorted(cumulative, target, side='left'))
    idx = max(0, min(idx, len(v) - 1))
    return float(v[idx])


def project_xy_km(latitudes, longitudes) -> np.ndarray:
    lats = np.asarray(latitudes, dtype=np.float64)
    lons = np.asarray(longitudes, dtype=np.float64)
    cos_ref = math.cos(math.radians(SUPPORT_PROJECTION_LAT))
    return np.column_stack((lons * 111.32 * cos_ref, lats * 111.32))


def node_dist_km(a: dict, b: dict) -> float:
    cos_m = math.cos(math.radians((a['lat'] + b['lat']) / 2))
    return math.sqrt(
        ((a['lat'] - b['lat']) * 111.32) ** 2 +
        ((a['lon'] - b['lon']) * 111.32 * cos_m) ** 2
    )


def physical_candidate_radius_km(radius_m: Optional[float]) -> float:
    derived = (radius_m / 1000.0) * RF_SOURCE_LINK_RADIUS_MULTIPLIER if radius_m is not None else DEFAULT_PHYSICAL_LINK_RADIUS_KM
    return min(MAX_PHYSICAL_LINK_RADIUS_KM, max(MIN_PHYSICAL_LINK_RADIUS_KM, derived))


def refresh_rf_calibration(db, force: bool = False) -> None:
    now = time.time()
    if not force and now - float(RF_CALIBRATION['updated_at']) < CALIBRATION_REFRESH_S:
        return

    with db.cursor() as cur:
        cur.execute(
            '''
            WITH link_packet_stats AS (
              SELECT
                LEAST(p.rx_node_id, p.src_node_id)    AS node_a_id,
                GREATEST(p.rx_node_id, p.src_node_id) AS node_b_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.snr) AS median_snr_db,
                COUNT(*)                               AS packet_count
              FROM packets p
              WHERE p.snr IS NOT NULL
                AND p.rx_node_id IS NOT NULL
                AND p.src_node_id IS NOT NULL
                AND p.rx_node_id <> p.src_node_id
                AND p.hop_count = 0
                AND p.time > NOW() - (%s * INTERVAL \'1 hour\')
              GROUP BY node_a_id, node_b_id
              HAVING COUNT(*) >= %s
            )
            SELECT
              nl.itm_path_loss_db,
              lps.packet_count    AS neighbor_report_count,
              lps.median_snr_db   AS neighbor_best_snr_db
            FROM node_links nl
            JOIN link_packet_stats lps
              ON lps.node_a_id = nl.node_a_id
             AND lps.node_b_id = nl.node_b_id
            WHERE nl.itm_path_loss_db IS NOT NULL
              AND nl.force_viable = false
            ''',
            (RADIO_NEIGHBOR_MAX_AGE_HOURS, CALIBRATION_MIN_OBSERVED_COUNT),
        )
        rows = cur.fetchall()

    if len(rows) < CALIBRATION_MIN_LINKS:
        RF_CALIBRATION['usable_path_loss_db'] = DEFAULT_USABLE_PATH_LOSS_DB
        RF_CALIBRATION['signal_thresholds_db'] = {
            'green': max(116.0, DEFAULT_USABLE_PATH_LOSS_DB - 16.0),
            'amber': max(124.0, DEFAULT_USABLE_PATH_LOSS_DB - 8.0),
            'red': DEFAULT_USABLE_PATH_LOSS_DB,
        }
        RF_CALIBRATION['samples'] = len(rows)
        RF_CALIBRATION['updated_at'] = now
        log.info(
            'RF calibration: insufficient observed links '
            f'({len(rows)}/{CALIBRATION_MIN_LINKS}) — using default threshold {DEFAULT_USABLE_PATH_LOSS_DB:.1f} dB'
        )
        return

    losses = np.asarray([float(row[0]) for row in rows], dtype=np.float64)
    counts = np.asarray([max(1.0, float(row[1])) for row in rows], dtype=np.float64)
    snrs = np.asarray([float(row[2]) if row[2] is not None else -6.0 for row in rows], dtype=np.float64)
    snr_weights = np.asarray([
        1.75 if radio_snr_band(snr) == 'strong'
        else 1.35 if radio_snr_band(snr) == 'medium'
        else 1.10 if radio_snr_band(snr) == 'weak'
        else 0.75
        for snr in snrs
    ], dtype=np.float64)
    weights = np.clip(np.sqrt(counts) * snr_weights, 0.5, 24.0)

    green_threshold = weighted_quantile(losses, weights, 0.35)
    amber_threshold = weighted_quantile(losses, weights, 0.65)
    observed_tail = weighted_quantile(losses, weights, CALIBRATION_PERCENTILE)
    usable_threshold = observed_tail + CALIBRATION_EXTRA_MARGIN_DB
    usable_threshold = min(DEFAULT_USABLE_PATH_LOSS_DB + CALIBRATION_MAX_THRESHOLD_BOOST_DB, usable_threshold)
    usable_threshold = max(110.0, usable_threshold)
    green_threshold = min(green_threshold, usable_threshold)
    amber_threshold = min(max(green_threshold, amber_threshold), usable_threshold)

    RF_CALIBRATION['usable_path_loss_db'] = round(float(usable_threshold), 2)
    RF_CALIBRATION['signal_thresholds_db'] = {
        'green': round(float(green_threshold), 2),
        'amber': round(float(amber_threshold), 2),
        'red': round(float(usable_threshold), 2),
    }
    RF_CALIBRATION['samples'] = len(rows)
    RF_CALIBRATION['updated_at'] = now
    log.info(
        'RF calibration: '
        f'samples={len(rows)}, q35={green_threshold:.1f} dB, q65={amber_threshold:.1f} dB, '
        f'p{int(CALIBRATION_PERCENTILE * 100)}={observed_tail:.1f} dB, '
        f'usable={RF_CALIBRATION["usable_path_loss_db"]:.1f} dB, '
        f'green={RF_CALIBRATION["signal_thresholds_db"]["green"]:.1f}, '
        f'amber={RF_CALIBRATION["signal_thresholds_db"]["amber"]:.1f}'
    )


def refresh_radio_neighbor_reports(db, r_client, force: bool = False) -> None:
    now = time.time()
    if not force and now - float(RADIO_NEIGHBOR_SYNC['updated_at']) < RADIO_NEIGHBOR_REFRESH_S:
        return

    try:
        response = requests.get(f'{RADIO_BOT_URL}/state', timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log.warning(f'Radio neighbour sync failed: {exc}')
        RADIO_NEIGHBOR_SYNC['updated_at'] = now
        return

    monitors = payload.get('monitors') if isinstance(payload, dict) else None
    if not isinstance(monitors, list):
        RADIO_NEIGHBOR_SYNC['updated_at'] = now
        return

    imported = 0
    queued = 0
    with db.cursor() as cur:
        for monitor in monitors:
            if not isinstance(monitor, dict):
                continue
            reporter_id = str(monitor.get('fullPublicKey') or monitor.get('targetHex') or '').strip().lower()
            if len(reporter_id) != 64:
                continue

            neighbours_at_raw = monitor.get('lastNeighboursAt') or monitor.get('lastSuccessAt')
            base_seen_at: Optional[dt.datetime] = None
            if neighbours_at_raw:
                try:
                    base_seen_at = dt.datetime.fromisoformat(str(neighbours_at_raw).replace('Z', '+00:00'))
                except Exception:
                    base_seen_at = None

            last_neighbours = monitor.get('lastNeighbours')
            if not isinstance(last_neighbours, list):
                continue

            for neighbour in last_neighbours:
                if not isinstance(neighbour, dict):
                    continue
                peer_id = str(neighbour.get('fullPublicKey') or '').strip().lower()
                if len(peer_id) != 64 or peer_id == reporter_id:
                    continue
                snr_db = neighbour.get('snrDb')
                if not isinstance(snr_db, (int, float)):
                    continue

                seen_at = base_seen_at
                heard_seconds_ago = neighbour.get('heardSecondsAgo')
                if seen_at is not None and isinstance(heard_seconds_ago, (int, float)):
                    seen_at = seen_at - dt.timedelta(seconds=float(heard_seconds_ago))
                if seen_at is None:
                    seen_at = dt.datetime.now(dt.timezone.utc)

                a_id, b_id = sorted((reporter_id, peer_id))
                cur.execute(
                    '''
                    INSERT INTO node_links (
                      node_a_id, node_b_id, observed_count, last_observed, count_a_to_b, count_b_to_a
                    )
                    VALUES (%s, %s, 0, NOW(), 0, 0)
                    ON CONFLICT (node_a_id, node_b_id) DO NOTHING
                    ''',
                    (a_id, b_id),
                )
                cur.execute(
                    '''
                    INSERT INTO node_link_radio_reports (
                      node_a_id, node_b_id, reporter_node_id, peer_node_id,
                      last_snr_db, best_snr_db, last_seen, sample_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                    ON CONFLICT (node_a_id, node_b_id, reporter_node_id) DO UPDATE
                    SET peer_node_id = EXCLUDED.peer_node_id,
                        last_snr_db = CASE
                          WHEN EXCLUDED.last_seen >= node_link_radio_reports.last_seen
                            THEN EXCLUDED.last_snr_db
                          ELSE node_link_radio_reports.last_snr_db
                        END,
                        best_snr_db = GREATEST(
                          COALESCE(node_link_radio_reports.best_snr_db, EXCLUDED.best_snr_db),
                          EXCLUDED.best_snr_db
                        ),
                        last_seen = GREATEST(node_link_radio_reports.last_seen, EXCLUDED.last_seen),
                        sample_count = CASE
                          WHEN EXCLUDED.last_seen > node_link_radio_reports.last_seen
                            THEN node_link_radio_reports.sample_count + 1
                          ELSE node_link_radio_reports.sample_count
                        END
                    ''',
                    (a_id, b_id, reporter_id, peer_id, float(snr_db), float(snr_db), seen_at),
                )
                r_client.rpush(LINK_JOB_QUEUE, json.dumps({
                    'type': 'physical_pair',
                    'node_a_id': a_id,
                    'node_b_id': b_id,
                }))
                imported += 1
                queued += 1
    db.commit()
    RADIO_NEIGHBOR_SYNC['updated_at'] = now
    if imported > 0:
        log.info(f'Radio neighbour sync: imported {imported} report(s), queued {queued} physical link job(s)')


def refresh_support_context(db, force: bool = False) -> None:
    now = time.time()
    if not force and now - float(SUPPORT_CONTEXT['updated_at']) < SUPPORT_REFRESH_S:
        return

    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT node_id, lat, lon
            FROM nodes
            WHERE lat IS NOT NULL
              AND lon IS NOT NULL
              AND lat BETWEEN %s AND %s
              AND lon BETWEEN %s AND %s
              AND NOT (ABS(lat) < 1e-9 AND ABS(lon) < 1e-9)
              AND (name IS NULL OR name NOT LIKE %s)
              AND (role IS NULL OR role = 2)
            ''',
            (UK_LAT_MIN, UK_LAT_MAX, UK_LON_MIN, UK_LON_MAX, '%🚫%',),
        )
        repeater_rows = cur.fetchall()
        cur.execute(
            '''
            SELECT nl.node_a_id, nl.node_b_id,
                   na.lat, na.lon, nb.lat, nb.lon
            FROM node_links nl
            JOIN nodes na ON na.node_id = nl.node_a_id
            JOIN nodes nb ON nb.node_id = nl.node_b_id
            WHERE na.lat IS NOT NULL
              AND na.lon IS NOT NULL
              AND nb.lat IS NOT NULL
              AND nb.lon IS NOT NULL
              AND (nl.itm_viable = true OR nl.force_viable = true)
            '''
        )
        link_rows = cur.fetchall()

    node_ids = [row[0] for row in repeater_rows]
    xy = project_xy_km([row[1] for row in repeater_rows], [row[2] for row in repeater_rows]) if repeater_rows else np.empty((0, 2))
    SUPPORT_CONTEXT['tree'] = cKDTree(xy) if len(node_ids) > 0 else None
    SUPPORT_CONTEXT['node_ids'] = node_ids
    SUPPORT_CONTEXT['node_index_by_id'] = {node_id: idx for idx, node_id in enumerate(node_ids)}

    max_link_km_by_node: dict[str, float] = {}
    for a_id, b_id, a_lat, a_lon, b_lat, b_lon in link_rows:
      cos_mid = math.cos(math.radians((a_lat + b_lat) / 2))
      dist_km = math.sqrt(
          ((a_lat - b_lat) * 111.32) ** 2 +
          ((a_lon - b_lon) * 111.32 * cos_mid) ** 2
      )
      if dist_km <= 0:
          continue
      max_link_km_by_node[a_id] = max(max_link_km_by_node.get(a_id, 0.0), dist_km)
      max_link_km_by_node[b_id] = max(max_link_km_by_node.get(b_id, 0.0), dist_km)
    SUPPORT_CONTEXT['max_link_km_by_node'] = max_link_km_by_node
    SUPPORT_CONTEXT['updated_at'] = now
    log.info(
        f'Mesh support context: repeaters={len(node_ids)}, '
        f'link-capped nodes={len(max_link_km_by_node)}'
    )


def source_support_radius_m(node_id: str, fallback_radius_m: float) -> float:
    max_link_km = SUPPORT_CONTEXT['max_link_km_by_node'].get(node_id)
    if not max_link_km:
        return fallback_radius_m
    return min(
        fallback_radius_m,
        max(RF_MIN_RADIUS_M, max_link_km * 1000.0 * RF_SOURCE_LINK_RADIUS_MULTIPLIER),
    )


def support_penalty_db(source_node_id: str, sample_lats: np.ndarray, sample_lons: np.ndarray) -> np.ndarray:
    tree: Optional[cKDTree] = SUPPORT_CONTEXT['tree']
    node_ids: list[str] = SUPPORT_CONTEXT['node_ids']
    source_index = SUPPORT_CONTEXT['node_index_by_id'].get(source_node_id)
    if tree is None or len(node_ids) < 1:
        return np.zeros(sample_lats.shape[0], dtype=np.float32)

    points_xy = project_xy_km(sample_lats, sample_lons)
    k = 2 if source_index is not None and len(node_ids) > 1 else 1
    distances, indices = tree.query(points_xy, k=k)

    if k == 1:
        nearest_km = np.asarray(distances, dtype=np.float32)
    else:
        d = np.asarray(distances, dtype=np.float32)
        i = np.asarray(indices, dtype=np.int32)
        primary_is_source = i[:, 0] == source_index
        nearest_km = np.where(primary_is_source, d[:, 1], d[:, 0]).astype(np.float32)

    penalty = np.maximum(0.0, nearest_km - SUPPORT_NEARBY_REPEATER_KM) * SUPPORT_PENALTY_PER_KM_DB
    return np.clip(penalty, 0.0, SUPPORT_MAX_PENALTY_DB).astype(np.float32)

def resolve_rf_radial_boundaries(node_id: str,
                                 lat: float,
                                 lon: float,
                                 elev: np.ndarray,
                                 gt: tuple[float, float, float, float, float, float],
                                 observer_h: float,
                                 base_radius_m: float) -> tuple[dict[str, list[tuple[float, float]]], float]:
    search_radius_m = min(MAX_RADIUS_M, max(base_radius_m * RF_RADIUS_MULTIPLIER, RF_MIN_RADIUS_M))
    n_rows, n_cols = elev.shape
    dpmlat = 1.0 / 111_320.0
    dpmlon = 1.0 / (111_320.0 * math.cos(math.radians(lat)))
    ds_arr = np.arange(RF_RADIAL_STEP_M, search_radius_m + RF_RADIAL_STEP_M, RF_RADIAL_STEP_M, dtype=np.float32)
    thetas = np.linspace(0.0, 2.0 * math.pi, RF_N_RAYS, endpoint=False, dtype=np.float32)
    cos_t = np.cos(thetas)
    sin_t = np.sin(thetas)

    signal_thresholds = current_signal_thresholds_db()
    boundaries: dict[str, list[tuple[float, float]]] = {key: [] for key in signal_thresholds}
    max_reached = 0.0

    for theta_idx in range(RF_N_RAYS):
        pt_lats = lat + sin_t[theta_idx] * ds_arr * dpmlat
        pt_lons = lon + cos_t[theta_idx] * ds_arr * dpmlon
        pxs = np.clip(((pt_lons - gt[0]) / gt[1]).astype(np.int32), 0, n_cols - 1)
        pys = np.clip(((pt_lats - gt[3]) / gt[5]).astype(np.int32), 0, n_rows - 1)
        hs = elev[pys, pxs].astype(np.float32)

        losses: list[float] = []
        for idx in range(len(ds_arr)):
            dists = ds_arr[:idx + 1]
            heights = hs[:idx + 1]
            h_rx = float(heights[-1]) + COVERAGE_TARGET_HEIGHT_M
            loss, _viable = compute_path_loss_from_profile(dists, heights, observer_h, h_rx)
            losses.append(loss)
        losses_arr = np.asarray(losses, dtype=np.float32)
        effective_losses = losses_arr + support_penalty_db(node_id, pt_lats, pt_lons)

        for band, threshold in signal_thresholds.items():
            passing = np.where(effective_losses <= threshold)[0]
            if passing.size < 1:
                end_dist = float(ds_arr[0])
            else:
                end_dist = float(ds_arr[int(passing[-1])])
            if band == 'red':
                max_reached = max(max_reached, end_dist)
            boundaries[band].append((
                lon + float(cos_t[theta_idx]) * end_dist * dpmlon,
                lat + float(sin_t[theta_idx]) * end_dist * dpmlat,
            ))

    for band_boundary in boundaries.values():
        if band_boundary:
            band_boundary.append(band_boundary[0])
    return boundaries, max_reached


def clip_and_simplify_polygon(poly) -> Optional[dict]:
    if poly.is_empty:
        return None
    if not poly.is_valid:
        poly = poly.buffer(0)
    if UK_MAINLAND is not None:
        poly = poly.intersection(UK_MAINLAND)
        if poly.is_empty:
            return None
    result = poly.simplify(SIMPLIFY_DEG, preserve_topology=True)
    if result.is_empty or result.geom_type not in ('Polygon', 'MultiPolygon'):
        return None
    return mapping(result)


def build_exclusive_strength_geoms(band_polys: dict[str, ShapelyPolygon]) -> dict[str, dict]:
    """Convert nested strength polygons into exclusive green/amber/red areas.

    The strongest band should own the fill for a location. Without this, the
    frontend ends up stacking green over amber over red and the center reads as
    muddy yellow instead of a clean strength gradient.
    """
    exclusive: dict[str, dict] = {}

    green_poly = band_polys.get('green')
    if green_poly is not None and not green_poly.is_empty:
        clipped_green = clip_and_simplify_polygon(green_poly)
        if clipped_green is not None:
            exclusive['green'] = clipped_green

    amber_poly = band_polys.get('amber')
    if amber_poly is not None and not amber_poly.is_empty:
        amber_only = amber_poly
        if green_poly is not None and not green_poly.is_empty:
            amber_only = amber_only.difference(green_poly)
        clipped_amber = clip_and_simplify_polygon(amber_only)
        if clipped_amber is not None:
            exclusive['amber'] = clipped_amber

    red_poly = band_polys.get('red')
    if red_poly is not None and not red_poly.is_empty:
        red_only = red_poly
        if amber_poly is not None and not amber_poly.is_empty:
            red_only = red_only.difference(amber_poly)
        elif green_poly is not None and not green_poly.is_empty:
            red_only = red_only.difference(green_poly)
        clipped_red = clip_and_simplify_polygon(red_only)
        if clipped_red is not None:
            exclusive['red'] = clipped_red

    return exclusive


UK_MAINLAND = load_uk_mainland(Path(__file__).parent, log)


# ── Viewshed calculation ──────────────────────────────────────────────────────

def calculate_viewshed(node_id: str, lat: float, lon: float, antenna_height_m: float = ANTENNA_HEIGHT_M) -> Optional[tuple[dict, dict[str, dict], float, float]]:
    if not is_viewshed_eligible_coordinate(lat, lon):
        log.info(f'Skipping viewshed for {node_id[:12]}… outside UK coverage bounds at ({lat:.4f}, {lon:.4f})')
        return None
    with tempfile.TemporaryDirectory() as tmp:
        # 1. Download the observer's own tile and sample terrain elevation.
        #    This single tile is sufficient to determine node height; we need
        #    it before we know how far to reach for surrounding tiles.
        obs_tile = (math.floor(lat), math.floor(lon))
        obs_path = download_tile(SRTM_DIR, *obs_tile, log)
        if not obs_path:
            log.error(f'No SRTM tile for observer at {node_id} ({lat:.4f}, {lon:.4f})')
            return None

        obs_vrt = f'{tmp}/observer.vrt'
        subprocess.run(
            ['gdalbuildvrt', obs_vrt, str(obs_path)],
            capture_output=True, text=True,
        )
        elevation_m = sample_elevation(obs_vrt, lat, lon)

        # 2. Radio-horizon radius: node ASL + per-node antenna height.
        effective_height_m = elevation_m + antenna_height_m
        radius_m = min(radio_horizon_m(effective_height_m), MAX_RADIUS_M)
        radius_m = source_support_radius_m(node_id, radius_m)
        log.info(
            f'  {node_id[:12]}…: elevation={elevation_m:.0f} m ASL, '
            f'antenna={effective_height_m:.0f} m, horizon={radius_m / 1000:.1f} km'
        )

        # 3. Download all tiles covering the computed horizon radius
        needed = tiles_for_radius(lat, lon, radius_m)
        paths  = [p for t in needed if (p := download_tile(SRTM_DIR, *t, log))]
        if not paths:
            log.error(f'No SRTM tiles for {node_id} ({lat:.4f}, {lon:.4f})')
            return None

        # 4. Merge tiles into a single VRT
        vrt = f'{tmp}/input.vrt'
        r   = subprocess.run(
            ['gdalbuildvrt', vrt] + [str(p) for p in paths],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            log.error(f'gdalbuildvrt failed: {r.stderr}')
            return None

        # 5. Read entire elevation raster into memory once.
        #    NODATA ocean pixels (INT16 -32768) are clamped to 0 — treated as sea level.
        ds   = gdal.Open(vrt)
        gt   = ds.GetGeoTransform()   # (x_origin, px_lon, 0, y_origin, 0, px_lat)
        elev = np.clip(
            ds.GetRasterBand(1).ReadAsArray().astype(np.float32),
            0, None,
        )
        n_rows, n_cols = elev.shape
        ds = None

        # 5b. Approximate DTM from SRTM DSM via spatial minimum filter.
        #     SRTM is a Digital Surface Model — building heights corrupt urban
        #     areas causing raycasting to terminate within metres of the observer.
        #     A 9-pixel (~270 m for SRTM1 at 30 m/px) minimum filter strips
        #     building-height spikes while preserving genuine terrain features
        #     (hills, ridges) whose footprints are wider than ~270 m.
        elev = _min_filter(elev, size=9)

        # 5c. Re-sample observer elevation from the DTM-approximated raster.
        #     This corrects the radio-horizon radius when SRTM reads building tops.
        obs_px = int(np.clip((lon - gt[0]) / gt[1], 0, n_cols - 1))
        obs_py = int(np.clip((lat - gt[3]) / gt[5], 0, n_rows - 1))
        dtm_elev = float(elev[obs_py, obs_px])
        # Guard against coastal bleed-in: min filter near shoreline may return 0
        # (ocean NODATA) even for land pixels.  Fall back to raw SRTM in that case.
        if dtm_elev > 0.0 or elevation_m <= 0.0:
            elevation_m = dtm_elev
        effective_height_m = elevation_m + antenna_height_m
        radius_m = min(radio_horizon_m(effective_height_m), MAX_RADIUS_M)
        radius_m = source_support_radius_m(node_id, radius_m)
        log.info(
            f'  {node_id[:12]}… DTM elevation={elevation_m:.0f} m ASL, '
            f'horizon={radius_m / 1000:.1f} km'
        )

        observer_h = elevation_m + antenna_height_m
        strength_geoms: dict[str, dict] = {}
        if COVERAGE_MODEL == 'terrain_los':
            # Vectorised raycasting terrain line-of-sight model.
            dpmlat = 1.0 / 111_320.0                                       # deg/m northward
            dpmlon = 1.0 / (111_320.0 * math.cos(math.radians(lat)))       # deg/m eastward
            R_eff_2 = 2.0 * K_FACTOR * R_EARTH_M                          # 2kR curvature denom

            n_steps = max(1, int(radius_m / STEP_M))
            ds_arr  = np.linspace(STEP_M, radius_m, n_steps)    # (M,) distances in metres
            thetas  = np.linspace(0.0, 2.0 * math.pi, N_RAYS, endpoint=False)   # (N,) angles

            # Ray sample coordinates: (N, M)
            sin_t   = np.sin(thetas)[:, None]    # (N, 1)
            cos_t   = np.cos(thetas)[:, None]    # (N, 1)
            pt_lats = lat + sin_t * ds_arr[None, :] * dpmlat   # (N, M)
            pt_lons = lon + cos_t * ds_arr[None, :] * dpmlon   # (N, M)

            # Pixel indices — clamped to raster bounds (N, M)
            pxs = np.clip(((pt_lons - gt[0]) / gt[1]).astype(np.int32), 0, n_cols - 1)
            pys = np.clip(((pt_lats - gt[3]) / gt[5]).astype(np.int32), 0, n_rows - 1)

            # Terrain heights at each ray step: (N, M)
            hs = elev[pys, pxs]

            # Angles with Earth-curvature correction: (N, M)
            curvature = ds_arr[None, :] ** 2 / R_eff_2
            terrain_angles = (hs - observer_h - curvature) / ds_arr[None, :]
            target_angles = ((hs + COVERAGE_TARGET_HEIGHT_M) - observer_h - curvature) / ds_arr[None, :]

            running_max = np.maximum.accumulate(terrain_angles, axis=1)
            prev_max  = np.concatenate([np.full((N_RAYS, 1), -np.inf), running_max[:, :-1]], axis=1)
            in_shadow = target_angles + ANGLE_EPS < prev_max

            has_shadow  = in_shadow.any(axis=1)
            first_shad  = np.where(has_shadow, in_shadow.argmax(axis=1), n_steps)
            last_js     = np.clip(first_shad - 1, 0, n_steps - 1)
            last_ds     = ds_arr[last_js]

            lons_b = lon + np.cos(thetas) * last_ds * dpmlon
            lats_b = lat + np.sin(thetas) * last_ds * dpmlat
            boundary = list(zip(lons_b.tolist(), lats_b.tolist()))
            boundary.append(boundary[0])
            poly = ShapelyPolygon(boundary)
            clipped = clip_and_simplify_polygon(poly)
            if clipped is None:
                log.warning(f'{node_id}: degenerate geometry after clipping — skipping')
                return None
            geom = clipped
            strength_geoms = {'green': geom}
        elif COVERAGE_MODEL == 'rf_radial_100m':
            band_boundaries, radius_m = resolve_rf_radial_boundaries(node_id, lat, lon, elev, gt, observer_h, radius_m)
            raw_band_polys: dict[str, ShapelyPolygon] = {}
            for band, band_boundary in band_boundaries.items():
                if len(band_boundary) < 4:
                    continue
                band_poly = ShapelyPolygon(band_boundary)
                if not band_poly.is_empty:
                    raw_band_polys[band] = band_poly
            strength_geoms = build_exclusive_strength_geoms(raw_band_polys)
            geom = clip_and_simplify_polygon(raw_band_polys.get('red')) if raw_band_polys.get('red') is not None else None
            if geom is None:
                log.warning(f'{node_id}: degenerate RF coverage geometry after clipping — skipping')
                return None
        else:
            raise ValueError(f'Unknown COVERAGE_MODEL={COVERAGE_MODEL}')
        return geom, strength_geoms, radius_m, elevation_m

# ── DB helpers ────────────────────────────────────────────────────────────────

def already_calculated(db, node_id: str) -> bool:
    with db.cursor() as cur:
        cur.execute(
            'SELECT 1 FROM node_coverage WHERE node_id = %s AND model_version >= %s',
            (node_id, COVERAGE_MODEL_VERSION),
        )
        return cur.fetchone() is not None

def store_coverage(db, node_id: str, geom: dict, strength_geoms: dict[str, dict], radius_m: float, elevation_m: float, antenna_height_m: float = ANTENNA_HEIGHT_M):
    with db.cursor() as cur:
        cur.execute(
            '''INSERT INTO node_coverage (node_id, geom, strength_geoms, antenna_height_m, radius_m, model_version)
               VALUES (%s, %s::jsonb, %s::jsonb, %s, %s, %s)
               ON CONFLICT (node_id) DO UPDATE
                 SET geom = EXCLUDED.geom,
                     strength_geoms = EXCLUDED.strength_geoms,
                     antenna_height_m = EXCLUDED.antenna_height_m,
                     radius_m = EXCLUDED.radius_m,
                     model_version = EXCLUDED.model_version,
                     calculated_at = NOW()''',
            (node_id, json.dumps(geom), json.dumps(strength_geoms), antenna_height_m, radius_m, COVERAGE_MODEL_VERSION),
        )
        cur.execute(
            'UPDATE nodes SET elevation_m = %s WHERE node_id = %s',
            (round(elevation_m, 1), node_id),
        )
    db.commit()

def backfill_elevations(db):
    """For nodes that already have a computed viewshed but no elevation stored,
    reverse-compute elevation from radius_m: h = r² / (2·k·R) - antenna_height."""
    with db.cursor() as cur:
        cur.execute('''
            SELECT nc.node_id, nc.radius_m
            FROM node_coverage nc
            JOIN nodes n ON n.node_id = nc.node_id
            WHERE n.elevation_m IS NULL AND nc.radius_m IS NOT NULL
        ''')
        rows = cur.fetchall()
    if not rows:
        return
    log.info(f'Backfilling elevation for {len(rows)} node(s) from stored radius_m')
    with db.cursor() as cur:
        for node_id, radius_m in rows:
            elevation_m = max(0.0, (radius_m ** 2) / (2 * K_FACTOR * R_EARTH_M) - ANTENNA_HEIGHT_M)
            cur.execute(
                'UPDATE nodes SET elevation_m = %s WHERE node_id = %s',
                (round(elevation_m, 1), node_id),
            )
            log.info(f'  {node_id[:12]}…: elevation={elevation_m:.0f} m ASL (from radius {radius_m/1000:.1f} km)')
    db.commit()

def load_positioned_repeaters(db) -> dict[str, dict]:
    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT n.node_id, n.lat, n.lon, n.elevation_m, n.name, n.role, nc.radius_m, nc.antenna_height_m
            FROM nodes n
            LEFT JOIN node_coverage nc ON nc.node_id = n.node_id
            WHERE n.lat IS NOT NULL
              AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
            ''',
            (UK_LAT_MIN, UK_LAT_MAX, UK_LON_MIN, UK_LON_MAX, '%🚫%'),
        )
        return {
            row[0]: {
                'lat': row[1],
                'lon': row[2],
                'elevation_m': row[3],
                'name': row[4],
                'role': row[5],
                'radius_m': row[6],
                'antenna_height_m': row[7] if row[7] is not None else ANTENNA_HEIGHT_M,
            }
            for row in cur.fetchall()
        }


def publish_link_update(r_client, a_id: str, b_id: str, obs_count: int, path_loss_db: Optional[float],
                        itm_viable: Optional[bool], count_a_to_b: int, count_b_to_a: int,
                        multibyte_obs: int) -> None:
    r_client.publish(LIVE_CHANNEL, json.dumps({
        'type': 'link_update',
        'data': {
            'node_a_id': a_id,
            'node_b_id': b_id,
            'observed_count': obs_count,
            'itm_path_loss_db': path_loss_db,
            'itm_viable': itm_viable,
            'count_a_to_b': count_a_to_b,
            'count_b_to_a': count_b_to_a,
            'multibyte_observed_count': multibyte_obs,
        },
        'ts': int(time.time() * 1000),
    }))


def upsert_link_pair(db, a_id: str, b_id: str, inc_atob: int, inc_btoa: int, inc_multibyte: int):
    obs_delta = inc_atob + inc_btoa
    with db.cursor() as cur:
        cur.execute(
            '''INSERT INTO node_links
                   (node_a_id, node_b_id, observed_count, last_observed,
                    count_a_to_b, count_b_to_a, multibyte_observed_count)
               VALUES (%s, %s, %s, NOW(), %s, %s, %s)
               ON CONFLICT (node_a_id, node_b_id) DO UPDATE
                 SET observed_count = node_links.observed_count + %s,
                     last_observed = CASE WHEN %s > 0 THEN NOW() ELSE node_links.last_observed END,
                     count_a_to_b = node_links.count_a_to_b + %s,
                     count_b_to_a = node_links.count_b_to_a + %s,
                     multibyte_observed_count = node_links.multibyte_observed_count + %s
               RETURNING observed_count, itm_computed_at, itm_path_loss_db, itm_viable,
                         count_a_to_b, count_b_to_a, multibyte_observed_count''',
            (
                a_id, b_id, obs_delta, inc_atob, inc_btoa, inc_multibyte,
                obs_delta, obs_delta, inc_atob, inc_btoa, inc_multibyte,
            ),
        )
        return cur.fetchone()


def ensure_physical_link_metrics(db, a_id: str, a: dict, b_id: str, b: dict):
    row = upsert_link_pair(db, a_id, b_id, 0, 0, 0)
    obs_count = row[0] if row else 0
    itm_computed = row[1] if row else None
    path_loss_db = row[2] if row else None
    itm_viable = row[3] if row else None
    count_a_to_b = row[4] if row else 0
    count_b_to_a = row[5] if row else 0
    multibyte_obs = row[6] if row else 0

    missing_endpoint_elev = a.get('elevation_m') is None or b.get('elevation_m') is None
    if itm_computed is not None and not missing_endpoint_elev:
        return obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs

    with tempfile.TemporaryDirectory() as tmp:
        vrt = build_link_vrt(a['lat'], a['lon'], b['lat'], b['lon'], tmp, SRTM_DIR)
        if not vrt:
            return obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs
        try:
            a_elev = a.get('elevation_m')
            b_elev = b.get('elevation_m')
            if a_elev is None:
                a_elev = sample_elevation(vrt, a['lat'], a['lon'])
                a['elevation_m'] = a_elev
                with db.cursor() as cur:
                    cur.execute(
                        'UPDATE nodes SET elevation_m = %s WHERE node_id = %s AND elevation_m IS NULL',
                        (round(a_elev, 1), a_id),
                    )
            if b_elev is None:
                b_elev = sample_elevation(vrt, b['lat'], b['lon'])
                b['elevation_m'] = b_elev
                with db.cursor() as cur:
                    cur.execute(
                        'UPDATE nodes SET elevation_m = %s WHERE node_id = %s AND elevation_m IS NULL',
                        (round(b_elev, 1), b_id),
                    )
            path_loss_db, itm_viable = compute_path_loss(
                a['lat'], a['lon'], a_elev,
                b['lat'], b['lon'], b_elev,
                vrt,
                antenna_height_m_tx=a.get('antenna_height_m', ANTENNA_HEIGHT_M),
                antenna_height_m_rx=b.get('antenna_height_m', ANTENNA_HEIGHT_M),
            )
            path_loss_db = round(path_loss_db, 1)
            with db.cursor() as cur:
                cur.execute(
                    '''UPDATE node_links
                       SET itm_path_loss_db = %s,
                           itm_viable = %s,
                           itm_computed_at = NOW()
                       WHERE node_a_id = %s AND node_b_id = %s''',
                    (path_loss_db, itm_viable, a_id, b_id),
                )
            log.info(
                f'Link {a_id[:8]}…↔{b_id[:8]}…: '
                f'{path_loss_db:.1f} dB {"✓" if itm_viable else "✗"} '
                f'(obs={obs_count})'
            )
        except Exception as exc:
            log.warning(f'Path loss computation failed: {exc}')

    return obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs


def process_physical_link_job(db, r_client, job: dict):
    node_a_id = job.get('node_a_id')
    node_b_id = job.get('node_b_id')
    if not node_a_id or not node_b_id or node_a_id == node_b_id:
        return

    nodes = load_positioned_repeaters(db)
    a = nodes.get(node_a_id)
    b = nodes.get(node_b_id)
    if not a or not b:
        return

    obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs = ensure_physical_link_metrics(
        db, node_a_id, a, node_b_id, b,
    )
    publish_link_update(r_client, node_a_id, node_b_id, obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs)


def process_observation_link_job(db, r_client, job: dict):
    """Resolve multibyte packet paths and annotate already-physical links."""
    rx_node_id = job.get('rx_node_id')
    src_node_id = job.get('src_node_id')
    path_hashes = job.get('path_hashes', [])
    path_hash_size_bytes = int(job.get('path_hash_size_bytes') or 1)

    if not rx_node_id or not path_hashes or path_hash_size_bytes <= 1:
        return

    all_nodes = load_positioned_repeaters(db)

    with db.cursor() as cur:
        cur.execute(
            'SELECT node_a_id, node_b_id FROM node_links WHERE itm_viable = true OR force_viable = true',
        )
        physical_pairs: set[tuple[str, str]] = {
            (min(a, b), max(a, b)) for a, b in cur.fetchall()
        }

    def physical_link(a_id: str, b_id: str) -> bool:
        return (min(a_id, b_id), max(a_id, b_id)) in physical_pairs

    rx = all_nodes.get(rx_node_id)
    if not rx:
        return

    def normalize_path_hash(value) -> str:
        return str(value or '').strip().upper()

    def node_matches_path_hash(node_id: str, path_hash: str) -> bool:
        return bool(path_hash) and node_id.upper().startswith(path_hash)

    def local_prefix_ambiguity_penalty(path_hash: str, target_id: str, target_node: dict, anchor_node: dict,
                                       pool: list[tuple[str, dict]]) -> float:
        target_dist = node_dist_km(target_node, anchor_node)
        raw = 0.0
        for cand_id, cand_node in pool:
            if cand_id == target_id:
                continue
            if not node_matches_path_hash(cand_id, path_hash):
                continue
            cand_dist = node_dist_km(cand_node, anchor_node)
            if cand_dist > PREFIX_AMBIGUITY_RADIUS_KM:
                continue
            dist_similarity = max(0.0, 1.0 - abs(cand_dist - target_dist) / PREFIX_AMBIGUITY_RADIUS_KM)
            proximity = max(0.0, 1.0 - cand_dist / PREFIX_AMBIGUITY_RADIUS_KM)
            raw += dist_similarity * proximity
        return min(0.24, raw * 0.12)

    resolved: list[tuple[str, dict]] = []
    prev_id = rx_node_id
    prev = rx
    visited = {rx_node_id}

    for raw_hash in reversed(path_hashes):
        path_hash = normalize_path_hash(raw_hash)
        if not path_hash:
            continue
        candidates = [
            (nid, nd) for nid, nd in all_nodes.items()
            if node_matches_path_hash(nid, path_hash)
            and nid not in visited
        ]
        if not candidates:
            continue

        best_id = None
        best = None
        best_score = float('-inf')
        for nid, nd in candidates:
            confirmed_bonus = 2.5 if physical_link(nid, prev_id) else 0.0
            distance_score = -node_dist_km(nd, prev) / 12.0
            ambiguity_penalty = local_prefix_ambiguity_penalty(path_hash, nid, nd, prev, candidates)
            score = confirmed_bonus + distance_score - ambiguity_penalty
            if score > best_score:
                best_score = score
                best_id, best = nid, nd

        if best_id is None or best is None:
            continue

        resolved.insert(0, (best_id, best))
        visited.add(best_id)
        prev_id = best_id
        prev = best

    full: list[tuple[str, dict]] = []
    if src_node_id and src_node_id in all_nodes:
        full.append((src_node_id, all_nodes[src_node_id]))
    full.extend(resolved)
    full.append((rx_node_id, rx))
    if len(full) < 2:
        return

    for i in range(len(full) - 1):
        src_id, src = full[i]
        dst_id, dst = full[i + 1]
        if src_id == dst_id:
            continue
        if src['lat'] is None or src['lon'] is None or dst['lat'] is None or dst['lon'] is None:
            continue

        if src_id < dst_id:
            a_id, a, b_id, b = src_id, src, dst_id, dst
            inc_atob, inc_btoa = 1, 0
        else:
            a_id, a, b_id, b = dst_id, dst, src_id, src
            inc_atob, inc_btoa = 0, 1

        if not physical_link(a_id, b_id):
            continue

        row = upsert_link_pair(db, a_id, b_id, inc_atob, inc_btoa, 1)
        obs_count = row[0] if row else 1
        path_loss_db = row[2] if row else None
        itm_viable = row[3] if row else None
        count_a_to_b = row[4] if row else inc_atob
        count_b_to_a = row[5] if row else inc_btoa
        multibyte_obs = row[6] if row else 1

        if itm_viable is None:
            obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs = ensure_physical_link_metrics(
                db, a_id, a, b_id, b,
            )
        publish_link_update(r_client, a_id, b_id, obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs)


def process_link_job(db, r_client, job: dict):
    job_type = str(job.get('type') or 'observe').strip().lower()
    if job_type == 'physical_pair':
        process_physical_link_job(db, r_client, job)
        return
    process_observation_link_job(db, r_client, job)


def enqueue_physical_link_jobs_for_node(db, r_client, node_id: str, lat: float, lon: float, radius_m: Optional[float]) -> int:
    origin = {'lat': lat, 'lon': lon, 'radius_m': radius_m}
    origin_radius_km = physical_candidate_radius_km(radius_m)
    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT n.node_id, n.lat, n.lon, nc.radius_m
            FROM nodes n
            LEFT JOIN node_coverage nc ON nc.node_id = n.node_id
            WHERE n.node_id <> %s
              AND n.lat IS NOT NULL
              AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
            ''',
            (node_id, UK_LAT_MIN, UK_LAT_MAX, UK_LON_MIN, UK_LON_MAX, '%🚫%'),
        )
        rows = cur.fetchall()

    queued = 0
    for peer_id, peer_lat, peer_lon, peer_radius_m in rows:
        peer = {'lat': peer_lat, 'lon': peer_lon, 'radius_m': peer_radius_m}
        if node_dist_km(origin, peer) > max(origin_radius_km, physical_candidate_radius_km(peer_radius_m)):
            continue
        [a_id, b_id] = sorted((node_id, peer_id))
        r_client.lpush(LINK_JOB_QUEUE, json.dumps({
            'type': 'physical_pair',
            'node_a_id': a_id,
            'node_b_id': b_id,
        }))
        queued += 1
    return queued


def enqueue_uncovered(db, r_client):
    """On startup, queue all nodes that have a position but no coverage yet."""
    # Remove any coverage that was previously computed for hidden or non-repeater nodes.
    with db.cursor() as cur:
        cur.execute("""
            DELETE FROM node_coverage WHERE node_id IN (
                SELECT node_id FROM nodes
                WHERE name LIKE '%🚫%' OR (role IS NOT NULL AND role != 2)
            )
        """)
    db.commit()

    with db.cursor() as cur:
        cur.execute('''
            SELECT n.node_id, n.lat, n.lon
            FROM nodes n
            LEFT JOIN node_coverage nc ON n.node_id = nc.node_id
            WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (nc.node_id IS NULL OR nc.model_version < %s)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
        ''', (UK_LAT_MIN, UK_LAT_MAX, UK_LON_MIN, UK_LON_MAX, COVERAGE_MODEL_VERSION, '%🚫%',))
        rows = cur.fetchall()
    if rows:
        log.info(f'Queuing {len(rows)} existing node(s) for viewshed calculation (model v{COVERAGE_MODEL_VERSION})')
        for node_id, lat, lon in rows:
            if r_client.sadd(JOB_PENDING_SET, node_id):
                r_client.lpush(JOB_QUEUE, json.dumps({'node_id': node_id, 'lat': lat, 'lon': lon}))

def rebuild_pending_viewshed_set(r_client):
    """Rebuild the pending-node set from the current queue contents on startup."""
    r_client.delete(JOB_PENDING_SET)
    raw_jobs = r_client.lrange(JOB_QUEUE, 0, -1)
    node_ids = []
    for raw in raw_jobs:
        try:
            job = json.loads(raw)
        except Exception:
            continue
        node_id = str(job.get('node_id') or '').strip()
        if node_id:
            node_ids.append(node_id)
    if node_ids:
        r_client.sadd(JOB_PENDING_SET, *node_ids)
    log.info(f'Rebuilt viewshed pending set from queue ({len(set(node_ids))} unique node(s))')

# ── Job processor ─────────────────────────────────────────────────────────────

def process_job(db, r_client, job: dict):
    node_id = job['node_id']
    lat     = float(job['lat'])
    lon     = float(job['lon'])
    try:
        if not is_viewshed_eligible_coordinate(lat, lon):
            log.info(f'Skipping out-of-UK viewshed job {node_id[:12]}… at ({lat:.4f}, {lon:.4f})')
            # Write placeholder so this node isn't re-queued on restart
            with db.cursor() as cur:
                cur.execute(
                    '''INSERT INTO node_coverage (node_id, geom, strength_geoms, antenna_height_m, radius_m, model_version)
                       VALUES (%s, '{"type":"Polygon","coordinates":[]}'::jsonb, NULL, %s, NULL, %s)
                       ON CONFLICT (node_id) DO UPDATE
                         SET geom = '{"type":"Polygon","coordinates":[]}'::jsonb,
                             strength_geoms = NULL,
                             model_version = EXCLUDED.model_version,
                             calculated_at = NOW()''',
                    (node_id, ANTENNA_HEIGHT_M, COVERAGE_MODEL_VERSION),
                )
            db.commit()
            return
        # Skip hidden (🚫) or non-repeater nodes regardless of how the job arrived
        with db.cursor() as cur:
            cur.execute(
                '''SELECT n.name, n.role, nc.antenna_height_m
                   FROM nodes n
                   LEFT JOIN node_coverage nc ON nc.node_id = n.node_id
                   WHERE n.node_id = %s''',
                (node_id,),
            )
            row = cur.fetchone()
        node_antenna_height_m = ANTENNA_HEIGHT_M
        if row:
            name, role, stored_antenna_h = row
            if stored_antenna_h is not None:
                node_antenna_height_m = float(stored_antenna_h)
            if name and '🚫' in name:
                log.info(f'Skipping hidden node {node_id[:12]}…')
                return
            if role is not None and role != 2:
                log.info(f'Skipping non-repeater {node_id[:12]}… (role={role})')
                return

        if already_calculated(db, node_id):
            log.info(f'Coverage already exists for {node_id[:12]}…, skipping')
            return

        log.info(f'Viewshed: {node_id[:12]}… at ({lat:.4f}, {lon:.4f})')
        t0     = time.time()
        result = calculate_viewshed(node_id, lat, lon, antenna_height_m=node_antenna_height_m)
        if result is None:
            # Write an empty-geom placeholder so this node isn't re-queued on restart
            with db.cursor() as cur:
                cur.execute(
                    '''INSERT INTO node_coverage (node_id, geom, strength_geoms, antenna_height_m, radius_m, model_version)
                       VALUES (%s, '{"type":"Polygon","coordinates":[]}'::jsonb, NULL, %s, NULL, %s)
                       ON CONFLICT (node_id) DO UPDATE
                         SET geom = '{"type":"Polygon","coordinates":[]}'::jsonb,
                             strength_geoms = NULL,
                             model_version = EXCLUDED.model_version,
                             calculated_at = NOW()''',
                    (node_id, node_antenna_height_m, COVERAGE_MODEL_VERSION),
                )
            db.commit()
            return

        geom, strength_geoms, radius_m, elevation_m = result
        store_coverage(db, node_id, geom, strength_geoms, radius_m, elevation_m, antenna_height_m=node_antenna_height_m)
        if WORKER_MODE in ('all', 'link'):
            queued_links = enqueue_physical_link_jobs_for_node(db, r_client, node_id, lat, lon, radius_m)
            if queued_links > 0:
                log.info(f'Queued {queued_links} physical link job(s) for {node_id[:12]}…')
        log.info(f'Done in {time.time() - t0:.1f}s — notifying frontend')

        r_client.publish(LIVE_CHANNEL, json.dumps({
            'type': 'coverage_update',
            'data': {'node_id': node_id, 'geom': geom, 'strength_geoms': strength_geoms},
            'ts':   int(time.time() * 1000),
        }))
        r_client.publish(LIVE_CHANNEL, json.dumps({
            'type': 'node_upsert',
            'data': {'node_id': node_id, 'elevation_m': round(elevation_m, 1)},
            'ts':   int(time.time() * 1000),
        }))
    finally:
        r_client.srem(JOB_PENDING_SET, node_id)

# ── Main loop ─────────────────────────────────────────────────────────────────

def wait_for_db() -> psycopg2.extensions.connection:
    for attempt in range(30):
        try:
            conn = psycopg2.connect(DATABASE_URL, application_name=DB_APPLICATION_NAME)
            # autocommit=True prevents SELECT queries from holding open transactions
            # that would block schema DDL (CREATE EXTENSION etc.) on app restart.
            conn.autocommit = True
            conn.cursor().execute('SELECT 1')
            return conn
        except Exception:
            log.info(f'Waiting for DB… (attempt {attempt + 1}/30)')
            time.sleep(3)
    raise RuntimeError('DB never became ready')

def worker_loop():
    """Single worker process: owns its own DB and Redis connections."""
    name     = multiprocessing.current_process().name
    db       = wait_for_db()
    r_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    sync_radio_neighbors = name in ('MainProcess', 'Worker-1')
    if sync_radio_neighbors and WORKER_MODE in ('all', 'link'):
        refresh_radio_neighbor_reports(db, r_client, force=True)
    refresh_rf_calibration(db, force=True)
    refresh_support_context(db, force=True)
    log.info(f'{name} ready')

    while True:
        try:
            if sync_radio_neighbors and WORKER_MODE in ('all', 'link'):
                refresh_radio_neighbor_reports(db, r_client)
            refresh_rf_calibration(db)
            refresh_support_context(db)
            if WORKER_MODE in ('all', 'link'):
                # Drain pending link jobs first (fast) before blocking
                while True:
                    raw = r_client.rpop(LINK_JOB_QUEUE)
                    if raw is None:
                        break
                    process_link_job(db, r_client, json.loads(raw))

            if WORKER_MODE == 'viewshed':
                wait_queues = [JOB_QUEUE]
            elif WORKER_MODE == 'link':
                wait_queues = [LINK_JOB_QUEUE]
            else:
                wait_queues = [JOB_QUEUE, LINK_JOB_QUEUE]

            item = r_client.brpop(wait_queues, timeout=30)
            if item is None:
                continue
            queue_name, raw = item
            if queue_name == LINK_JOB_QUEUE:
                process_link_job(db, r_client, json.loads(raw))
            else:
                process_job(db, r_client, json.loads(raw))
        except psycopg2.OperationalError:
            log.warning(f'{name}: DB connection lost — reconnecting')
            db = wait_for_db()
        except Exception as exc:
            log.error(f'{name}: job error: {exc}', exc_info=True)


def resolve_node_ref(db, ref: str) -> dict:
    ref = str(ref or '').strip()
    if not ref:
        raise ValueError('Empty node reference')

    with db.cursor() as cur:
        if len(ref) == 64 and all(ch in '0123456789abcdefABCDEF' for ch in ref):
            cur.execute(
                'SELECT node_id, name, lat, lon, elevation_m FROM nodes WHERE lower(node_id) = lower(%s)',
                (ref,),
            )
            rows = cur.fetchall()
        elif len(ref) >= 6 and all(ch in '0123456789abcdefABCDEF' for ch in ref):
            cur.execute(
                'SELECT node_id, name, lat, lon, elevation_m FROM nodes WHERE lower(node_id) LIKE lower(%s) ORDER BY node_id LIMIT 2',
                (f'{ref}%',),
            )
            rows = cur.fetchall()
        else:
            cur.execute(
                '''
                SELECT node_id, name, lat, lon, elevation_m
                FROM nodes
                WHERE lower(coalesce(name, '')) = lower(%s)
                   OR lower(coalesce(name, '')) LIKE lower(%s)
                ORDER BY node_id
                LIMIT 2
                ''',
                (ref, f'%{ref}%'),
            )
            rows = cur.fetchall()

    if len(rows) < 1:
        raise ValueError(f'No node matched "{ref}"')
    if len(rows) > 1:
        raise ValueError(f'Ambiguous node reference "{ref}"')
    node_id, name, lat, lon, elevation_m = rows[0]
    return {
        'node_id': node_id,
        'name': name,
        'lat': lat,
        'lon': lon,
        'elevation_m': elevation_m,
    }


def compute_pair_diagnostics(a: dict, b: dict) -> dict:
    if a.get('lat') is None or a.get('lon') is None or b.get('lat') is None or b.get('lon') is None:
        raise ValueError('Both nodes need coordinates')

    elev_a = float(a.get('elevation_m') or 0.0)
    elev_b = float(b.get('elevation_m') or 0.0)

    with tempfile.TemporaryDirectory() as tmp_dir:
        vrt = build_link_vrt(float(a['lat']), float(a['lon']), float(b['lat']), float(b['lon']), tmp_dir, SRTM_DIR)
        if vrt is None:
            raise RuntimeError('No SRTM tiles available for this path')

        cos_mid = math.cos(math.radians((float(a['lat']) + float(b['lat'])) / 2))
        dlat = (float(b['lat']) - float(a['lat'])) * 111_320
        dlon = (float(b['lon']) - float(a['lon'])) * 111_320 * cos_mid
        d_total = math.sqrt(dlat ** 2 + dlon ** 2)
        n_samples = max(20, min(200, int(d_total / PROFILE_STEP_M)))

        ds = gdal.Open(vrt)
        if ds is None:
            raise RuntimeError('Failed to open generated VRT')
        gt = ds.GetGeoTransform()
        inv_gt = gdal.InvGeoTransform(gt)
        band = ds.GetRasterBand(1)

        heights: list[float] = []
        dists: list[float] = []
        for i in range(n_samples + 1):
            t = i / n_samples
            la = float(a['lat']) + t * (float(b['lat']) - float(a['lat']))
            lo = float(a['lon']) + t * (float(b['lon']) - float(a['lon']))
            px, py = gdal.ApplyGeoTransform(inv_gt, lo, la)
            px = int(np.clip(px, 0, ds.RasterXSize - 1))
            py = int(np.clip(py, 0, ds.RasterYSize - 1))
            data = band.ReadAsArray(px, py, 1, 1)
            h = max(0.0, float(data[0][0])) if data is not None else 0.0
            heights.append(h)
            dists.append(t * d_total)
        ds = None

    dists_arr = np.asarray(dists, dtype=np.float64)
    heights_arr = np.asarray(heights, dtype=np.float64)
    h_tx = elev_a + ANTENNA_HEIGHT_M
    h_rx = elev_b + ANTENNA_HEIGHT_M
    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)

    d1 = dists_arr[1:-1]
    d2 = d_total - d1
    valid = (d1 > 0) & (d2 > 0)
    d1 = d1[valid]
    d2 = d2[valid]
    profile_h = heights_arr[1:-1][valid]
    los_h = h_tx + (h_rx - h_tx) * (d1 / d_total)
    earth_bulge = (d1 * d2) / (2 * K_FACTOR * R_EARTH_M)
    excess_h = profile_h + earth_bulge - los_h
    with np.errstate(divide='ignore', invalid='ignore'):
        vs = excess_h * np.sqrt(2 * (d1 + d2) / (LAMBDA_M * d1 * d2))
    max_v = float(np.max(vs)) if vs.size else -999.0

    if max_v <= -0.78:
        diff_loss = 0.0
    else:
        diff_loss = max(0.0, 6.9 + 20 * math.log10(
            math.sqrt((max_v - 0.1) ** 2 + 1) + max_v - 0.1
        ))

    total_loss = fspl + diff_loss
    clear_los = max_v <= LINK_LOS_MAX_V
    usable_threshold_db = current_usable_path_loss_db()
    return {
        'node_a_id': a['node_id'],
        'node_a_name': a.get('name'),
        'node_b_id': b['node_id'],
        'node_b_name': b.get('name'),
        'distance_km': round(d_total / 1000.0, 3),
        'fspl_db': round(fspl, 3),
        'diffraction_loss_db': round(diff_loss, 3),
        'total_path_loss_db': round(total_loss, 3),
        'max_v': round(max_v, 6),
        'los_limit_v': LINK_LOS_MAX_V,
        'clear_los': clear_los,
        'usable_threshold_db': usable_threshold_db,
        'viable': bool(clear_los and total_loss < usable_threshold_db),
    }


def run_test_mode(args) -> None:
    db = wait_for_db()
    try:
        refresh_rf_calibration(db, force=True)
        refs: list[tuple[str, str, Optional[dict]]] = []

        if args.test_pairs:
            for raw in args.test_pairs.split(','):
                raw = raw.strip()
                if not raw:
                    continue
                left, sep, right = raw.partition(':')
                if not sep:
                    raise ValueError(f'Invalid pair "{raw}" — expected A:B')
                refs.append((left.strip(), right.strip(), None))

        if args.test_from_node:
            response = requests.get(f'{RADIO_BOT_URL}/state', timeout=10)
            response.raise_for_status()
            payload = response.json()
            monitors = payload.get('monitors') if isinstance(payload, dict) else None
            if not isinstance(monitors, list):
                raise RuntimeError('Radio bot state missing monitors list')
            source_ref = str(args.test_from_node).strip().lower()
            selected = None
            for monitor in monitors:
                if not isinstance(monitor, dict):
                    continue
                candidates = [
                    str(monitor.get('fullPublicKey') or '').strip().lower(),
                    str(monitor.get('targetHex') or '').strip().lower(),
                    str(monitor.get('nodeName') or '').strip().lower(),
                    str(monitor.get('label') or '').strip().lower(),
                ]
                if any(source_ref and source_ref in candidate for candidate in candidates if candidate):
                    selected = monitor
                    break
            if selected is None:
                raise ValueError(f'No radio monitor matched "{args.test_from_node}"')
            reporter_ref = str(selected.get('fullPublicKey') or selected.get('targetHex') or '').strip()
            neighbours = selected.get('lastNeighbours')
            if not reporter_ref or not isinstance(neighbours, list):
                raise RuntimeError('Selected radio monitor has no neighbour data')
            for neighbour in neighbours:
                if not isinstance(neighbour, dict):
                    continue
                peer_ref = str(neighbour.get('fullPublicKey') or '').strip()
                if len(peer_ref) != 64:
                    continue
                refs.append((reporter_ref, peer_ref, neighbour))

        if not refs:
            raise ValueError('No test pairs were provided')

        results = []
        for left_ref, right_ref, neighbour_meta in refs:
            a = resolve_node_ref(db, left_ref)
            b = resolve_node_ref(db, right_ref)
            result = compute_pair_diagnostics(a, b)
            if neighbour_meta is not None:
                reported_snr = neighbour_meta.get('snrDb')
                result['reported_snr_db'] = reported_snr
                result['reported_quality_band'] = radio_snr_band(reported_snr if isinstance(reported_snr, (int, float)) else None)
                result['reported_heard_seconds_ago'] = neighbour_meta.get('heardSecondsAgo')
                result['reported_adv_name'] = neighbour_meta.get('advName')
            results.append(result)

        print(json.dumps({
            'usable_threshold_db': current_usable_path_loss_db(),
            'signal_thresholds_db': current_signal_thresholds_db(),
            'results': results,
        }, indent=2))
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser(description='MeshCore viewshed/link worker')
    parser.add_argument('--test-pairs', help='Comma-separated node pairs in the form A:B,C:D')
    parser.add_argument('--test-from-node', help='Evaluate all current radio-bot neighbours for one repeater')
    args = parser.parse_args()
    if args.test_pairs or args.test_from_node:
        run_test_mode(args)
        return

    log.info(
        f'Viewshed worker starting (mode={WORKER_MODE}, '
        f'coverage_model={COVERAGE_MODEL}, model_version={COVERAGE_MODEL_VERSION})'
    )
    SRTM_DIR.mkdir(parents=True, exist_ok=True)

    # Connect once just to enqueue any nodes that lack coverage, then hand off
    # to the worker processes (each gets its own connection).
    db = wait_for_db()
    log.info('Connected to DB')
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    log.info('Connected to Redis')
    if WORKER_MODE in ('all', 'link'):
        refresh_radio_neighbor_reports(db, r, force=True)
    refresh_rf_calibration(db, force=True)
    refresh_support_context(db, force=True)
    if WORKER_MODE in ('all', 'viewshed'):
        rebuild_pending_viewshed_set(r)
        backfill_elevations(db)
        enqueue_uncovered(db, r)
    db.close()

    num_workers = int(os.environ.get('NUM_WORKERS', '2'))
    log.info(f'Launching {num_workers} worker process(es)')

    if num_workers <= 1:
        worker_loop()
        return

    procs = [
        multiprocessing.Process(target=worker_loop, name=f'Worker-{i + 1}', daemon=True)
        for i in range(num_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    main()
