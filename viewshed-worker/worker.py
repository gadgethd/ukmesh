"""
MeshCore Analytics — Viewshed Worker
Consumes jobs from Redis, downloads SRTM1 tiles, computes a raycasting
viewshed, clips to the UK mainland, stores the result polygon in
node_coverage, then notifies the frontend.
"""

import gzip
import json
import logging
import math
import multiprocessing
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

import numpy as np
import psycopg2
from scipy.ndimage import minimum_filter as _min_filter
from scipy.spatial import cKDTree
import redis
import requests
from osgeo import gdal
from shapely.geometry import mapping, Polygon as ShapelyPolygon

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
WORKER_MODE  = os.environ.get('WORKER_MODE', 'all').lower()
COVERAGE_MODEL = os.environ.get('COVERAGE_MODEL', 'rf_radial_100m').lower()

JOB_QUEUE      = 'meshcore:viewshed_jobs'
JOB_PENDING_SET = 'meshcore:viewshed_pending'
LINK_JOB_QUEUE = 'meshcore:link_jobs'
LIVE_CHANNEL   = 'meshcore:live'

ANTENNA_HEIGHT_M      = 5    # source repeater antenna above ground (m)
COVERAGE_TARGET_HEIGHT_M = 5 # target repeater antenna above ground (m) for coverage polygons
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
SUPPORT_REFRESH_S = int(os.environ.get('COVERAGE_SUPPORT_REFRESH_S', '900'))
SUPPORT_NEARBY_REPEATER_KM = float(os.environ.get('COVERAGE_SUPPORT_NEARBY_REPEATER_KM', '12'))
SUPPORT_PENALTY_PER_KM_DB = float(os.environ.get('COVERAGE_SUPPORT_PENALTY_PER_KM_DB', '0.6'))
SUPPORT_MAX_PENALTY_DB = float(os.environ.get('COVERAGE_SUPPORT_MAX_PENALTY_DB', '14'))
SUPPORT_PROJECTION_LAT = float(os.environ.get('COVERAGE_SUPPORT_PROJECTION_LAT', '54.0'))

DEFAULT_LINK_BUDGET_DB = 148.0
DEFAULT_FADE_MARGIN_DB = 10.0
DEFAULT_USABLE_PATH_LOSS_DB = DEFAULT_LINK_BUDGET_DB - DEFAULT_FADE_MARGIN_DB
CALIBRATION_REFRESH_S = int(os.environ.get('COVERAGE_CALIBRATION_REFRESH_S', '900'))
CALIBRATION_MIN_LINKS = int(os.environ.get('COVERAGE_CALIBRATION_MIN_LINKS', '24'))
CALIBRATION_MIN_OBSERVED_COUNT = int(os.environ.get('COVERAGE_CALIBRATION_MIN_OBSERVED_COUNT', '3'))
CALIBRATION_MAX_THRESHOLD_BOOST_DB = float(os.environ.get('COVERAGE_CALIBRATION_MAX_THRESHOLD_BOOST_DB', '8'))
CALIBRATION_PERCENTILE = float(os.environ.get('COVERAGE_CALIBRATION_PERCENTILE', '0.9'))
CALIBRATION_EXTRA_MARGIN_DB = float(os.environ.get('COVERAGE_CALIBRATION_EXTRA_MARGIN_DB', '1.5'))

# Radio horizon parameters
K_FACTOR  = 4 / 3        # effective Earth radius multiplier (standard troposphere)
R_EARTH_M = 6_371_000    # mean Earth radius (m)

# ── RF propagation model parameters (LoRa 868 MHz) ───────────────────────────

FREQ_MHZ        = 868.0
LAMBDA_M        = 3e8 / (FREQ_MHZ * 1e6)   # wavelength ~0.345 m
PROFILE_STEP_M  = 250.0   # terrain profile sample spacing (m)

RF_CALIBRATION = {
    'usable_path_loss_db': DEFAULT_USABLE_PATH_LOSS_DB,
    'signal_thresholds_db': {
        'green': max(116.0, DEFAULT_USABLE_PATH_LOSS_DB - 16.0),
        'amber': max(124.0, DEFAULT_USABLE_PATH_LOSS_DB - 8.0),
        'red': DEFAULT_USABLE_PATH_LOSS_DB,
    },
    'samples': 0,
    'updated_at': 0.0,
}

SUPPORT_CONTEXT = {
    'tree': None,
    'node_ids': [],
    'node_index_by_id': {},
    'max_link_km_by_node': {},
    'updated_at': 0.0,
}


def current_usable_path_loss_db() -> float:
    return float(RF_CALIBRATION['usable_path_loss_db'])


def current_signal_thresholds_db() -> dict[str, float]:
    return dict(RF_CALIBRATION['signal_thresholds_db'])


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


def refresh_rf_calibration(db, force: bool = False) -> None:
    now = time.time()
    if not force and now - float(RF_CALIBRATION['updated_at']) < CALIBRATION_REFRESH_S:
        return

    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT itm_path_loss_db, observed_count
            FROM node_links
            WHERE itm_path_loss_db IS NOT NULL
              AND observed_count >= %s
              AND force_viable = false
            ''',
            (CALIBRATION_MIN_OBSERVED_COUNT,),
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
    weights = np.asarray([min(16.0, max(1.0, math.sqrt(float(row[1])))) for row in rows], dtype=np.float64)
    observed_tail = weighted_quantile(losses, weights, CALIBRATION_PERCENTILE)
    usable_threshold = max(DEFAULT_USABLE_PATH_LOSS_DB, observed_tail + CALIBRATION_EXTRA_MARGIN_DB)
    usable_threshold = min(DEFAULT_USABLE_PATH_LOSS_DB + CALIBRATION_MAX_THRESHOLD_BOOST_DB, usable_threshold)

    RF_CALIBRATION['usable_path_loss_db'] = round(float(usable_threshold), 2)
    RF_CALIBRATION['signal_thresholds_db'] = {
        'green': round(max(116.0, usable_threshold - 16.0), 2),
        'amber': round(max(124.0, usable_threshold - 8.0), 2),
        'red': round(float(usable_threshold), 2),
    }
    RF_CALIBRATION['samples'] = len(rows)
    RF_CALIBRATION['updated_at'] = now
    log.info(
        'RF calibration: '
        f'samples={len(rows)}, p{int(CALIBRATION_PERCENTILE * 100)}={observed_tail:.1f} dB, '
        f'usable={RF_CALIBRATION["usable_path_loss_db"]:.1f} dB, '
        f'green={RF_CALIBRATION["signal_thresholds_db"]["green"]:.1f}, '
        f'amber={RF_CALIBRATION["signal_thresholds_db"]["amber"]:.1f}'
    )


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
              AND (name IS NULL OR name NOT LIKE %s)
              AND (role IS NULL OR role = 2)
            ''',
            ('%🚫%',),
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
              AND nl.observed_count >= %s
              AND (nl.itm_viable = true OR nl.force_viable = true)
            ''',
            (MIN_LINK_OBSERVATIONS,),
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

def compute_path_loss(lat1: float, lon1: float, elev1: float,
                      lat2: float, lon2: float, elev2: float,
                      vrt_path: str) -> tuple[float, bool]:
    """Estimate RF path loss (dB) between two points.

    Uses free-space path loss plus ITU-R P.526 single knife-edge diffraction
    over the dominant terrain obstruction, corrected for Earth curvature.
    Returns (path_loss_db, is_viable).
    """
    cos_mid = math.cos(math.radians((lat1 + lat2) / 2))
    dlat    = (lat2 - lat1) * 111_320
    dlon    = (lon2 - lon1) * 111_320 * cos_mid
    d_total = math.sqrt(dlat ** 2 + dlon ** 2)

    if d_total < 1.0:
        return 0.0, True

    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)
    usable_threshold_db = current_usable_path_loss_db()

    # Terrain profile: N evenly-spaced samples along the path
    N = max(20, min(200, int(d_total / PROFILE_STEP_M)))

    ds = gdal.Open(vrt_path)
    if ds is None:
        viable = fspl < usable_threshold_db
        return fspl, viable

    gt     = ds.GetGeoTransform()
    inv_gt = gdal.InvGeoTransform(gt)
    band   = ds.GetRasterBand(1)

    heights: list[float] = []
    dists:   list[float] = []
    for i in range(N + 1):
        t    = i / N
        la   = lat1 + t * (lat2 - lat1)
        lo   = lon1 + t * (lon2 - lon1)
        px, py = gdal.ApplyGeoTransform(inv_gt, lo, la)
        px   = int(np.clip(px, 0, ds.RasterXSize - 1))
        py   = int(np.clip(py, 0, ds.RasterYSize - 1))
        data = band.ReadAsArray(px, py, 1, 1)
        h    = max(0.0, float(data[0][0])) if data is not None else 0.0
        heights.append(h)
        dists.append(t * d_total)
    ds = None

    h_tx = elev1 + ANTENNA_HEIGHT_M   # transmitter height ASL + antenna
    h_rx = elev2 + ANTENNA_HEIGHT_M

    # Find dominant obstruction via Fresnel-Kirchhoff diffraction parameter
    max_v = -999.0
    for i in range(1, N):
        d1 = dists[i]
        d2 = d_total - dists[i]
        if d1 <= 0 or d2 <= 0:
            continue
        los_h       = h_tx + (h_rx - h_tx) * (d1 / d_total)
        earth_bulge = (d1 * d2) / (2 * K_FACTOR * R_EARTH_M)
        excess_h    = heights[i] + earth_bulge - los_h
        v = excess_h * math.sqrt(2 * (d1 + d2) / (LAMBDA_M * d1 * d2))
        max_v = max(max_v, v)

    return compute_path_loss_from_profile(
        np.asarray(dists, dtype=np.float32),
        np.asarray(heights, dtype=np.float32),
        h_tx,
        h_rx,
    )


def compute_path_loss_from_profile(dists: np.ndarray,
                                   heights: np.ndarray,
                                   h_tx: float,
                                   h_rx: float) -> tuple[float, bool]:
    d_total = float(dists[-1]) if len(dists) else 0.0
    usable_threshold_db = current_usable_path_loss_db()
    if d_total < 1.0:
        return 0.0, True

    # Free-space path loss (dB)
    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)

    if len(dists) <= 2:
        viable = fspl < usable_threshold_db
        return fspl, viable

    d1 = dists[1:-1].astype(np.float64)
    d2 = d_total - d1
    valid = (d1 > 0) & (d2 > 0)
    if not np.any(valid):
        viable = fspl < usable_threshold_db
        return fspl, viable

    d1 = d1[valid]
    d2 = d2[valid]
    profile_h = heights[1:-1].astype(np.float64)[valid]
    los_h = h_tx + (h_rx - h_tx) * (d1 / d_total)
    earth_bulge = (d1 * d2) / (2 * K_FACTOR * R_EARTH_M)
    excess_h = profile_h + earth_bulge - los_h
    with np.errstate(divide='ignore', invalid='ignore'):
        vs = excess_h * np.sqrt(2 * (d1 + d2) / (LAMBDA_M * d1 * d2))
    max_v = float(np.max(vs)) if vs.size else -999.0

    # ITU-R P.526 knife-edge diffraction loss (dB)
    if max_v <= -0.78:
        diff_loss = 0.0
    else:
        diff_loss = max(0.0, 6.9 + 20 * math.log10(
            math.sqrt((max_v - 0.1) ** 2 + 1) + max_v - 0.1
        ))

    total_loss = fspl + diff_loss
    viable     = total_loss < usable_threshold_db
    return total_loss, viable


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


def build_link_vrt(lat1: float, lon1: float, lat2: float, lon2: float,
                   tmp_dir: str) -> Optional[str]:
    """Build a GDAL VRT from already-cached SRTM tiles covering the path.
    Returns None if no tiles are available (will be retried later once
    nearby viewsheds have triggered tile downloads)."""
    min_lat = math.floor(min(lat1, lat2))
    max_lat = math.floor(max(lat1, lat2))
    min_lon = math.floor(min(lon1, lon2))
    max_lon = math.floor(max(lon1, lon2))
    paths   = [
        str(SRTM_DIR / f'{tile_name(lt, ln)}.hgt')
        for lt in range(min_lat, max_lat + 1)
        for ln in range(min_lon, max_lon + 1)
        if (SRTM_DIR / f'{tile_name(lt, ln)}.hgt').exists()
    ]
    if not paths:
        return None
    vrt = f'{tmp_dir}/link.vrt'
    r   = subprocess.run(['gdalbuildvrt', vrt] + paths, capture_output=True, text=True)
    return vrt if r.returncode == 0 else None

# ── UK mainland polygon (loaded once at startup for ocean clipping) ───────────

def _load_uk_mainland():
    path = Path(__file__).parent / 'uk_mainland.json'
    if not path.exists():
        log.warning('uk_mainland.json not found — ocean clipping disabled')
        return None
    with open(path) as f:
        data = json.load(f)
    from shapely.geometry import shape as _shape
    poly = _shape(data)
    if not poly.is_valid:
        poly = poly.buffer(0)
    if data['type'] == 'MultiPolygon':
        total_pts = sum(len(ring) for poly in data['coordinates'] for ring in poly)
        log.info(f'UK mainland MultiPolygon loaded ({len(data["coordinates"])} polygons, {total_pts} total points)')
    else:
        log.info(f'UK mainland polygon loaded ({len(data["coordinates"][0])} points)')
    return poly

UK_MAINLAND = _load_uk_mainland()

# ── SRTM tile download (AWS Terrain Tiles — public, no auth) ─────────────────

def tile_name(lat: int, lon: int) -> str:
    ns = 'N' if lat >= 0 else 'S'
    ew = 'E' if lon >= 0 else 'W'
    return f'{ns}{abs(lat):02d}{ew}{abs(lon):03d}'

def download_tile(lat: int, lon: int) -> Optional[Path]:
    name = tile_name(lat, lon)
    path = SRTM_DIR / f'{name}.hgt'
    if path.exists():
        return path

    url = (
        f'https://s3.amazonaws.com/elevation-tiles-prod/skadi/'
        f'{name[:3]}/{name}.hgt.gz'
    )
    log.info(f'Downloading {name} ...')
    try:
        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code == 404:
            log.debug(f'{name} not found (ocean / outside coverage)')
            return None
        resp.raise_for_status()
        data = gzip.decompress(resp.content)
        SRTM_DIR.mkdir(parents=True, exist_ok=True)
        # Atomic write: temp file → rename prevents partial-read races between workers
        tmp = path.with_suffix('.tmp')
        tmp.write_bytes(data)
        tmp.rename(path)
        log.info(f'Saved {name}.hgt ({len(data) // 1024} KB)')
        return path
    except Exception as exc:
        log.error(f'Failed to download {name}: {exc}')
        return None

def tiles_for_radius(lat: float, lon: float, radius_m: float) -> list[tuple[int, int]]:
    """All 1°×1° SRTM tiles that overlap a bounding box around (lat, lon)."""
    d_lat = radius_m / 111_320
    d_lon = radius_m / (111_320 * math.cos(math.radians(lat)))
    return [
        (lt, ln)
        for lt in range(math.floor(lat - d_lat), math.floor(lat + d_lat) + 1)
        for ln in range(math.floor(lon - d_lon), math.floor(lon + d_lon) + 1)
    ]

def radio_horizon_m(height_asl_m: float) -> float:
    """One-way radio horizon distance (m) for an antenna at height_asl_m above sea level.

    Uses the standard 4/3-Earth-radius model for tropospheric refraction.
    Formula: d = sqrt(2 * k * R * h)
    """
    h = max(1.0, height_asl_m)  # clamp: 1 m minimum to avoid zero
    return math.sqrt(2 * K_FACTOR * R_EARTH_M * h)


def sample_elevation(vrt_path: str, lat: float, lon: float) -> float:
    """Return terrain elevation (m ASL) at (lat, lon) sampled from a GDAL VRT."""
    ds = gdal.Open(vrt_path)
    if ds is None:
        return 0.0
    gt  = ds.GetGeoTransform()
    inv = gdal.InvGeoTransform(gt)
    if inv is None:
        ds = None
        return 0.0
    px, py = gdal.ApplyGeoTransform(inv, lon, lat)
    px = max(0, min(int(px), ds.RasterXSize - 1))
    py = max(0, min(int(py), ds.RasterYSize - 1))
    data = ds.GetRasterBand(1).ReadAsArray(px, py, 1, 1)
    ds   = None
    return max(0.0, float(data[0][0])) if data is not None else 0.0


# ── Viewshed calculation ──────────────────────────────────────────────────────

def calculate_viewshed(node_id: str, lat: float, lon: float) -> Optional[tuple[dict, dict[str, dict], float, float]]:
    with tempfile.TemporaryDirectory() as tmp:
        # 1. Download the observer's own tile and sample terrain elevation.
        #    This single tile is sufficient to determine node height; we need
        #    it before we know how far to reach for surrounding tiles.
        obs_tile = (math.floor(lat), math.floor(lon))
        obs_path = download_tile(*obs_tile)
        if not obs_path:
            log.error(f'No SRTM tile for observer at {node_id} ({lat:.4f}, {lon:.4f})')
            return None

        obs_vrt = f'{tmp}/observer.vrt'
        subprocess.run(
            ['gdalbuildvrt', obs_vrt, str(obs_path)],
            capture_output=True, text=True,
        )
        elevation_m = sample_elevation(obs_vrt, lat, lon)

        # 2. Radio-horizon radius: node ASL + 5 m fixed antenna height.
        effective_height_m = elevation_m + ANTENNA_HEIGHT_M
        radius_m = min(radio_horizon_m(effective_height_m), MAX_RADIUS_M)
        radius_m = source_support_radius_m(node_id, radius_m)
        log.info(
            f'  {node_id[:12]}…: elevation={elevation_m:.0f} m ASL, '
            f'antenna={effective_height_m:.0f} m, horizon={radius_m / 1000:.1f} km'
        )

        # 3. Download all tiles covering the computed horizon radius
        needed = tiles_for_radius(lat, lon, radius_m)
        paths  = [p for t in needed if (p := download_tile(*t))]
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
        effective_height_m = elevation_m + ANTENNA_HEIGHT_M
        radius_m = min(radio_horizon_m(effective_height_m), MAX_RADIUS_M)
        radius_m = source_support_radius_m(node_id, radius_m)
        log.info(
            f'  {node_id[:12]}… DTM elevation={elevation_m:.0f} m ASL, '
            f'horizon={radius_m / 1000:.1f} km'
        )

        observer_h = elevation_m + ANTENNA_HEIGHT_M
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

def store_coverage(db, node_id: str, geom: dict, strength_geoms: dict[str, dict], radius_m: float, elevation_m: float):
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
            (node_id, json.dumps(geom), json.dumps(strength_geoms), ANTENNA_HEIGHT_M, radius_m, COVERAGE_MODEL_VERSION),
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

def process_link_job(db, r_client, job: dict):
    """Resolve relay path prefixes to known nodes (backwards from the receiver),
    record observations in node_links, and compute RF path loss for new pairs.

    Uses accumulated confirmed-link knowledge (node_links) to prefer known
    neighbours over purely geographic proximity — the algorithm improves as
    more packets are observed.
    """
    rx_node_id  = job.get('rx_node_id')
    src_node_id = job.get('src_node_id')
    path_hashes = job.get('path_hashes', [])

    if not rx_node_id or not path_hashes:
        return

    # Load all positioned repeater nodes
    with db.cursor() as cur:
        cur.execute(
            'SELECT node_id, lat, lon, elevation_m, name, role FROM nodes '
            'WHERE lat IS NOT NULL AND lon IS NOT NULL'
        )
        all_nodes = {
            row[0]: {'lat': row[1], 'lon': row[2], 'elevation_m': row[3],
                     'name': row[4], 'role': row[5]}
            for row in cur.fetchall()
        }

    # Load confirmed link pairs so we can prefer known neighbours when resolving
    # relay hashes — forms the self-improving feedback loop.
    with db.cursor() as cur:
        cur.execute(
            'SELECT node_a_id, node_b_id FROM node_links '
            'WHERE itm_viable = true AND observed_count >= %s',
            (MIN_LINK_OBSERVATIONS,),
        )
        confirmed_pairs: set[tuple[str, str]] = {
            (min(a, b), max(a, b)) for a, b in cur.fetchall()
        }

    def confirmed_link(a_id: str, b_id: str) -> bool:
        return (min(a_id, b_id), max(a_id, b_id)) in confirmed_pairs

    rx = all_nodes.get(rx_node_id)
    if not rx:
        return

    def node_dist(a: dict, b: dict) -> float:
        cos_m = math.cos(math.radians((a['lat'] + b['lat']) / 2))
        return math.sqrt(
            ((a['lat'] - b['lat']) * 111.32) ** 2 +
            ((a['lon'] - b['lon']) * 111.32 * cos_m) ** 2
        )

    def normalize_path_hash(value) -> str:
        return str(value or '').strip().upper()

    def node_matches_path_hash(node_id: str, path_hash: str) -> bool:
        return bool(path_hash) and node_id.upper().startswith(path_hash)

    def local_prefix_ambiguity_penalty(path_hash: str, target_id: str, target_node: dict, anchor_node: dict, pool: list[tuple[str, dict]]) -> float:
        target_dist = node_dist(target_node, anchor_node)
        raw = 0.0
        for cand_id, cand_node in pool:
            if cand_id == target_id:
                continue
            if not node_matches_path_hash(cand_id, path_hash):
                continue
            cand_dist = node_dist(cand_node, anchor_node)
            if cand_dist > PREFIX_AMBIGUITY_RADIUS_KM:
                continue
            dist_similarity = max(0.0, 1.0 - abs(cand_dist - target_dist) / PREFIX_AMBIGUITY_RADIUS_KM)
            proximity = max(0.0, 1.0 - cand_dist / PREFIX_AMBIGUITY_RADIUS_KM)
            raw += dist_similarity * proximity
        # Bound so this is only a modest confidence deduction in clustered regions.
        return min(0.24, raw * 0.12)

    # Resolve path working backwards from rx (known position anchor).
    # Each node can only appear once — MeshCore nodes never relay the same
    # packet twice.
    resolved: list[tuple[str, dict]] = []
    prev_id  = rx_node_id
    prev     = rx
    visited  = {rx_node_id}

    for raw_hash in reversed(path_hashes):
        path_hash = normalize_path_hash(raw_hash)
        if not path_hash:
            continue
        candidates = [
            (nid, nd) for nid, nd in all_nodes.items()
            if node_matches_path_hash(nid, path_hash)
            and nid not in visited
            and (nd['role'] is None or nd['role'] == 2)
            and nd['name'] and '🚫' not in nd['name']
        ]
        if not candidates:
            continue

        # Prefer confirmed neighbours of the previous node, but deduct confidence
        # when same-prefix repeaters cluster near the same anchor (possible ambiguity).
        best_id = None
        best = None
        best_score = float('-inf')
        for nid, nd in candidates:
            confirmed_bonus = 2.5 if confirmed_link(nid, prev_id) else 0.0
            distance_score = -node_dist(nd, prev) / 12.0
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
        prev    = best

    # Build adjacency list: src → relays → rx
    full: list[tuple[str, dict]] = []
    if src_node_id and src_node_id in all_nodes:
        full.append((src_node_id, all_nodes[src_node_id]))
    full.extend(resolved)
    full.append((rx_node_id, rx))

    if len(full) < 2:
        return

    # Upsert observations and compute path loss for each adjacent pair.
    # full[i] → full[i+1] means full[i] transmitted, full[i+1] received.
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(len(full) - 1):
            src_id, src = full[i]    # transmitted
            dst_id, dst = full[i + 1]  # received
            if src['lat'] is None or src['lon'] is None or dst['lat'] is None or dst['lon'] is None:
                continue

            # Canonical ordering (lower ID first) → unique primary key
            if src_id < dst_id:
                a_id, a, b_id, b = src_id, src, dst_id, dst
                inc_atob, inc_btoa = 1, 0   # src==a transmitted to dst==b
            else:
                a_id, a, b_id, b = dst_id, dst, src_id, src
                inc_atob, inc_btoa = 0, 1   # src==b transmitted to dst==a

            # Upsert observation with directional counts; check whether ITM already computed
            with db.cursor() as cur:
                cur.execute(
                    '''INSERT INTO node_links
                           (node_a_id, node_b_id, observed_count, last_observed,
                            count_a_to_b, count_b_to_a)
                       VALUES (%s, %s, 1, NOW(), %s, %s)
                       ON CONFLICT (node_a_id, node_b_id) DO UPDATE
                         SET observed_count = node_links.observed_count + 1,
                             last_observed  = NOW(),
                             count_a_to_b   = node_links.count_a_to_b + %s,
                             count_b_to_a   = node_links.count_b_to_a + %s
                       RETURNING observed_count, itm_computed_at, itm_path_loss_db, itm_viable, count_a_to_b, count_b_to_a''',
                    (a_id, b_id, inc_atob, inc_btoa, inc_atob, inc_btoa),
                )
                row = cur.fetchone()
            obs_count       = row[0] if row else 1
            itm_computed    = row[1] if row else None
            path_loss_db_db = row[2] if row else None
            itm_viable_db   = row[3] if row else None
            count_a_to_b    = row[4] if row else inc_atob
            count_b_to_a    = row[5] if row else inc_btoa

            # Compute ITM path loss if not yet done and tiles are cached
            path_loss_db: Optional[float] = path_loss_db_db
            itm_viable:   Optional[bool]  = itm_viable_db
            missing_endpoint_elev = a.get('elevation_m') is None or b.get('elevation_m') is None
            if itm_computed is None or missing_endpoint_elev:
                vrt = build_link_vrt(a['lat'], a['lon'], b['lat'], b['lon'], tmp)
                if vrt:
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
                        )
                        with db.cursor() as cur:
                            cur.execute(
                                '''UPDATE node_links
                                   SET itm_path_loss_db = %s,
                                       itm_viable       = %s,
                                       itm_computed_at  = NOW()
                                   WHERE node_a_id = %s AND node_b_id = %s''',
                                (round(path_loss_db, 1), itm_viable, a_id, b_id),
                            )
                        path_loss_db = round(path_loss_db, 1)
                        log.info(
                            f'Link {a_id[:8]}…↔{b_id[:8]}…: '
                            f'{path_loss_db:.1f} dB {"✓" if itm_viable else "✗"} '
                            f'(obs={obs_count})'
                        )
                    except Exception as exc:
                        log.warning(f'Path loss computation failed: {exc}')

            # Notify frontend
            r_client.publish(LIVE_CHANNEL, json.dumps({
                'type': 'link_update',
                'data': {
                    'node_a_id':        a_id,
                    'node_b_id':        b_id,
                    'observed_count':   obs_count,
                    'itm_path_loss_db': path_loss_db,
                    'itm_viable':       itm_viable,
                    'count_a_to_b':     count_a_to_b,
                    'count_b_to_a':     count_b_to_a,
                },
                'ts': int(time.time() * 1000),
            }))


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
              AND (nc.node_id IS NULL OR nc.model_version < %s)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
        ''', (COVERAGE_MODEL_VERSION, '%🚫%',))
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
        # Skip hidden (🚫) or non-repeater nodes regardless of how the job arrived
        with db.cursor() as cur:
            cur.execute('SELECT name, role FROM nodes WHERE node_id = %s', (node_id,))
            row = cur.fetchone()
        if row:
            name, role = row
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
        result = calculate_viewshed(node_id, lat, lon)
        if result is None:
            return

        geom, strength_geoms, radius_m, elevation_m = result
        store_coverage(db, node_id, geom, strength_geoms, radius_m, elevation_m)
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
            conn = psycopg2.connect(DATABASE_URL)
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
    refresh_rf_calibration(db, force=True)
    refresh_support_context(db, force=True)
    log.info(f'{name} ready')

    while True:
        try:
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

def main():
    log.info(
        f'Viewshed worker starting (mode={WORKER_MODE}, '
        f'coverage_model={COVERAGE_MODEL}, model_version={COVERAGE_MODEL_VERSION})'
    )
    SRTM_DIR.mkdir(parents=True, exist_ok=True)

    # Connect once just to enqueue any nodes that lack coverage, then hand off
    # to the worker processes (each gets its own connection).
    db = wait_for_db()
    log.info('Connected to DB')
    refresh_rf_calibration(db, force=True)
    refresh_support_context(db, force=True)
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    log.info('Connected to Redis')
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
