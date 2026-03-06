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

JOB_QUEUE      = 'meshcore:viewshed_jobs'
LINK_JOB_QUEUE = 'meshcore:link_jobs'
LIVE_CHANNEL   = 'meshcore:live'

ANTENNA_HEIGHT_M     = 5    # observer height above ground (m) — fixed 5 m antenna
MIN_LINK_OBSERVATIONS = 5  # must match backend db/index.ts
PREFIX_AMBIGUITY_RADIUS_KM = 45.0  # only penalize same-prefix ambiguity when nodes are realistically in range
MAX_RADIUS_M     = 100_000  # absolute cap on viewshed radius (m)
SIMPLIFY_DEG     = 0.001    # Douglas-Peucker tolerance (~100 m)
N_RAYS           = 720      # number of radial rays cast from the observer
STEP_M           = 50.0     # ray step size in metres

# Radio horizon parameters
K_FACTOR  = 4 / 3        # effective Earth radius multiplier (standard troposphere)
R_EARTH_M = 6_371_000    # mean Earth radius (m)

# ── RF propagation model parameters (LoRa 868 MHz) ───────────────────────────

FREQ_MHZ        = 868.0
LAMBDA_M        = 3e8 / (FREQ_MHZ * 1e6)   # wavelength ~0.345 m
LINK_BUDGET_DB  = 148.0   # 17 dBm TX + ~130 dBm RX sensitivity (LoRa SF10 BW125) +1 dB SRTM DSM correction
FADE_MARGIN_DB  = 10.0    # safety / link margin
PROFILE_STEP_M  = 250.0   # terrain profile sample spacing (m)

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

    # Free-space path loss (dB)
    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)

    # Terrain profile: N evenly-spaced samples along the path
    N = max(20, min(200, int(d_total / PROFILE_STEP_M)))

    ds = gdal.Open(vrt_path)
    if ds is None:
        viable = fspl < LINK_BUDGET_DB - FADE_MARGIN_DB
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

    # ITU-R P.526 knife-edge diffraction loss (dB)
    if max_v <= -0.78:
        diff_loss = 0.0
    else:
        diff_loss = max(0.0, 6.9 + 20 * math.log10(
            math.sqrt((max_v - 0.1) ** 2 + 1) + max_v - 0.1
        ))

    total_loss = fspl + diff_loss
    viable     = total_loss < LINK_BUDGET_DB - FADE_MARGIN_DB
    return total_loss, viable


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

def calculate_viewshed(node_id: str, lat: float, lon: float) -> Optional[tuple[dict, float]]:
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
        log.info(
            f'  {node_id[:12]}… DTM elevation={elevation_m:.0f} m ASL, '
            f'horizon={radius_m / 1000:.1f} km'
        )

        # 6. Vectorised raycasting viewshed.
        #
        # For each of N_RAYS directions, walk outward in STEP_M increments tracking
        # the maximum "elevation angle" seen so far (corrected for Earth curvature).
        # When a step's angle falls below the running maximum, the terrain at that
        # step is in the shadow of an earlier ridge → the ray terminates.
        # The stop-point of each ray becomes a vertex of the coverage boundary.
        #
        # Elevation angle formula:
        #   angle(d) = (terrain_h - observer_h - d² / (2·k·R)) / d
        # where the d²/(2kR) term accounts for Earth's curvature under the ray.
        # A ray is blocked when angle(d) < running_max — no wrap-around artefacts.
        observer_h = elevation_m + ANTENNA_HEIGHT_M
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
        # gt[0]=x_origin (lon), gt[1]=px_width (deg/px), gt[3]=y_origin (lat), gt[5]=px_height (<0)
        pxs = np.clip(((pt_lons - gt[0]) / gt[1]).astype(np.int32), 0, n_cols - 1)
        pys = np.clip(((pt_lats - gt[3]) / gt[5]).astype(np.int32), 0, n_rows - 1)

        # Terrain heights at each ray step: (N, M)
        hs = elev[pys, pxs]

        # Elevation angles with Earth-curvature correction: (N, M)
        angles = (hs - observer_h - ds_arr[None, :] ** 2 / R_eff_2) / ds_arr[None, :]

        # Running max along each ray — only terrain AT OR ABOVE the observer
        # height can establish a blocking horizon.  Terrain below the observer
        # always lets the ray "see past" it; coverage is only cut off when
        # something taller than the node rises into the line of sight.
        blocking    = np.where(hs >= observer_h, angles, -np.inf)   # (N, M)
        running_max = np.maximum.accumulate(blocking, axis=1)        # (N, M)

        # "Previous" running max — shift one step so we compare current angle with
        # the max established BEFORE this step.  First column = -inf (never blocked).
        prev_max   = np.concatenate([np.full((N_RAYS, 1), -np.inf), running_max[:, :-1]], axis=1)
        in_shadow  = angles < prev_max   # (N, M): True where ray is terrain-blocked

        # Index of first shadow step per ray; n_steps if never shadowed.
        has_shadow  = in_shadow.any(axis=1)                                          # (N,)
        first_shad  = np.where(has_shadow, in_shadow.argmax(axis=1), n_steps)       # (N,)
        last_js     = np.clip(first_shad - 1, 0, n_steps - 1)                       # (N,)
        last_ds     = ds_arr[last_js]                                                # (N,)

        # Build boundary ring in (lon, lat) GeoJSON order.
        lons_b   = lon + np.cos(thetas) * last_ds * dpmlon   # (N,)
        lats_b   = lat + np.sin(thetas) * last_ds * dpmlat   # (N,)
        boundary = list(zip(lons_b.tolist(), lats_b.tolist()))
        boundary.append(boundary[0])   # close ring

        poly = ShapelyPolygon(boundary)
        if not poly.is_valid:
            poly = poly.buffer(0)

        # 7. Clip to UK mainland — removes coverage that extends into the sea.
        if UK_MAINLAND is not None:
            poly = poly.intersection(UK_MAINLAND)
            if poly.is_empty:
                log.warning(f'{node_id}: viewshed entirely at sea — skipping')
                return None

        # 8. Simplify (~100 m tolerance) to reduce stored polygon size.
        result = poly.simplify(SIMPLIFY_DEG, preserve_topology=True)

        if result.is_empty or result.geom_type not in ('Polygon', 'MultiPolygon'):
            log.warning(f'{node_id}: degenerate geometry after clipping — skipping')
            return None

        return mapping(result), radius_m, elevation_m

# ── DB helpers ────────────────────────────────────────────────────────────────

def already_calculated(db, node_id: str) -> bool:
    with db.cursor() as cur:
        cur.execute('SELECT 1 FROM node_coverage WHERE node_id = %s', (node_id,))
        return cur.fetchone() is not None

def store_coverage(db, node_id: str, geom: dict, radius_m: float, elevation_m: float):
    with db.cursor() as cur:
        cur.execute(
            '''INSERT INTO node_coverage (node_id, geom, antenna_height_m, radius_m)
               VALUES (%s, %s::jsonb, %s, %s)
               ON CONFLICT (node_id) DO UPDATE
                 SET geom = EXCLUDED.geom,
                     antenna_height_m = EXCLUDED.antenna_height_m,
                     radius_m = EXCLUDED.radius_m,
                     calculated_at = NOW()''',
            (node_id, json.dumps(geom), ANTENNA_HEIGHT_M, radius_m),
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
            row[0]: {'lat': row[1], 'lon': row[2], 'elevation_m': row[3] or 0.0,
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

    def local_prefix_ambiguity_penalty(prefix: str, target_id: str, target_node: dict, anchor_node: dict, pool: list[tuple[str, dict]]) -> float:
        target_dist = node_dist(target_node, anchor_node)
        raw = 0.0
        for cand_id, cand_node in pool:
            if cand_id == target_id:
                continue
            if not cand_id.upper().startswith(prefix):
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

    for prefix in reversed(path_hashes):
        prefix = prefix[:2].upper()
        candidates = [
            (nid, nd) for nid, nd in all_nodes.items()
            if nid.upper().startswith(prefix)
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
            ambiguity_penalty = local_prefix_ambiguity_penalty(prefix, nid, nd, prev, candidates)
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
            if itm_computed is None:
                vrt = build_link_vrt(a['lat'], a['lon'], b['lat'], b['lon'], tmp)
                if vrt:
                    try:
                        path_loss_db, itm_viable = compute_path_loss(
                            a['lat'], a['lon'], a['elevation_m'],
                            b['lat'], b['lon'], b['elevation_m'],
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
              AND nc.node_id IS NULL
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
        ''', ('%🚫%',))
        rows = cur.fetchall()
    if rows:
        log.info(f'Queuing {len(rows)} existing node(s) for viewshed calculation')
        for node_id, lat, lon in rows:
            r_client.lpush(JOB_QUEUE, json.dumps({'node_id': node_id, 'lat': lat, 'lon': lon}))

# ── Job processor ─────────────────────────────────────────────────────────────

def process_job(db, r_client, job: dict):
    node_id = job['node_id']
    lat     = float(job['lat'])
    lon     = float(job['lon'])

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

    geom, radius_m, elevation_m = result
    store_coverage(db, node_id, geom, radius_m, elevation_m)
    log.info(f'Done in {time.time() - t0:.1f}s — notifying frontend')

    r_client.publish(LIVE_CHANNEL, json.dumps({
        'type': 'coverage_update',
        'data': {'node_id': node_id, 'geom': geom},
        'ts':   int(time.time() * 1000),
    }))
    r_client.publish(LIVE_CHANNEL, json.dumps({
        'type': 'node_upsert',
        'data': {'node_id': node_id, 'elevation_m': round(elevation_m, 1)},
        'ts':   int(time.time() * 1000),
    }))

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
    log.info(f'{name} ready')

    while True:
        try:
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
    log.info(f'Viewshed worker starting (mode={WORKER_MODE})')
    SRTM_DIR.mkdir(parents=True, exist_ok=True)

    # Connect once just to enqueue any nodes that lack coverage, then hand off
    # to the worker processes (each gets its own connection).
    db = wait_for_db()
    log.info('Connected to DB')
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    log.info('Connected to Redis')
    if WORKER_MODE in ('all', 'viewshed'):
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
