import math
import os

import numpy as np
from osgeo import gdal

from rf.config import (
    ANTENNA_HEIGHT_M,
    K_FACTOR,
    LAMBDA_M,
    LINK_LOS_MAX_V,
    PROFILE_STEP_M,
    R_EARTH_M,
    current_usable_path_loss_db,
)

PATH_LOSS_BIAS_DB = float(os.environ.get('RF_PATH_LOSS_BIAS_DB', '0'))
TERRAIN_ROUGHNESS_FACTOR = max(0.0, min(1.0, float(os.environ.get('RF_TERRAIN_ROUGHNESS_FACTOR', '0.08'))))


def compute_path_loss(
    lat1: float,
    lon1: float,
    elev1: float,
    lat2: float,
    lon2: float,
    elev2: float,
    vrt_path: str,
    antenna_height_m_tx: float = ANTENNA_HEIGHT_M,
    antenna_height_m_rx: float = ANTENNA_HEIGHT_M,
) -> tuple[float, bool]:
    cos_mid = math.cos(math.radians((lat1 + lat2) / 2))
    dlat = (lat2 - lat1) * 111_320
    dlon = (lon2 - lon1) * 111_320 * cos_mid
    d_total = math.sqrt(dlat ** 2 + dlon ** 2)

    if d_total < 1.0:
      return 0.0, True

    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)
    usable_threshold_db = current_usable_path_loss_db()

    n_samples = max(20, min(200, int(d_total / PROFILE_STEP_M)))

    ds = gdal.Open(vrt_path)
    if ds is None:
        viable = fspl < usable_threshold_db
        return fspl, viable

    gt = ds.GetGeoTransform()
    inv_gt = gdal.InvGeoTransform(gt)
    band = ds.GetRasterBand(1)

    heights: list[float] = []
    dists: list[float] = []
    for i in range(n_samples + 1):
        t = i / n_samples
        la = lat1 + t * (lat2 - lat1)
        lo = lon1 + t * (lon2 - lon1)
        px, py = gdal.ApplyGeoTransform(inv_gt, lo, la)
        px = int(np.clip(px, 0, ds.RasterXSize - 1))
        py = int(np.clip(py, 0, ds.RasterYSize - 1))
        data = band.ReadAsArray(px, py, 1, 1)
        h = max(0.0, float(data[0][0])) if data is not None else 0.0
        heights.append(h)
        dists.append(t * d_total)
    ds = None

    h_tx = elev1 + antenna_height_m_tx
    h_rx = elev2 + antenna_height_m_rx

    return compute_path_loss_from_profile(
        np.asarray(dists, dtype=np.float32),
        np.asarray(heights, dtype=np.float32),
        h_tx,
        h_rx,
    )


def compute_path_loss_from_profile(
    dists: np.ndarray,
    heights: np.ndarray,
    h_tx: float,
    h_rx: float,
) -> tuple[float, bool]:
    if len(dists) != len(heights) or len(dists) == 0:
        return float('inf'), False
    finite = np.isfinite(dists) & np.isfinite(heights)
    if np.count_nonzero(finite) < 2:
        return float('inf'), False
    dists = dists[finite]
    heights = heights[finite]
    order = np.argsort(dists)
    dists = dists[order]
    heights = heights[order]
    d_total = float(dists[-1])
    usable_threshold_db = current_usable_path_loss_db()
    if d_total < 1.0:
        return 0.0, True

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

    if max_v <= -0.78:
        diff_loss = 0.0
    else:
        diff_loss = max(0.0, 6.9 + 20 * math.log10(
            math.sqrt((max_v - 0.1) ** 2 + 1) + max_v - 0.1
        ))

    # A small robust roughness term improves agreement with observed UK links
    # without allowing a single SRTM spike to dominate the knife-edge loss.
    terrain_roughness = float(np.percentile(np.abs(np.diff(heights)), 75)) if len(heights) > 3 else 0.0
    roughness_loss = min(6.0, math.log1p(max(0.0, terrain_roughness)) * TERRAIN_ROUGHNESS_FACTOR)
    total_loss = max(fspl, fspl + diff_loss + roughness_loss + PATH_LOSS_BIAS_DB)
    clear_los = max_v <= LINK_LOS_MAX_V
    viable = clear_los and total_loss < usable_threshold_db
    return total_loss, viable
