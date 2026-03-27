import math

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
    d_total = float(dists[-1]) if len(dists) else 0.0
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

    total_loss = fspl + diff_loss
    clear_los = max_v <= LINK_LOS_MAX_V
    viable = clear_los and total_loss < usable_threshold_db
    return total_loss, viable
