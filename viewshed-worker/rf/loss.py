import bisect
import math
import os
from dataclasses import dataclass

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


@dataclass(frozen=True)
class PathLossResult:
    path_loss_db: float
    viable: bool
    profile: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class PrefixPathLossResult:
    path_loss_db: np.ndarray
    viable: np.ndarray
    max_fresnel_v: np.ndarray


def _running_percentile_75(values: np.ndarray) -> np.ndarray:
    """Return the exact NumPy-linear p75 for every growing value prefix.

    RF radial profiles are capped at 1,000 samples.  Maintaining a sorted
    Python list is nominally O(n²), but at that deliberately small bound its
    contiguous moves run in C and are substantially faster than performing two
    Python-level Fenwick-tree searches for every sample.  Result index ``i``
    represents the first ``i`` input values.
    """
    values = np.asarray(values, dtype=np.float64)
    result = np.zeros(len(values) + 1, dtype=np.float64)
    if len(values) < 1:
        return result

    ordered: list[float] = []
    for count, value in enumerate(values, start=1):
        bisect.insort_right(ordered, float(value))
        position = 0.75 * (count - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        fraction = position - lower
        lower_value = ordered[lower]
        upper_value = ordered[upper]
        result[count] = lower_value + (upper_value - lower_value) * fraction
    return result


def compute_prefix_path_losses(
    dists: np.ndarray,
    heights: np.ndarray,
    h_tx: float,
    h_rx_by_endpoint: np.ndarray,
    *,
    endpoint_batch_size: int = 64,
    ray_batch_size: int = 8,
) -> PrefixPathLossResult:
    """Calculate growing path prefixes for one ray or a batch of rays.

    This is mathematically equivalent to calling
    :func:`compute_path_loss_from_profile` for each prefix, but removes the
    repeated Python-level full-prefix work used by radial coverage jobs.  A 2-D
    ``heights`` array is processed in bounded ray and endpoint batches so the
    worker can amortise NumPy dispatch without materialising the full
    rays-by-endpoints-by-obstacles cube.
    """
    dists = np.asarray(dists, dtype=np.float64)
    heights = np.asarray(heights, dtype=np.float64)
    h_rx = np.asarray(h_rx_by_endpoint, dtype=np.float64)
    single_ray = heights.ndim == 1
    if single_ray:
        heights = heights[None, :]
        h_rx = h_rx[None, :]
    if heights.ndim != 2 or h_rx.ndim != 2:
        raise ValueError('height inputs must both be one- or two-dimensional')
    if heights.shape != h_rx.shape or len(dists) != heights.shape[1]:
        raise ValueError('distance, height, and receiver-height arrays must match')
    if len(dists) < 1:
        empty_shape = (heights.shape[0], 0)
        empty_float = np.empty(empty_shape, dtype=np.float64)
        if single_ray:
            empty_float = empty_float[0]
        return PrefixPathLossResult(
            empty_float,
            np.empty_like(empty_float, dtype=bool),
            empty_float.copy(),
        )
    if not (
        np.all(np.isfinite(dists))
        and np.all(np.isfinite(heights))
        and np.all(np.isfinite(h_rx))
    ):
        raise ValueError('prefix path inputs must be finite')
    if np.any(np.diff(dists) < 0):
        raise ValueError('prefix path distances must be ordered')

    usable_threshold_db = current_usable_path_loss_db()
    base_losses = np.zeros(len(dists), dtype=np.float64)

    positive = dists >= 1.0
    base_losses[positive] = 20 * np.log10(
        4 * math.pi * dists[positive] / LAMBDA_M
    )
    base_losses[0] = float('inf')
    losses = np.broadcast_to(base_losses, heights.shape).copy()
    max_vs = np.full(heights.shape, -999.0, dtype=np.float64)
    viable = losses < usable_threshold_db
    # The scalar implementation requires at least two finite samples.
    viable[:, 0] = False
    if len(dists) <= 2:
        if single_ray:
            return PrefixPathLossResult(losses[0], viable[0], max_vs[0])
        return PrefixPathLossResult(losses, viable, max_vs)

    roughness_loss = np.zeros(heights.shape, dtype=np.float64)
    for ray_index, ray_heights in enumerate(heights):
        roughness_p75 = _running_percentile_75(np.abs(np.diff(ray_heights)))
        # Endpoint i has i height differences in its prefix.
        endpoint_roughness = np.zeros(len(dists), dtype=np.float64)
        endpoint_roughness[3:] = roughness_p75[3:len(dists)]
        roughness_loss[ray_index] = np.minimum(
            6.0,
            np.log1p(np.maximum(0.0, endpoint_roughness))
            * TERRAIN_ROUGHNESS_FACTOR,
        )

    obstacle_indices = np.arange(1, len(dists) - 1, dtype=np.int32)
    obstacle_dists = dists[1:-1]
    endpoint_batch = max(1, min(512, int(endpoint_batch_size)))
    ray_batch = max(1, min(64, int(ray_batch_size)))

    for ray_start in range(0, heights.shape[0], ray_batch):
        ray_end = min(heights.shape[0], ray_start + ray_batch)
        obstacle_heights = heights[ray_start:ray_end, 1:-1]
        for start in range(2, len(dists), endpoint_batch):
            endpoint_indices = np.arange(
                start,
                min(len(dists), start + endpoint_batch),
                dtype=np.int32,
            )
            totals = dists[endpoint_indices]
            receivers = h_rx[ray_start:ray_end, :][:, endpoint_indices]
            d1 = obstacle_dists[None, None, :]
            d2 = totals[None, :, None] - d1
            valid = (
                (obstacle_indices[None, :] < endpoint_indices[:, None])
                & (obstacle_dists[None, :] > 0)
                & ((totals[:, None] - obstacle_dists[None, :]) > 0)
            )

            los_h = h_tx + (
                (receivers[:, :, None] - h_tx)
                * (d1 / totals[None, :, None])
            )
            earth_bulge = (d1 * d2) / (2 * K_FACTOR * R_EARTH_M)
            excess_h = obstacle_heights[:, None, :] + earth_bulge - los_h
            with np.errstate(divide='ignore', invalid='ignore'):
                fresnel_v = excess_h * np.sqrt(
                    2 * totals[None, :, None] / (LAMBDA_M * d1 * d2)
                )
            fresnel_v = np.where(valid[None, :, :], fresnel_v, -np.inf)
            batch_max_v = np.max(fresnel_v, axis=2)
            batch_max_v = np.where(
                np.isfinite(batch_max_v),
                batch_max_v,
                -999.0,
            )
            max_vs[ray_start:ray_end, endpoint_indices] = batch_max_v

            diffraction_loss = np.zeros(batch_max_v.shape, dtype=np.float64)
            diffracted = batch_max_v > -0.78
            v = batch_max_v[diffracted]
            diffraction_loss[diffracted] = np.maximum(
                0.0,
                6.9 + 20 * np.log10(
                    np.sqrt((v - 0.1) ** 2 + 1) + v - 0.1
                ),
            )
            fspl = losses[ray_start:ray_end, :][:, endpoint_indices]
            total_loss = fspl + diffraction_loss
            total_loss += (
                roughness_loss[ray_start:ray_end, :][:, endpoint_indices]
                + PATH_LOSS_BIAS_DB
            )
            bounded_loss = np.maximum(fspl, total_loss)
            losses[ray_start:ray_end, endpoint_indices] = bounded_loss
            viable[ray_start:ray_end, endpoint_indices] = (
                (batch_max_v <= LINK_LOS_MAX_V)
                & (bounded_loss < usable_threshold_db)
            )

    if single_ray:
        return PrefixPathLossResult(losses[0], viable[0], max_vs[0])
    return PrefixPathLossResult(losses, viable, max_vs)


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
) -> PathLossResult:
    cos_mid = math.cos(math.radians((lat1 + lat2) / 2))
    dlat = (lat2 - lat1) * 111_320
    dlon = (lon2 - lon1) * 111_320 * cos_mid
    d_total = math.sqrt(dlat ** 2 + dlon ** 2)

    if d_total < 1.0:
        return PathLossResult(0.0, True, ((0.0, max(0.0, elev1)),))

    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)

    n_samples = max(20, min(200, int(d_total / PROFILE_STEP_M)))

    ds = gdal.Open(vrt_path)
    if ds is None:
        raise RuntimeError('failed to open terrain VRT')

    gt = ds.GetGeoTransform()
    inv_gt = gdal.InvGeoTransform(gt)
    if inv_gt is None:
        ds = None
        raise RuntimeError('terrain VRT has no invertible geotransform')
    band = ds.GetRasterBand(1)
    if band is None:
        ds = None
        raise RuntimeError('terrain VRT has no elevation band')

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
        if data is None:
            ds = None
            raise RuntimeError('failed to sample terrain VRT')
        h = float(data[0][0])
        if not math.isfinite(h) or h < -500 or h > 9_000:
            h = 0.0
        h = max(0.0, h)
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
        include_profile=True,
    )


def compute_path_loss_from_profile(
    dists: np.ndarray,
    heights: np.ndarray,
    h_tx: float,
    h_rx: float,
    *,
    include_profile: bool = True,
) -> PathLossResult:
    def result(path_loss_db: float, viable: bool) -> PathLossResult:
        profile = (
            tuple((round(float(distance), 1), round(float(height), 1)) for distance, height in zip(dists, heights))
            if include_profile
            else ()
        )
        return PathLossResult(float(path_loss_db), bool(viable), profile)

    if len(dists) != len(heights) or len(dists) == 0:
        return result(float('inf'), False)
    finite = np.isfinite(dists) & np.isfinite(heights)
    if np.count_nonzero(finite) < 2:
        return result(float('inf'), False)
    dists = dists[finite]
    heights = heights[finite]
    order = np.argsort(dists)
    dists = dists[order]
    heights = heights[order]
    d_total = float(dists[-1])
    usable_threshold_db = current_usable_path_loss_db()
    if d_total < 1.0:
        return result(0.0, True)

    fspl = 20 * math.log10(4 * math.pi * d_total / LAMBDA_M)

    if len(dists) <= 2:
        viable = fspl < usable_threshold_db
        return result(fspl, viable)

    d1 = dists[1:-1].astype(np.float64)
    d2 = d_total - d1
    valid = (d1 > 0) & (d2 > 0)
    if not np.any(valid):
        viable = fspl < usable_threshold_db
        return result(fspl, viable)

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
    return result(total_loss, viable)
