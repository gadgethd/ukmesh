"""Worker math: explicit worker subsystem ownership."""

from __future__ import annotations
import math
import resource
import time
from typing import Callable, Optional
import numpy as np
from worker_types import Cancelled, JobDeadlineExceeded


def peak_rss_mb() -> float:
    # Linux reports ru_maxrss in KiB; the production worker image is Linux.
    return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0


def check_job_budget(
    deadline_monotonic: Optional[float],
    cancellation_check: Optional[Callable[[], None]] = None,
    *,
    context,
) -> None:
    if cancellation_check is not None:
        try:
            cancellation_check()
        except Cancelled:
            raise
        except Exception as exc:
            raise Cancelled(str(exc)) from exc
    if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
        raise JobDeadlineExceeded("coverage job deadline exceeded")


def is_viewshed_eligible_coordinate(lat: float, lon: float, *, context) -> bool:
    if not math.isfinite(lat) or not math.isfinite(lon):
        return False
    if abs(lat) < 1e-9 and abs(lon) < 1e-9:
        return False
    return (
        context.settings.UK_LAT_MIN <= lat <= context.settings.UK_LAT_MAX
        and context.settings.UK_LON_MIN <= lon <= context.settings.UK_LON_MAX
    )


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    if values.size < 1:
        raise ValueError("No values provided")
    order = np.argsort(values)
    v = values[order]
    w = weights[order]
    cumulative = np.cumsum(w)
    target = float(np.clip(q, 0.0, 1.0)) * cumulative[-1]
    idx = int(np.searchsorted(cumulative, target, side="left"))
    idx = max(0, min(idx, len(v) - 1))
    return float(v[idx])


def project_xy_km(latitudes, longitudes, *, context) -> np.ndarray:
    lats = np.asarray(latitudes, dtype=np.float64)
    lons = np.asarray(longitudes, dtype=np.float64)
    cos_ref = math.cos(math.radians(context.settings.SUPPORT_PROJECTION_LAT))
    return np.column_stack((lons * 111.32 * cos_ref, lats * 111.32))


def node_dist_km(a: dict, b: dict) -> float:
    cos_m = math.cos(math.radians((a["lat"] + b["lat"]) / 2))
    return math.sqrt(
        ((a["lat"] - b["lat"]) * 111.32) ** 2
        + ((a["lon"] - b["lon"]) * 111.32 * cos_m) ** 2
    )


def physical_candidate_radius_km(radius_m: Optional[float], *, context) -> float:
    derived = (
        (radius_m / 1000.0) * context.settings.RF_SOURCE_LINK_RADIUS_MULTIPLIER
        if radius_m is not None
        else context.settings.DEFAULT_PHYSICAL_LINK_RADIUS_KM
    )
    return min(
        context.settings.MAX_PHYSICAL_LINK_RADIUS_KM,
        max(context.settings.MIN_PHYSICAL_LINK_RADIUS_KM, derived),
    )
