"""Coverage model: explicit worker subsystem ownership."""

from __future__ import annotations
from rf_snapshots import UNLOADED_MAINLAND
from rf_context import current_signal_thresholds_db, current_usable_path_loss_db
import math
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional
import numpy as np
from osgeo import gdal
from rf.config import ANTENNA_HEIGHT_M, COVERAGE_TARGET_HEIGHT_M, K_FACTOR, R_EARTH_M
from rf.loss import compute_prefix_path_losses
from rf.terrain import (
    PermanentOutOfScope,
    RetryableTerrainError,
    build_vrt,
    ensure_tiles,
    ensure_tiles_for_radius,
    load_uk_mainland,
    raster_window_for_radius,
    radio_horizon_m,
    sample_elevation,
)
from rf_context import source_support_radius_m, support_penalty_db
from worker_math import check_job_budget, is_viewshed_eligible_coordinate, peak_rss_mb
from worker_types import Computed, InvalidJob
import logging

log = logging.getLogger("worker")
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shapely.geometry import Polygon as ShapelyPolygon


def rf_search_radius_m(base_radius_m: float, *, context) -> float:
    """Return the exact radius both terrain acquisition and RF rays must cover."""
    return min(
        context.settings.MAX_RADIUS_M,
        max(
            base_radius_m * context.settings.RF_RADIUS_MULTIPLIER,
            context.settings.RF_MIN_RADIUS_M,
        ),
    )


def approximate_dtm_in_place(elevation: np.ndarray) -> np.ndarray:
    """Strip narrow DSM spikes without allocating another full-size raster."""
    if elevation.ndim != 2 or elevation.dtype not in (
        np.dtype(np.int16),
        np.dtype(np.float32),
    ):
        raise ValueError("DTM raster must be a two-dimensional int16 or float32 array")
    from scipy.ndimage import minimum_filter

    minimum_filter(elevation, size=9, output=elevation)
    return elevation


def resolve_rf_radial_boundaries(
    node_id: str,
    lat: float,
    lon: float,
    elev: np.ndarray,
    gt: tuple[float, float, float, float, float, float],
    observer_h: float,
    base_radius_m: float,
    *,
    deadline_monotonic: Optional[float] = None,
    cancellation_check: Optional[Callable[[], None]] = None,
    raster_x_offset: int = 0,
    raster_y_offset: int = 0,
    context,
) -> tuple[dict[str, list[tuple[float, float]]], float]:
    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    search_radius_m = rf_search_radius_m(base_radius_m, context=context)
    n_rows, n_cols = elev.shape
    dpmlat = 1.0 / 111_320.0
    dpmlon = 1.0 / (111_320.0 * math.cos(math.radians(lat)))
    ds_arr = np.arange(
        context.settings.RF_RADIAL_STEP_M,
        search_radius_m + context.settings.RF_RADIAL_STEP_M,
        context.settings.RF_RADIAL_STEP_M,
        dtype=np.float32,
    )
    thetas = np.linspace(
        0.0, 2.0 * math.pi, context.settings.RF_N_RAYS, endpoint=False, dtype=np.float32
    )
    cos_t = np.cos(thetas)
    sin_t = np.sin(thetas)

    signal_thresholds = current_signal_thresholds_db(context=context)
    boundaries: dict[str, list[tuple[float, float]]] = {
        key: [] for key in signal_thresholds
    }
    max_reached = 0.0

    # Materialise coordinates, raster samples, and support penalties once per
    # job. Prefix path loss remains ray-specific, but runs in bounded NumPy
    # endpoint batches instead of rebuilding every preceding prefix in Python.
    pt_lats = lat + sin_t[:, None] * ds_arr[None, :] * dpmlat
    pt_lons = lon + cos_t[:, None] * ds_arr[None, :] * dpmlon
    # Convert in the original VRT pixel space before subtracting the integer
    # read-window origin. Recomputing against a shifted floating-point origin
    # can move samples on exact pixel boundaries by one cell.
    pxs = np.clip(
        ((pt_lons - gt[0]) / gt[1]).astype(np.int32) - raster_x_offset,
        0,
        n_cols - 1,
    )
    pys = np.clip(
        ((pt_lats - gt[3]) / gt[5]).astype(np.int32) - raster_y_offset,
        0,
        n_rows - 1,
    )
    ray_heights = elev[pys, pxs].astype(np.float32)
    support_penalties = support_penalty_db(
        node_id, pt_lats.reshape(-1), pt_lons.reshape(-1), context=context
    ).reshape(pt_lats.shape)

    for ray_start in range(
        0, context.settings.RF_N_RAYS, context.settings.RF_PREFIX_RAY_BATCH
    ):
        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        ray_end = min(
            context.settings.RF_N_RAYS, ray_start + context.settings.RF_PREFIX_RAY_BATCH
        )
        ray_slice = slice(ray_start, ray_end)
        heights = ray_heights[ray_slice]
        prefix_result = compute_prefix_path_losses(
            ds_arr,
            heights,
            observer_h,
            heights + COVERAGE_TARGET_HEIGHT_M,
            endpoint_batch_size=context.settings.RF_PREFIX_ENDPOINT_BATCH,
            ray_batch_size=context.settings.RF_PREFIX_RAY_BATCH,
            usable_threshold_db=current_usable_path_loss_db(context=context),
        )
        effective_losses = prefix_result.path_loss_db + support_penalties[ray_slice]

        for band, threshold in signal_thresholds.items():
            passing = effective_losses <= threshold
            any_passing = np.any(passing, axis=1)
            last_passing = (
                len(ds_arr)
                - 1
                - np.argmax(
                    passing[:, ::-1],
                    axis=1,
                )
            )
            end_dists = np.where(
                any_passing,
                ds_arr[last_passing],
                ds_arr[0],
            )
            if band == "red":
                max_reached = max(max_reached, float(np.max(end_dists)))
            boundaries[band].extend(
                zip(
                    lon + cos_t[ray_slice] * end_dists * dpmlon,
                    lat + sin_t[ray_slice] * end_dists * dpmlat,
                )
            )

    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    for band_boundary in boundaries.values():
        if band_boundary:
            band_boundary.append(band_boundary[0])
    return boundaries, max_reached


def clip_and_simplify_polygon(poly, *, context) -> Optional[dict]:
    from shapely.geometry import mapping

    if poly.is_empty:
        return None
    if not poly.is_valid:
        poly = poly.buffer(0)
    mainland = get_uk_mainland(context=context)
    if mainland is not None:
        poly = poly.intersection(mainland)
        if poly.is_empty:
            return None
    result = poly.simplify(context.settings.SIMPLIFY_DEG, preserve_topology=True)
    if result.is_empty or result.geom_type not in ("Polygon", "MultiPolygon"):
        return None
    return mapping(result)


def require_mapped_coverage(
    node_id: str, geom: Optional[dict], model_name: str, *, context
) -> dict:
    """Return mapped coverage or classify a stable land-mask miss as terminal."""
    if geom is None:
        raise PermanentOutOfScope(
            f"{node_id}: {model_name} coverage does not intersect the map land mask"
        )
    return geom


def _polygonal_geometry(poly, *, context):
    if poly is None or poly.is_empty:
        return None
    if not poly.is_valid:
        poly = poly.buffer(0)
    if poly.is_empty:
        return None
    if poly.geom_type in ("Polygon", "MultiPolygon"):
        return poly
    if poly.geom_type == "GeometryCollection":
        polygon_parts = [
            part
            for part in poly.geoms
            if part.geom_type in ("Polygon", "MultiPolygon") and not part.is_empty
        ]
        if polygon_parts:
            from shapely.ops import unary_union

            return unary_union(polygon_parts)
    return None


def _clipped_simplified_polygon(poly, *, context):
    from shapely.geometry import shape as shapely_shape

    if poly is None:
        return None
    geom = clip_and_simplify_polygon(poly, context=context)
    return (
        _polygonal_geometry(shapely_shape(geom), context=context)
        if geom is not None
        else None
    )


def build_exclusive_strength_geoms(
    band_polys: dict[str, ShapelyPolygon],
    *,
    red_outer_geom: Optional[dict] = None,
    context,
) -> dict[str, dict]:
    """Convert nested strength polygons into exclusive green/amber/red areas.

    The strongest band should own the fill for a location. Without this, the
    frontend ends up stacking green over amber over red and the center reads as
    muddy yellow instead of a clean strength gradient.
    """
    import shapely
    from shapely.geometry import mapping, shape as shapely_shape

    red_outer = (
        _polygonal_geometry(shapely_shape(red_outer_geom), context=context)
        if red_outer_geom is not None
        else _clipped_simplified_polygon(band_polys.get("red"), context=context)
    )
    if red_outer is None:
        return {}

    amber_outer = _clipped_simplified_polygon(band_polys.get("amber"), context=context)
    if amber_outer is not None:
        amber_outer = _polygonal_geometry(
            shapely.intersection(
                amber_outer,
                red_outer,
                grid_size=context.settings.GEOMETRY_GRID_DEG,
            ),
            context=context,
        )

    green_outer = _clipped_simplified_polygon(band_polys.get("green"), context=context)
    if green_outer is not None:
        green_limit = amber_outer if amber_outer is not None else red_outer
        green_outer = _polygonal_geometry(
            shapely.intersection(
                green_outer,
                green_limit,
                grid_size=context.settings.GEOMETRY_GRID_DEG,
            ),
            context=context,
        )

    exclusive_shapes: dict[str, object] = {}
    occupied = None
    if green_outer is not None:
        exclusive_shapes["green"] = green_outer
        occupied = green_outer
    if amber_outer is not None:
        amber_only = _polygonal_geometry(
            shapely.difference(
                amber_outer,
                occupied,
                grid_size=context.settings.GEOMETRY_GRID_DEG,
            )
            if occupied is not None
            else amber_outer,
            context=context,
        )
        if amber_only is not None:
            exclusive_shapes["amber"] = amber_only
            occupied = (
                shapely.union_all(
                    (occupied, amber_only),
                    grid_size=context.settings.GEOMETRY_GRID_DEG,
                )
                if occupied is not None
                else amber_only
            )
    red_only = _polygonal_geometry(
        shapely.difference(
            red_outer,
            occupied,
            grid_size=context.settings.GEOMETRY_GRID_DEG,
        )
        if occupied is not None
        else red_outer,
        context=context,
    )
    if red_only is not None:
        exclusive_shapes["red"] = red_only
    return {name: mapping(poly) for name, poly in exclusive_shapes.items()}


def get_uk_mainland(*, context):
    if context.mainland is UNLOADED_MAINLAND:
        context.mainland = load_uk_mainland(Path(__file__).parent, log)
    return context.mainland


def calculate_viewshed(
    node_id: str,
    lat: float,
    lon: float,
    antenna_height_m: float = ANTENNA_HEIGHT_M,
    *,
    deadline_monotonic: Optional[float] = None,
    cancellation_check: Optional[Callable[[], None]] = None,
    context,
) -> Computed:
    from shapely.geometry import Polygon as ShapelyPolygon

    calculation_started = time.monotonic()
    stage_seconds: dict[str, float] = {}

    def record_stage(name: str, started: float) -> None:
        stage_seconds[name] = round(time.monotonic() - started, 6)

    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    if not is_viewshed_eligible_coordinate(lat, lon, context=context):
        raise PermanentOutOfScope(
            f"coordinate outside UK coverage bounds at ({lat:.4f}, {lon:.4f})"
        )
    with tempfile.TemporaryDirectory() as tmp:
        # 1. Download the observer's own tile and sample terrain elevation.
        #    This single tile is sufficient to determine node height; we need
        #    it before we know how far to reach for surrounding tiles.
        stage_started = time.monotonic()
        obs_tile = (math.floor(lat), math.floor(lon))
        obs_path = ensure_tiles(
            context.settings.SRTM_DIR,
            (obs_tile,),
            log,
            required_tiles=(obs_tile,),
            max_tiles=1,
        )[0]
        record_stage("observer_tile_acquisition", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)

        stage_started = time.monotonic()
        obs_vrt = f"{tmp}/observer.vrt"
        build_vrt((obs_path,), obs_vrt)
        elevation_m = sample_elevation(obs_vrt, lat, lon)
        record_stage("observer_vrt_and_sample", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)

        # 2. Radio-horizon radius: node ASL + per-node antenna height.
        effective_height_m = elevation_m + antenna_height_m
        radius_m = min(
            radio_horizon_m(effective_height_m), context.settings.MAX_RADIUS_M
        )
        radius_m = source_support_radius_m(node_id, radius_m, context=context)
        log.info(
            f"  {node_id[:12]}…: elevation={elevation_m:.0f} m ASL, "
            f"antenna={effective_height_m:.0f} m, horizon={radius_m / 1000:.1f} km"
        )

        # 3. Download all tiles covering the computed horizon radius
        stage_started = time.monotonic()
        terrain_radius_m = (
            rf_search_radius_m(radius_m, context=context)
            if context.settings.COVERAGE_MODEL == "rf_radial_100m"
            else radius_m
        )
        paths = ensure_tiles_for_radius(
            context.settings.SRTM_DIR,
            lat,
            lon,
            terrain_radius_m,
            log,
        )
        record_stage("coverage_tile_acquisition", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)

        # 4. Merge tiles into a single VRT
        stage_started = time.monotonic()
        vrt = f"{tmp}/input.vrt"
        build_vrt(paths, vrt)
        record_stage("coverage_vrt_construction", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)

        # 5. Read only the radius-bounded elevation window into memory once.
        #    NODATA ocean pixels (INT16 -32768) are clamped to 0 — treated as sea level.
        stage_started = time.monotonic()
        ds = gdal.Open(vrt)
        if ds is None:
            raise RetryableTerrainError("GDAL could not open the coverage VRT")
        full_gt = ds.GetGeoTransform()  # (x_origin, px_lon, 0, y_origin, 0, px_lat)
        band = ds.GetRasterBand(1)
        if band is None:
            ds = None
            raise RetryableTerrainError("coverage VRT has no elevation band")
        x_offset, y_offset, x_size, y_size, _window_gt = raster_window_for_radius(
            full_gt,
            ds.RasterXSize,
            ds.RasterYSize,
            lat,
            lon,
            terrain_radius_m,
            # minimum_filter(size=9) needs four real neighbouring pixels at
            # the coverage edge. One extra pixel also absorbs float-to-index
            # rounding at the final radial step.
            margin_pixels=5,
        )
        elev = band.ReadAsArray(x_offset, y_offset, x_size, y_size)
        if elev is None:
            ds = None
            raise RetryableTerrainError(
                "GDAL could not read the coverage elevation raster"
            )
        # SRTM is signed int16. Keep the full radius window in that native
        # representation through the DTM filter; only the bounded ray samples
        # are promoted to float32 later. This halves the dominant per-job
        # raster allocation without changing any elevation values.
        if elev.dtype != np.int16:
            elev = elev.astype(np.int16)
        np.maximum(elev, 0, out=elev)
        n_rows, n_cols = elev.shape
        ds = None
        record_stage("terrain_raster_read", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)

        # 5b. Approximate DTM from SRTM DSM via spatial minimum filter.
        #     SRTM is a Digital Surface Model — building heights corrupt urban
        #     areas causing raycasting to terminate within metres of the observer.
        #     A 9-pixel (~270 m for SRTM1 at 30 m/px) minimum filter strips
        #     building-height spikes while preserving genuine terrain features
        #     (hills, ridges) whose footprints are wider than ~270 m.
        stage_started = time.monotonic()
        approximate_dtm_in_place(elev)

        # 5c. Re-sample observer elevation from the DTM-approximated raster.
        #     This corrects the radio-horizon radius when SRTM reads building tops.
        obs_px = int(
            np.clip(
                int((lon - full_gt[0]) / full_gt[1]) - x_offset,
                0,
                n_cols - 1,
            )
        )
        obs_py = int(
            np.clip(
                int((lat - full_gt[3]) / full_gt[5]) - y_offset,
                0,
                n_rows - 1,
            )
        )
        dtm_elev = float(elev[obs_py, obs_px])
        # Guard against coastal bleed-in: min filter near shoreline may return 0
        # (ocean NODATA) even for land pixels.  Fall back to raw SRTM in that case.
        if dtm_elev > 0.0 or elevation_m <= 0.0:
            elevation_m = dtm_elev
        record_stage("terrain_filter_and_origin_sample", stage_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        effective_height_m = elevation_m + antenna_height_m
        radius_m = min(
            radio_horizon_m(effective_height_m), context.settings.MAX_RADIUS_M
        )
        radius_m = source_support_radius_m(node_id, radius_m, context=context)
        log.info(
            f"  {node_id[:12]}… DTM elevation={elevation_m:.0f} m ASL, "
            f"horizon={radius_m / 1000:.1f} km"
        )

        observer_h = elevation_m + antenna_height_m
        strength_geoms: dict[str, dict] = {}
        radial_started = time.monotonic()
        if context.settings.COVERAGE_MODEL == "terrain_los":
            # Vectorised raycasting terrain line-of-sight model.
            dpmlat = 1.0 / 111_320.0  # deg/m northward
            dpmlon = 1.0 / (111_320.0 * math.cos(math.radians(lat)))  # deg/m eastward
            R_eff_2 = 2.0 * K_FACTOR * R_EARTH_M  # 2kR curvature denom

            n_steps = max(1, int(radius_m / context.settings.STEP_M))
            ds_arr = np.linspace(
                context.settings.STEP_M, radius_m, n_steps
            )  # (M,) distances in metres
            thetas = np.linspace(
                0.0, 2.0 * math.pi, context.settings.N_RAYS, endpoint=False
            )  # (N,) angles

            # Ray sample coordinates: (N, M)
            sin_t = np.sin(thetas)[:, None]  # (N, 1)
            cos_t = np.cos(thetas)[:, None]  # (N, 1)
            pt_lats = lat + sin_t * ds_arr[None, :] * dpmlat  # (N, M)
            pt_lons = lon + cos_t * ds_arr[None, :] * dpmlon  # (N, M)

            # Pixel indices — clamped to raster bounds (N, M)
            pxs = np.clip(
                ((pt_lons - full_gt[0]) / full_gt[1]).astype(np.int32) - x_offset,
                0,
                n_cols - 1,
            )
            pys = np.clip(
                ((pt_lats - full_gt[3]) / full_gt[5]).astype(np.int32) - y_offset,
                0,
                n_rows - 1,
            )

            # Terrain heights at each ray step: (N, M)
            hs = elev[pys, pxs]

            # Angles with Earth-curvature correction: (N, M)
            curvature = ds_arr[None, :] ** 2 / R_eff_2
            terrain_angles = (hs - observer_h - curvature) / ds_arr[None, :]
            target_angles = (
                (hs + COVERAGE_TARGET_HEIGHT_M) - observer_h - curvature
            ) / ds_arr[None, :]

            running_max = np.maximum.accumulate(terrain_angles, axis=1)
            prev_max = np.concatenate(
                [np.full((context.settings.N_RAYS, 1), -np.inf), running_max[:, :-1]],
                axis=1,
            )
            in_shadow = target_angles + context.settings.ANGLE_EPS < prev_max

            has_shadow = in_shadow.any(axis=1)
            first_shad = np.where(has_shadow, in_shadow.argmax(axis=1), n_steps)
            last_js = np.clip(first_shad - 1, 0, n_steps - 1)
            last_ds = ds_arr[last_js]

            lons_b = lon + np.cos(thetas) * last_ds * dpmlon
            lats_b = lat + np.sin(thetas) * last_ds * dpmlat
            boundary = list(zip(lons_b.tolist(), lats_b.tolist()))
            boundary.append(boundary[0])
            poly = ShapelyPolygon(boundary)
            geom = require_mapped_coverage(
                node_id,
                clip_and_simplify_polygon(poly, context=context),
                "terrain",
                context=context,
            )
            strength_geoms = {"green": geom}
        elif context.settings.COVERAGE_MODEL == "rf_radial_100m":
            band_boundaries, radius_m = resolve_rf_radial_boundaries(
                node_id,
                lat,
                lon,
                elev,
                full_gt,
                observer_h,
                radius_m,
                deadline_monotonic=deadline_monotonic,
                cancellation_check=cancellation_check,
                raster_x_offset=x_offset,
                raster_y_offset=y_offset,
                context=context,
            )
            record_stage("radial_calculation", radial_started)
            check_job_budget(deadline_monotonic, cancellation_check, context=context)
            polygon_started = time.monotonic()
            raw_band_polys: dict[str, ShapelyPolygon] = {}
            for band, band_boundary in band_boundaries.items():
                if len(band_boundary) < 4:
                    continue
                band_poly = ShapelyPolygon(band_boundary)
                if not band_poly.is_empty:
                    raw_band_polys[band] = band_poly
            geom = require_mapped_coverage(
                node_id,
                (
                    clip_and_simplify_polygon(
                        raw_band_polys.get("red"), context=context
                    )
                    if raw_band_polys.get("red") is not None
                    else None
                ),
                "RF",
                context=context,
            )
            strength_geoms = build_exclusive_strength_geoms(
                raw_band_polys, red_outer_geom=geom, context=context
            )
            record_stage("polygonization_and_clipping", polygon_started)
        else:
            raise InvalidJob(
                f"Unknown COVERAGE_MODEL={context.settings.COVERAGE_MODEL}"
            )
        if "radial_calculation" not in stage_seconds:
            record_stage("radial_calculation", radial_started)
        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        stage_seconds["calculation_total"] = round(
            time.monotonic() - calculation_started,
            6,
        )
        return Computed(
            geom,
            strength_geoms,
            radius_m,
            elevation_m,
            stage_seconds,
            peak_rss_mb(),
        )
