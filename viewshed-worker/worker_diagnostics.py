"""Worker diagnostics: explicit worker subsystem ownership."""

from __future__ import annotations
from rf_context import current_signal_thresholds_db, current_usable_path_loss_db
import json
import math
import tempfile
from typing import Optional
import numpy as np
import requests
from osgeo import gdal
from rf.config import (
    ANTENNA_HEIGHT_M,
    K_FACTOR,
    LAMBDA_M,
    LINK_LOS_MAX_V,
    PROFILE_STEP_M,
    R_EARTH_M,
    radio_snr_band,
)
from rf.terrain import build_link_vrt, ensure_tiles_for_link
from rf_context import refresh_rf_calibration
from worker_connections import wait_for_db
import logging

log = logging.getLogger("worker")


def resolve_node_ref(db, ref: str, *, context) -> dict:
    ref = str(ref or "").strip()
    if not ref:
        raise ValueError("Empty node reference")

    with db.cursor() as cur:
        if len(ref) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in ref):
            cur.execute(
                "SELECT node_id, name, lat, lon, elevation_m FROM core.nodes WHERE lower(node_id) = lower(%s)",
                (ref,),
            )
            rows = cur.fetchall()
        elif len(ref) >= 6 and all(ch in "0123456789abcdefABCDEF" for ch in ref):
            cur.execute(
                "SELECT node_id, name, lat, lon, elevation_m FROM core.nodes WHERE lower(node_id) LIKE lower(%s) ORDER BY node_id LIMIT 2",
                (f"{ref}%",),
            )
            rows = cur.fetchall()
        else:
            cur.execute(
                """
                SELECT node_id, name, lat, lon, elevation_m
                FROM core.nodes
                WHERE lower(coalesce(name, '')) = lower(%s)
                   OR lower(coalesce(name, '')) LIKE lower(%s)
                ORDER BY node_id
                LIMIT 2
                """,
                (ref, f"%{ref}%"),
            )
            rows = cur.fetchall()

    if len(rows) < 1:
        raise ValueError(f'No node matched "{ref}"')
    if len(rows) > 1:
        raise ValueError(f'Ambiguous node reference "{ref}"')
    node_id, name, lat, lon, elevation_m = rows[0]
    return {
        "node_id": node_id,
        "name": name,
        "lat": lat,
        "lon": lon,
        "elevation_m": elevation_m,
    }


def compute_pair_diagnostics(a: dict, b: dict, *, context) -> dict:
    if (
        a.get("lat") is None
        or a.get("lon") is None
        or b.get("lat") is None
        or b.get("lon") is None
    ):
        raise ValueError("Both nodes need coordinates")

    elev_a = float(a.get("elevation_m") or 0.0)
    elev_b = float(b.get("elevation_m") or 0.0)

    with tempfile.TemporaryDirectory() as tmp_dir:
        ensure_tiles_for_link(
            context.settings.SRTM_DIR,
            float(a["lat"]),
            float(a["lon"]),
            float(b["lat"]),
            float(b["lon"]),
            log,
        )
        vrt = build_link_vrt(
            float(a["lat"]),
            float(a["lon"]),
            float(b["lat"]),
            float(b["lon"]),
            tmp_dir,
            context.settings.SRTM_DIR,
        )

        cos_mid = math.cos(math.radians((float(a["lat"]) + float(b["lat"])) / 2))
        dlat = (float(b["lat"]) - float(a["lat"])) * 111_320
        dlon = (float(b["lon"]) - float(a["lon"])) * 111_320 * cos_mid
        d_total = math.sqrt(dlat**2 + dlon**2)
        n_samples = max(20, min(200, int(d_total / PROFILE_STEP_M)))

        ds = gdal.Open(vrt)
        if ds is None:
            raise RuntimeError("Failed to open generated VRT")
        gt = ds.GetGeoTransform()
        inv_gt = gdal.InvGeoTransform(gt)
        band = ds.GetRasterBand(1)

        heights: list[float] = []
        dists: list[float] = []
        for i in range(n_samples + 1):
            t = i / n_samples
            la = float(a["lat"]) + t * (float(b["lat"]) - float(a["lat"]))
            lo = float(a["lon"]) + t * (float(b["lon"]) - float(a["lon"]))
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
    with np.errstate(divide="ignore", invalid="ignore"):
        vs = excess_h * np.sqrt(2 * (d1 + d2) / (LAMBDA_M * d1 * d2))
    max_v = float(np.max(vs)) if vs.size else -999.0

    if max_v <= -0.78:
        diff_loss = 0.0
    else:
        diff_loss = max(
            0.0, 6.9 + 20 * math.log10(math.sqrt((max_v - 0.1) ** 2 + 1) + max_v - 0.1)
        )

    total_loss = fspl + diff_loss
    clear_los = max_v <= LINK_LOS_MAX_V
    usable_threshold_db = current_usable_path_loss_db(context=context)
    return {
        "node_a_id": a["node_id"],
        "node_a_name": a.get("name"),
        "node_b_id": b["node_id"],
        "node_b_name": b.get("name"),
        "distance_km": round(d_total / 1000.0, 3),
        "fspl_db": round(fspl, 3),
        "diffraction_loss_db": round(diff_loss, 3),
        "total_path_loss_db": round(total_loss, 3),
        "max_v": round(max_v, 6),
        "los_limit_v": LINK_LOS_MAX_V,
        "clear_los": clear_los,
        "usable_threshold_db": usable_threshold_db,
        "viable": bool(clear_los and total_loss < usable_threshold_db),
    }


def run_test_mode(args, *, context) -> None:
    db = wait_for_db(context=context)
    try:
        refresh_rf_calibration(db, force=True, context=context)
        refs: list[tuple[str, str, Optional[dict]]] = []

        if args.test_pairs:
            for raw in args.test_pairs.split(","):
                raw = raw.strip()
                if not raw:
                    continue
                left, sep, right = raw.partition(":")
                if not sep:
                    raise ValueError(f'Invalid pair "{raw}" — expected A:B')
                refs.append((left.strip(), right.strip(), None))

        if args.test_from_node:
            response = requests.get(
                f"{context.settings.RADIO_BOT_URL}/state", timeout=10
            )
            response.raise_for_status()
            payload = response.json()
            monitors = payload.get("monitors") if isinstance(payload, dict) else None
            if not isinstance(monitors, list):
                raise RuntimeError("Radio bot state missing monitors list")
            source_ref = str(args.test_from_node).strip().lower()
            selected = None
            for monitor in monitors:
                if not isinstance(monitor, dict):
                    continue
                candidates = [
                    str(monitor.get("fullPublicKey") or "").strip().lower(),
                    str(monitor.get("targetHex") or "").strip().lower(),
                    str(monitor.get("nodeName") or "").strip().lower(),
                    str(monitor.get("label") or "").strip().lower(),
                ]
                if any(
                    source_ref and source_ref in candidate
                    for candidate in candidates
                    if candidate
                ):
                    selected = monitor
                    break
            if selected is None:
                raise ValueError(f'No radio monitor matched "{args.test_from_node}"')
            reporter_ref = str(
                selected.get("fullPublicKey") or selected.get("targetHex") or ""
            ).strip()
            neighbours = selected.get("lastNeighbours")
            if not reporter_ref or not isinstance(neighbours, list):
                raise RuntimeError("Selected radio monitor has no neighbour data")
            for neighbour in neighbours:
                if not isinstance(neighbour, dict):
                    continue
                peer_ref = str(neighbour.get("fullPublicKey") or "").strip()
                if len(peer_ref) != 64:
                    continue
                refs.append((reporter_ref, peer_ref, neighbour))

        if not refs:
            raise ValueError("No test pairs were provided")

        results = []
        for left_ref, right_ref, neighbour_meta in refs:
            a = resolve_node_ref(db, left_ref, context=context)
            b = resolve_node_ref(db, right_ref, context=context)
            result = compute_pair_diagnostics(a, b, context=context)
            if neighbour_meta is not None:
                reported_snr = neighbour_meta.get("snrDb")
                result["reported_snr_db"] = reported_snr
                result["reported_quality_band"] = radio_snr_band(
                    reported_snr if isinstance(reported_snr, (int, float)) else None
                )
                result["reported_heard_seconds_ago"] = neighbour_meta.get(
                    "heardSecondsAgo"
                )
                result["reported_adv_name"] = neighbour_meta.get("advName")
            results.append(result)

        print(
            json.dumps(
                {
                    "usable_threshold_db": current_usable_path_loss_db(context=context),
                    "signal_thresholds_db": current_signal_thresholds_db(
                        context=context
                    ),
                    "results": results,
                },
                indent=2,
            )
        )
    finally:
        db.close()
