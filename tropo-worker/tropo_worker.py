#!/usr/bin/env python3
"""UKMesh tropo forecast worker.

Fetches NOAA/NCEP GFS 0.25 degree pressure-level fields for the UK, computes a
low-level modified-refractivity (M) gradient, and writes one transparent PNG
per 3-hour forecast step plus an index.json manifest.

The index is a 0-150 "ducting potential" scale:
    index 0   = standard atmosphere (dM/dz >= 110 N/km)
    index 100 = ducting threshold (dM/dz <= 0)
    index 150 = very strong duct (dM/dz <= -55 N/km)

Data is public domain (NOAA NODD); attribution is requested, not required.
No F5LEN or other third-party tropo product is used.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
from PIL import Image

try:
    import eccodes
except ImportError as exc:  # pragma: no cover
    raise SystemExit("eccodes is required (pip install eccodes)") from exc

FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
USER_AGENT = "ukmesh-tropo/1.0 (ukmesh.com; amateur radio non-commercial)"
LEVELS = (1000, 975, 950, 925, 900, 850)
BBOX = {"north": 62.0, "south": 49.0, "west": -12.0, "east": 5.0}
LAND_BOUNDARY = Path(__file__).with_name("uk-boundary.geojson")
FORECAST_HOURS = tuple(range(0, 73, 3))
SCALE = 8  # PNG upscale factor for smooth rendering

# Colour stops: (index, (r, g, b, a))
COLOUR_STOPS = (
    (0, (0, 0, 0, 0)),
    (40, (0, 196, 255, 0)),
    (55, (0, 196, 255, 95)),
    (70, (34, 197, 94, 135)),
    (85, (251, 191, 36, 170)),
    (100, (255, 159, 67, 205)),
    (115, (239, 68, 68, 230)),
    (130, (217, 70, 239, 245)),
    (150, (217, 70, 239, 245)),
)


def log(message: str) -> None:
    print(f"[tropo] {message}", flush=True)


def candidate_cycles(now: dt.datetime | None = None):
    """Yield the most recent GFS cycles that should be published."""
    now = now or dt.datetime.now(dt.timezone.utc)
    # GFS output for a cycle is normally complete about 4.5 h after the cycle.
    anchor = (now - dt.timedelta(hours=4, minutes=30)).replace(
        minute=0, second=0, microsecond=0
    )
    anchor = anchor.replace(hour=(anchor.hour // 6) * 6)
    for offset in range(0, 4):
        yield anchor - dt.timedelta(hours=6 * offset)


def fetch_grib(cycle: dt.datetime, forecast_hour: int, timeout: int = 90) -> bytes:
    params = [
        ("file", f"gfs.t{cycle:%H}z.pgrb2.0p25.f{forecast_hour:03d}"),
        ("dir", f"/gfs.{cycle:%Y%m%d}/{cycle:%H}/atmos"),
        ("var_TMP", "on"),
        ("var_RH", "on"),
        ("var_HGT", "on"),
        ("subregion", ""),
        ("leftlon", str(BBOX["west"])),
        ("rightlon", str(BBOX["east"])),
        ("toplat", str(BBOX["north"])),
        ("bottomlat", str(BBOX["south"])),
    ]
    for level in LEVELS:
        params.append((f"lev_{level}_mb", "on"))
    url = FILTER_URL + "?" + urllib.parse.urlencode(params)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = response.read()
            if len(data) < 10_000 or not data.startswith(b"GRIB"):
                raise ValueError(f"unexpected response ({len(data)} bytes)")
            return data
        except Exception as exc:  # noqa: BLE001 - retry all transport/format errors
            last_error = exc
            if attempt < 2:
                time.sleep(2)
    raise RuntimeError(f"GFS f{forecast_hour:03d} fetch failed: {last_error}")


def pick_cycle(explicit: dt.datetime | None = None) -> dt.datetime:
    if explicit is not None:
        return explicit
    for cycle in candidate_cycles():
        try:
            fetch_grib(cycle, 0, timeout=45)
            log(f"using GFS cycle {cycle:%Y-%m-%d %HZ}")
            return cycle
        except Exception as exc:  # noqa: BLE001 - try the previous cycle
            log(f"cycle {cycle:%Y-%m-%d %HZ} unavailable: {exc}")
    raise RuntimeError("no recent GFS cycle is available")


def parse_grib(data: bytes) -> tuple[dict[tuple[int, str], np.ndarray], dict[str, float]]:
    fields: dict[tuple[int, str], np.ndarray] = {}
    meta: dict[str, float] = {}
    path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as handle:
            handle.write(data)
            path = handle.name
        with open(path, "rb") as handle:
            while True:
                gid = eccodes.codes_grib_new_from_file(handle)
                if gid is None:
                    break
                try:
                    short_name = eccodes.codes_get(gid, "shortName")
                    level = int(eccodes.codes_get(gid, "level"))
                    ni = int(eccodes.codes_get(gid, "Ni"))
                    nj = int(eccodes.codes_get(gid, "Nj"))
                    values = np.asarray(eccodes.codes_get_values(gid), dtype=np.float64)
                    fields[(level, short_name)] = values.reshape(nj, ni)
                    if not meta:
                        meta = {
                            "lat0": float(eccodes.codes_get(gid, "latitudeOfFirstGridPointInDegrees")),
                            "lon0": float(eccodes.codes_get(gid, "longitudeOfFirstGridPointInDegrees")),
                            "dlat": float(eccodes.codes_get(gid, "jDirectionIncrementInDegrees")),
                            "dlon": float(eccodes.codes_get(gid, "iDirectionIncrementInDegrees")),
                            "ni": ni,
                            "nj": nj,
                        }
                finally:
                    eccodes.codes_release(gid)
    finally:
        if path and os.path.exists(path):
            os.unlink(path)
    missing = [(level, short) for level in LEVELS for short in ("t", "r", "gh")
               if (level, short) not in fields]
    if missing:
        raise ValueError(f"GRIB missing fields: {missing}")
    return fields, meta


def compute_index(fields: dict[tuple[int, str], np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """Return (ducting_potential_index, duct_strength_N_per_km)."""
    levels = sorted({level for level, _ in fields})
    temperatures = np.stack([fields[(level, "t")] for level in levels])
    humidities = np.stack([fields[(level, "r")] for level in levels])
    heights = np.stack([fields[(level, "gh")] for level in levels])

    # Refractivity N (ITU-R P.453) and modified refractivity M.
    t_c = temperatures - 273.15
    saturation = 6.112 * np.exp(17.67 * t_c / (t_c + 243.5))
    vapour = (humidities / 100.0) * saturation
    n_units = 77.6 * (np.asarray(levels)[:, None, None] / temperatures) \
        + 3.73e5 * (vapour / (temperatures * temperatures))
    m_units = n_units + 0.157 * heights

    gradients = []
    for index in range(len(levels) - 1):
        dz = heights[index + 1] - heights[index]
        with np.errstate(divide="ignore", invalid="ignore"):
            gradient = (m_units[index + 1] - m_units[index]) / dz * 1000.0
        gradients.append(np.where(np.abs(dz) > 1.0, gradient, np.nan))
    minimum_gradient = np.nanmin(np.stack(gradients), axis=0)

    index = np.clip((110.0 - minimum_gradient) / 110.0 * 100.0, 0.0, 150.0)
    duct_strength = np.maximum(0.0, -minimum_gradient)
    return index, duct_strength


def colour_lut() -> np.ndarray:
    lut = np.zeros((151, 4), dtype=np.uint8)
    for value in range(151):
        for (low, low_colour), (high, high_colour) in zip(COLOUR_STOPS, COLOUR_STOPS[1:]):
            if low <= value <= high:
                span = max(1, high - low)
                fraction = (value - low) / span
                lut[value] = [
                    int(round(low_colour[channel] + (high_colour[channel] - low_colour[channel]) * fraction))
                    for channel in range(4)
                ]
                break
    return lut


def render_png(index: np.ndarray, path: str) -> None:
    height, width = index.shape
    # GFS rows run south -> north (row 0 = 49N); map images need north at the top.
    smooth = Image.fromarray(np.flipud(index).astype(np.float32), mode="F").resize(
        (width * SCALE, height * SCALE), Image.Resampling.BICUBIC
    )
    values = np.clip(np.rint(np.asarray(smooth)), 0, 150).astype(np.uint8)
    rgba = colour_lut()[values]
    Image.fromarray(rgba, mode="RGBA").save(path, optimize=True)


def point_in_ring(lon: float, lat: float, ring: list[list[float]]) -> bool:
    """Ray-cast a point against one polygon ring (lon/lat degrees)."""
    inside = False
    previous = len(ring) - 1
    for current in range(len(ring)):
        x_current, y_current = ring[current][0], ring[current][1]
        x_previous, y_previous = ring[previous][0], ring[previous][1]
        if (y_current > lat) != (y_previous > lat):
            crossing = (x_previous - x_current) * (lat - y_current) / (y_previous - y_current) + x_current
            if lon < crossing:
                inside = not inside
        previous = current
    return inside


def load_land_mask(meta: dict[str, float]) -> np.ndarray:
    """Rasterise UK land at GFS grid-cell centres (row 0 = south)."""
    with open(LAND_BOUNDARY, encoding="utf-8") as handle:
        boundary = json.load(handle)
    rings: list[list[list[float]]] = []
    for feature in boundary.get("features", []):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") == "Polygon":
            polygons = [geometry["coordinates"]]
        elif geometry.get("type") == "MultiPolygon":
            polygons = geometry["coordinates"]
        else:
            continue
        for polygon in polygons:
            rings.append(polygon[0])
    if not rings:
        raise ValueError("UK boundary contains no polygons")
    width = int(meta["ni"])
    height = int(meta["nj"])
    mask = np.zeros((height, width), dtype=bool)
    for row in range(height):
        lat = meta["lat0"] + row * meta["dlat"]
        for column in range(width):
            lon = (meta["lon0"] + column * meta["dlon"] + 180.0) % 360.0 - 180.0
            mask[row, column] = any(point_in_ring(lon, lat, ring) for ring in rings)
    if not mask.any():
        raise ValueError("UK land mask is empty")
    return mask


def peak_location(index: np.ndarray, meta: dict[str, float], mask: np.ndarray | None = None) -> dict[str, float]:
    if mask is not None and mask.any():
        search = np.where(mask, index, -np.inf)
    else:
        search = index
    row, column = np.unravel_index(int(np.nanargmax(search)), index.shape)
    lat = meta["lat0"] + row * meta["dlat"]
    lon = meta["lon0"] + column * meta["dlon"]
    if lon > 180:
        lon -= 360
    return {"lat": round(float(lat), 3), "lon": round(float(lon), 3)}


def run(out_dir: str, cycle: dt.datetime, hours: tuple[int, ...]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    staging = tempfile.mkdtemp(prefix=".staging-", dir=out_dir)
    steps = []
    land_mask = None
    png_width = 0
    png_height = 0
    started = dt.datetime.now(dt.timezone.utc)
    try:
        for offset, forecast_hour in enumerate(hours):
            log(f"fetching f{forecast_hour:03d}")
            data = fetch_grib(cycle, forecast_hour)
            fields, meta = parse_grib(data)
            index, duct = compute_index(fields)
            if land_mask is None:
                land_mask = load_land_mask(meta)
                log(f"UK land mask: {int(land_mask.sum())} of {land_mask.size} grid cells")
            land_valid = land_mask & np.isfinite(index)
            if not land_valid.any():
                land_valid = np.isfinite(index)
            land_index = index[land_valid]
            land_duct = duct[land_valid]
            if land_index.size:
                max_index = float(np.nanmax(land_index))
                peak_duct = float(np.nanmax(land_duct))
                pct_strong = float((land_index >= 85).mean() * 100.0)
                pct_ducting = float((land_index >= 100).mean() * 100.0)
            else:
                max_index = peak_duct = pct_strong = pct_ducting = 0.0
            file_name = f"step-{offset:03d}.png"
            render_png(index, os.path.join(staging, file_name))
            png_width = int(index.shape[1] * SCALE)
            png_height = int(index.shape[0] * SCALE)
            steps.append({
                "index": offset,
                "forecast_hour": forecast_hour,
                "valid": (cycle + dt.timedelta(hours=forecast_hour)).isoformat().replace("+00:00", "Z"),
                "file": file_name,
                "max_index": round(max_index, 1),
                "peak_duct": round(peak_duct, 1),
                "pct_strong": round(pct_strong, 1),
                "pct_ducting": round(pct_ducting, 1),
                "peak": peak_location(index, meta, land_mask),
            })
            log(
                f"f{forecast_hour:03d} max_index={steps[-1]['max_index']:.0f} "
                f"peak_duct={steps[-1]['peak_duct']:.0f} N/km"
            )
        manifest = {
            "generated_at": started.isoformat().replace("+00:00", "Z"),
            "run": cycle.isoformat().replace("+00:00", "Z"),
            "cycle": f"{cycle:%H}Z",
            "model": "NOAA GFS 0.25°",
            "bounds": BBOX,
            "index_basis": "UK land cells (Natural Earth 1:50m United Kingdom boundary)",
            "land_cells": int(land_mask.sum()) if land_mask is not None else 0,
            "width": png_width,
            "height": png_height,
            "steps": steps,
            "source": "NOAA/NCEP GFS",
            "attribution": "Forecast data: NOAA/NCEP GFS (public domain). Boundary: Natural Earth (public domain). Basemap: OpenStreetMap contributors.",
        }
        # Move images into place first, then atomically replace the manifest.
        for step in steps:
            os.replace(os.path.join(staging, step["file"]), os.path.join(out_dir, step["file"]))
        manifest_path = os.path.join(out_dir, "index.json")
        temporary_manifest = manifest_path + ".tmp"
        with open(temporary_manifest, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, separators=(",", ":"))
        os.replace(temporary_manifest, manifest_path)
        log(f"wrote {len(steps)} steps to {out_dir}")
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "tropo-data"))
    parser.add_argument("--cycle", help="explicit cycle, e.g. 2026-11-27T12:00:00Z")
    parser.add_argument("--hours", default="0-72:3", help="forecast hours, e.g. 0-72:3")
    args = parser.parse_args()

    start_text, _, step_text = args.hours.partition(":")
    start, end = (int(part) for part in start_text.split("-", 1))
    step = int(step_text or "3")
    hours = tuple(range(start, end + 1, step))

    if args.cycle:
        cycle = dt.datetime.fromisoformat(args.cycle.replace("Z", "+00:00"))
    else:
        cycle = pick_cycle()
    run(os.path.abspath(args.out), cycle, hours)
    return 0


if __name__ == "__main__":
    sys.exit(main())
