import gzip
import math
import subprocess
from pathlib import Path
from typing import Optional

import requests
from osgeo import gdal

from rf.config import K_FACTOR, R_EARTH_M


def load_uk_mainland(base_path: Path, log) -> Optional[object]:
    path = base_path / 'uk_mainland.json'
    if not path.exists():
        log.warning('uk_mainland.json not found — ocean clipping disabled')
        return None
    with open(path) as f:
        import json
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


def tile_name(lat: int, lon: int) -> str:
    ns = 'N' if lat >= 0 else 'S'
    ew = 'E' if lon >= 0 else 'W'
    return f'{ns}{abs(lat):02d}{ew}{abs(lon):03d}'


def download_tile(srtm_dir: Path, lat: int, lon: int, log) -> Optional[Path]:
    name = tile_name(lat, lon)
    path = srtm_dir / f'{name}.hgt'
    if path.exists():
        return path

    url = f'https://s3.amazonaws.com/elevation-tiles-prod/skadi/{name[:3]}/{name}.hgt.gz'
    log.info(f'Downloading {name} ...')
    try:
        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code == 404:
            log.debug(f'{name} not found (ocean / outside coverage)')
            return None
        resp.raise_for_status()
        data = gzip.decompress(resp.content)
        srtm_dir.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix('.tmp')
        tmp.write_bytes(data)
        tmp.rename(path)
        log.info(f'Saved {name}.hgt ({len(data) // 1024} KB)')
        return path
    except Exception as exc:
        log.error(f'Failed to download {name}: {exc}')
        return None


def tiles_for_radius(lat: float, lon: float, radius_m: float) -> list[tuple[int, int]]:
    d_lat = radius_m / 111_320
    d_lon = radius_m / (111_320 * math.cos(math.radians(lat)))
    return [
        (lt, ln)
        for lt in range(math.floor(lat - d_lat), math.floor(lat + d_lat) + 1)
        for ln in range(math.floor(lon - d_lon), math.floor(lon + d_lon) + 1)
    ]


def radio_horizon_m(height_asl_m: float) -> float:
    h = max(1.0, height_asl_m)
    return math.sqrt(2 * K_FACTOR * R_EARTH_M * h)


def sample_elevation(vrt_path: str, lat: float, lon: float) -> float:
    ds = gdal.Open(vrt_path)
    if ds is None:
        return 0.0
    gt = ds.GetGeoTransform()
    inv = gdal.InvGeoTransform(gt)
    if inv is None:
        ds = None
        return 0.0
    px, py = gdal.ApplyGeoTransform(inv, lon, lat)
    px = max(0, min(int(px), ds.RasterXSize - 1))
    py = max(0, min(int(py), ds.RasterYSize - 1))
    data = ds.GetRasterBand(1).ReadAsArray(px, py, 1, 1)
    ds = None
    return max(0.0, float(data[0][0])) if data is not None else 0.0


def build_link_vrt(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    tmp_dir: str,
    srtm_dir: Path,
) -> Optional[str]:
    min_lat = math.floor(min(lat1, lat2))
    max_lat = math.floor(max(lat1, lat2))
    min_lon = math.floor(min(lon1, lon2))
    max_lon = math.floor(max(lon1, lon2))
    paths = [
        str(srtm_dir / f'{tile_name(lt, ln)}.hgt')
        for lt in range(min_lat, max_lat + 1)
        for ln in range(min_lon, max_lon + 1)
        if (srtm_dir / f'{tile_name(lt, ln)}.hgt').exists()
    ]
    if not paths:
        return None
    vrt = f'{tmp_dir}/link.vrt'
    result = subprocess.run(['gdalbuildvrt', vrt] + paths, capture_output=True, text=True)
    return vrt if result.returncode == 0 else None
