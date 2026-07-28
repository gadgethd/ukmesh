import gzip
import fcntl
import os
import math
import subprocess
from pathlib import Path
from typing import Optional

import requests
from osgeo import gdal

from rf.config import K_FACTOR, R_EARTH_M

SRTM_CONNECT_TIMEOUT_S = max(2.0, float(os.environ.get('SRTM_CONNECT_TIMEOUT_S', '8')))
SRTM_READ_TIMEOUT_S = max(5.0, float(os.environ.get('SRTM_READ_TIMEOUT_S', '30')))
SRTM_MAX_COMPRESSED_BYTES = max(1_000_000, int(os.environ.get('SRTM_MAX_COMPRESSED_BYTES', '10000000')))


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

    srtm_dir.mkdir(parents=True, exist_ok=True)
    lock_path = srtm_dir / f'.{name}.lock'
    with lock_path.open('a+b') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if path.exists():
            return path
        url = f'https://s3.amazonaws.com/elevation-tiles-prod/skadi/{name[:3]}/{name}.hgt.gz'
        log.info(f'Downloading {name} ...')
        tmp_gz = path.with_suffix('.hgt.gz.part')
        tmp = path.with_suffix('.hgt.part')
        try:
            with requests.get(url, timeout=(SRTM_CONNECT_TIMEOUT_S, SRTM_READ_TIMEOUT_S), stream=True) as resp:
                if resp.status_code == 404:
                    log.debug(f'{name} not found (ocean / outside coverage)')
                    return None
                resp.raise_for_status()
                length = int(resp.headers.get('content-length', '0') or 0)
                if length > SRTM_MAX_COMPRESSED_BYTES:
                    raise ValueError(f'{name} response exceeds compressed size limit')
                downloaded = 0
                with tmp_gz.open('wb') as output:
                    for chunk in resp.iter_content(64 * 1024):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if downloaded > SRTM_MAX_COMPRESSED_BYTES:
                            raise ValueError(f'{name} download exceeded compressed size limit')
                        output.write(chunk)
            with gzip.open(tmp_gz, 'rb') as source, tmp.open('wb') as output:
                while chunk := source.read(128 * 1024):
                    output.write(chunk)
            if tmp.stat().st_size not in (2_884_802, 25_934_402):
                raise ValueError(f'{name} has unexpected HGT size {tmp.stat().st_size}')
            tmp.replace(path)
            log.info(f'Saved {name}.hgt ({path.stat().st_size // 1024} KB)')
            return path
        except (requests.Timeout, requests.ConnectionError) as exc:
            log.warning(f'Timed out downloading {name}: {exc}')
            return None
        except (requests.RequestException, OSError, ValueError, gzip.BadGzipFile) as exc:
            log.error(f'Failed to download {name}: {exc}')
            return None
        finally:
            tmp_gz.unlink(missing_ok=True)
            tmp.unlink(missing_ok=True)


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
    if not all(math.isfinite(value) for value in (lat, lon)) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return 0.0
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
    band = ds.GetRasterBand(1)
    data = band.ReadAsArray(px, py, 1, 1)
    nodata = band.GetNoDataValue()
    ds = None
    if data is None:
        return 0.0
    value = float(data[0][0])
    if not math.isfinite(value) or (nodata is not None and value == nodata) or value < -500 or value > 9_000:
        return 0.0
    return max(0.0, value)


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
    try:
        result = subprocess.run(['gdalbuildvrt', vrt] + paths, capture_output=True, text=True, timeout=30, check=False)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return vrt if result.returncode == 0 else None
