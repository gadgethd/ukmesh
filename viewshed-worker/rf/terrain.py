import gzip
import fcntl
import os
import math
import subprocess
from pathlib import Path
from typing import Iterable, Optional

import requests
from osgeo import gdal

from rf.config import K_FACTOR, R_EARTH_M
from worker_metrics import record_srtm

SRTM_CONNECT_TIMEOUT_S = max(2.0, float(os.environ.get('SRTM_CONNECT_TIMEOUT_S', '8')))
SRTM_READ_TIMEOUT_S = max(5.0, float(os.environ.get('SRTM_READ_TIMEOUT_S', '30')))
SRTM_MAX_COMPRESSED_BYTES = max(1_000_000, int(os.environ.get('SRTM_MAX_COMPRESSED_BYTES', '10000000')))
SRTM_MAX_DECOMPRESSED_BYTES = max(2_884_802, int(os.environ.get('SRTM_MAX_DECOMPRESSED_BYTES', '25934402')))
SRTM_MAX_LINK_TILES = max(1, int(os.environ.get('SRTM_MAX_LINK_TILES', '64')))
SRTM_MAX_JOB_TILES = max(SRTM_MAX_LINK_TILES, int(os.environ.get('SRTM_MAX_JOB_TILES', '256')))
SRTM_CACHE_MAX_BYTES = max(
    SRTM_MAX_DECOMPRESSED_BYTES * 4,
    int(os.environ.get('SRTM_CACHE_MAX_BYTES', str(20 * 1024 * 1024 * 1024))),
)
VALID_HGT_SIZES = frozenset((2_884_802, 25_934_402))


class TerrainError(RuntimeError):
    """Base class for explicit terrain acquisition/calculation outcomes."""


class RetryableTerrainError(TerrainError):
    """A transient download, filesystem, subprocess, or GDAL failure."""


class PermanentOutOfScope(TerrainError):
    """A valid request outside available SRTM terrain coverage."""


class InvalidTerrainRequest(TerrainError):
    """A malformed or unbounded terrain request."""


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


def _validate_tile_coordinate(lat: int, lon: int) -> None:
    if not isinstance(lat, int) or not isinstance(lon, int):
        raise InvalidTerrainRequest('SRTM tile coordinates must be integers')
    if not (-90 <= lat <= 89 and -180 <= lon <= 179):
        raise PermanentOutOfScope(f'SRTM tile {lat},{lon} is outside supported coordinates')


def _is_valid_cached_tile(path: Path) -> bool:
    try:
        return path.is_file() and not path.is_symlink() and path.stat().st_size in VALID_HGT_SIZES
    except OSError:
        return False


def _prune_cache(srtm_dir: Path, protected: set[Path], log) -> None:
    """Bound the shared tile cache without deleting files used by this job."""
    try:
        candidates = [
            path for path in srtm_dir.glob('*.hgt')
            if path not in protected and path.is_file() and not path.is_symlink()
        ]
        total = sum(path.stat().st_size for path in candidates)
        total += sum(path.stat().st_size for path in protected if path.exists())
        if total <= SRTM_CACHE_MAX_BYTES:
            return
        for path in sorted(candidates, key=lambda item: item.stat().st_mtime):
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            total -= size
            if total <= SRTM_CACHE_MAX_BYTES:
                break
        if total > SRTM_CACHE_MAX_BYTES:
            log.warning('SRTM cache remains above its byte budget because active tiles are protected')
    except OSError as exc:
        log.warning(f'Unable to prune SRTM cache: {exc}')


def download_tile(srtm_dir: Path, lat: int, lon: int, log) -> Optional[Path]:
    _validate_tile_coordinate(lat, lon)
    name = tile_name(lat, lon)
    path = srtm_dir / f'{name}.hgt'
    if _is_valid_cached_tile(path):
        record_srtm('cache_hit')
        # Cache eviction uses mtime as an inexpensive cross-process LRU signal.
        try:
            os.utime(path, None)
        except OSError:
            pass
        return path
    if path.exists():
        log.warning(f'Removing invalid cached terrain tile {name}')
        try:
            path.unlink()
        except OSError as exc:
            raise RetryableTerrainError(f'cannot remove invalid cached tile {name}: {exc}') from exc

    srtm_dir.mkdir(parents=True, exist_ok=True)
    lock_path = srtm_dir / f'.{name}.lock'
    with lock_path.open('a+b') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if _is_valid_cached_tile(path):
            record_srtm('cache_hit')
            return path
        url = f'https://s3.amazonaws.com/elevation-tiles-prod/skadi/{name[:3]}/{name}.hgt.gz'
        log.info(f'Downloading {name} ...')
        tmp_gz = path.with_suffix('.hgt.gz.part')
        tmp = path.with_suffix('.hgt.part')
        try:
            with requests.get(
                url,
                timeout=(SRTM_CONNECT_TIMEOUT_S, SRTM_READ_TIMEOUT_S),
                stream=True,
                allow_redirects=False,
            ) as resp:
                if resp.status_code == 404:
                    record_srtm('not_found')
                    log.debug(f'{name} not found (ocean / outside coverage)')
                    return None
                if 300 <= resp.status_code < 400:
                    raise RetryableTerrainError(f'{name} returned an unexpected redirect')
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
            decompressed = 0
            with gzip.open(tmp_gz, 'rb') as source, tmp.open('wb') as output:
                while chunk := source.read(128 * 1024):
                    decompressed += len(chunk)
                    if decompressed > SRTM_MAX_DECOMPRESSED_BYTES:
                        raise ValueError(f'{name} decompressed data exceeded size limit')
                    output.write(chunk)
            if tmp.stat().st_size not in VALID_HGT_SIZES:
                raise ValueError(f'{name} has unexpected HGT size {tmp.stat().st_size}')
            tmp.replace(path)
            os.utime(path, None)
            log.info(f'Saved {name}.hgt ({path.stat().st_size // 1024} KB)')
            record_srtm('success')
            return path
        except RetryableTerrainError:
            record_srtm('retry')
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            record_srtm('retry')
            raise RetryableTerrainError(f'timed out downloading {name}: {exc}') from exc
        except (requests.RequestException, OSError, ValueError, gzip.BadGzipFile) as exc:
            record_srtm('failure')
            raise RetryableTerrainError(f'failed to acquire {name}: {exc}') from exc
        finally:
            tmp_gz.unlink(missing_ok=True)
            tmp.unlink(missing_ok=True)


def ensure_tiles(
    srtm_dir: Path,
    tiles: Iterable[tuple[int, int]],
    log,
    *,
    required_tiles: Iterable[tuple[int, int]] = (),
    max_tiles: int = SRTM_MAX_JOB_TILES,
) -> list[Path]:
    ordered = list(dict.fromkeys(tiles))
    required = set(required_tiles)
    if not ordered:
        raise InvalidTerrainRequest('terrain request contains no tiles')
    if len(ordered) > max_tiles:
        raise InvalidTerrainRequest(
            f'terrain request requires {len(ordered)} tiles (limit {max_tiles})'
        )

    paths: list[Path] = []
    missing: list[tuple[int, int]] = []
    for tile in ordered:
        path = download_tile(srtm_dir, *tile, log)
        if path is None:
            if tile in required:
                missing.append(tile)
            continue
        paths.append(path)

    if missing:
        names = ', '.join(tile_name(*tile) for tile in missing)
        raise PermanentOutOfScope(f'required terrain is unavailable: {names}')
    if not paths:
        raise PermanentOutOfScope('no terrain tiles are available for this request')
    _prune_cache(srtm_dir, set(paths), log)
    return paths


def ensure_tiles_for_link(
    srtm_dir: Path,
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    log,
) -> list[Path]:
    coords = (lat1, lon1, lat2, lon2)
    if not all(math.isfinite(value) for value in coords):
        raise InvalidTerrainRequest('link coordinates must be finite')
    if not (-90 <= lat1 <= 90 and -90 <= lat2 <= 90 and -180 <= lon1 <= 180 and -180 <= lon2 <= 180):
        raise InvalidTerrainRequest('link coordinates are outside geographic bounds')
    min_lat = math.floor(min(lat1, lat2))
    max_lat = math.floor(max(lat1, lat2))
    min_lon = math.floor(min(lon1, lon2))
    max_lon = math.floor(max(lon1, lon2))
    tiles = [
        (lat, lon)
        for lat in range(min_lat, max_lat + 1)
        for lon in range(min_lon, max_lon + 1)
    ]
    endpoints = {
        (math.floor(lat1), math.floor(lon1)),
        (math.floor(lat2), math.floor(lon2)),
    }
    return ensure_tiles(
        srtm_dir,
        tiles,
        log,
        required_tiles=endpoints,
        max_tiles=SRTM_MAX_LINK_TILES,
    )


def ensure_tiles_for_radius(
    srtm_dir: Path,
    lat: float,
    lon: float,
    radius_m: float,
    log,
) -> list[Path]:
    if not all(math.isfinite(value) for value in (lat, lon, radius_m)) or radius_m <= 0:
        raise InvalidTerrainRequest('coverage coordinates and radius must be finite and positive')
    center = (math.floor(lat), math.floor(lon))
    return ensure_tiles(
        srtm_dir,
        tiles_for_radius(lat, lon, radius_m),
        log,
        required_tiles=(center,),
        max_tiles=SRTM_MAX_JOB_TILES,
    )


def tiles_for_radius(lat: float, lon: float, radius_m: float) -> list[tuple[int, int]]:
    d_lat = radius_m / 111_320
    d_lon = radius_m / (111_320 * math.cos(math.radians(lat)))
    return [
        (lt, ln)
        for lt in range(math.floor(lat - d_lat), math.floor(lat + d_lat) + 1)
        for ln in range(math.floor(lon - d_lon), math.floor(lon + d_lon) + 1)
    ]


def raster_window_for_radius(
    geotransform: tuple[float, float, float, float, float, float],
    raster_x_size: int,
    raster_y_size: int,
    lat: float,
    lon: float,
    radius_m: float,
    *,
    margin_pixels: int = 0,
) -> tuple[int, int, int, int, tuple[float, float, float, float, float, float]]:
    """Return a bounded GDAL read window and its shifted geotransform.

    Tile acquisition deliberately fetches whole one-degree SRTM files, but a
    coverage job samples only the radius around its observer. Reading the full
    tile rectangle can waste hundreds of MiB near tile boundaries. The margin
    preserves neighbouring pixels needed by spatial filters at the sampled
    radius edge.
    """
    if (
        len(geotransform) != 6
        or raster_x_size < 1
        or raster_y_size < 1
        or not all(math.isfinite(value) for value in (*geotransform, lat, lon, radius_m))
        or radius_m <= 0
    ):
        raise InvalidTerrainRequest('coverage raster window inputs are invalid')
    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 1e-6:
        raise InvalidTerrainRequest('coverage raster window is too close to a pole')
    inv = gdal.InvGeoTransform(geotransform)
    if inv is None:
        raise RetryableTerrainError('coverage VRT has no invertible geotransform')

    d_lat = radius_m / 111_320.0
    d_lon = radius_m / (111_320.0 * cos_lat)
    corners = (
        (lon - d_lon, lat - d_lat),
        (lon - d_lon, lat + d_lat),
        (lon + d_lon, lat - d_lat),
        (lon + d_lon, lat + d_lat),
    )
    pixel_corners = [gdal.ApplyGeoTransform(inv, x, y) for x, y in corners]
    margin = max(0, min(256, int(margin_pixels)))
    x_offset = max(0, math.floor(min(point[0] for point in pixel_corners)) - margin)
    y_offset = max(0, math.floor(min(point[1] for point in pixel_corners)) - margin)
    x_stop = min(
        raster_x_size,
        math.ceil(max(point[0] for point in pixel_corners)) + margin + 1,
    )
    y_stop = min(
        raster_y_size,
        math.ceil(max(point[1] for point in pixel_corners)) + margin + 1,
    )
    if x_stop <= x_offset or y_stop <= y_offset:
        raise PermanentOutOfScope('coverage radius does not intersect its terrain VRT')

    shifted = (
        geotransform[0] + x_offset * geotransform[1] + y_offset * geotransform[2],
        geotransform[1],
        geotransform[2],
        geotransform[3] + x_offset * geotransform[4] + y_offset * geotransform[5],
        geotransform[4],
        geotransform[5],
    )
    return (
        x_offset,
        y_offset,
        x_stop - x_offset,
        y_stop - y_offset,
        shifted,
    )


def radio_horizon_m(height_asl_m: float) -> float:
    h = max(1.0, height_asl_m)
    return math.sqrt(2 * K_FACTOR * R_EARTH_M * h)


def sample_elevation(vrt_path: str, lat: float, lon: float) -> float:
    if not all(math.isfinite(value) for value in (lat, lon)) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise InvalidTerrainRequest('elevation coordinates are invalid')
    ds = gdal.Open(vrt_path)
    if ds is None:
        raise RetryableTerrainError('GDAL could not open the terrain VRT')
    gt = ds.GetGeoTransform()
    inv = gdal.InvGeoTransform(gt)
    if inv is None:
        ds = None
        raise RetryableTerrainError('terrain VRT has no invertible geotransform')
    px, py = gdal.ApplyGeoTransform(inv, lon, lat)
    px = max(0, min(int(px), ds.RasterXSize - 1))
    py = max(0, min(int(py), ds.RasterYSize - 1))
    band = ds.GetRasterBand(1)
    if band is None:
        ds = None
        raise RetryableTerrainError('terrain VRT has no elevation band')
    data = band.ReadAsArray(px, py, 1, 1)
    nodata = band.GetNoDataValue()
    ds = None
    if data is None:
        raise RetryableTerrainError('GDAL could not sample the terrain VRT')
    value = float(data[0][0])
    if not math.isfinite(value) or value < -500 or value > 9_000:
        raise RetryableTerrainError('terrain VRT returned an invalid elevation')
    if nodata is not None and value == nodata:
        return 0.0
    return max(0.0, value)


def build_vrt(paths: Iterable[Path], output_path: str, *, timeout_s: float = 30.0) -> str:
    source_paths = [str(path) for path in paths]
    if not source_paths:
        raise PermanentOutOfScope('cannot build a terrain VRT without source tiles')
    try:
        result = subprocess.run(
            ['gdalbuildvrt', output_path, *source_paths],
            capture_output=True,
            text=True,
            timeout=max(1.0, timeout_s),
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        raise RetryableTerrainError(f'gdalbuildvrt failed: {exc}') from exc
    if result.returncode != 0:
        raise RetryableTerrainError(f'gdalbuildvrt failed: {result.stderr[:500]}')
    if not Path(output_path).is_file():
        raise RetryableTerrainError('gdalbuildvrt returned success without an output file')
    return output_path


def build_link_vrt(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    tmp_dir: str,
    srtm_dir: Path,
) -> str:
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
        raise PermanentOutOfScope('no cached terrain tiles cover this link')
    vrt = f'{tmp_dir}/link.vrt'
    return build_vrt((Path(path) for path in paths), vrt)
