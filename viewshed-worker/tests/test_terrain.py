import gzip
import io
import logging
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from osgeo import gdal

from rf.terrain import (
    PermanentOutOfScope,
    RetryableTerrainError,
    build_link_vrt,
    download_tile,
    ensure_tiles_for_link,
    raster_window_for_radius,
)
from rf.loss import compute_path_loss


class FakeResponse:
    def __init__(self, payload: bytes = b'', status_code: int = 200):
        self.payload = payload
        self.status_code = status_code
        self.headers = {'content-length': str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.payload), chunk_size):
            yield self.payload[offset:offset + chunk_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class TerrainAcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.log = logging.getLogger('terrain-test')

    def test_empty_cache_downloads_and_atomically_promotes_valid_tile(self):
        compressed = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed, mode='wb') as archive:
            archive.write(bytes(2_884_802))

        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            'rf.terrain.requests.get',
            return_value=FakeResponse(compressed.getvalue()),
        ) as request:
            root = Path(tmp)
            tile = download_tile(root, 51, -1, self.log)

            self.assertEqual(tile, root / 'N51W001.hgt')
            self.assertEqual(tile.stat().st_size, 2_884_802)
            self.assertFalse((root / 'N51W001.hgt.gz.part').exists())
            self.assertFalse((root / 'N51W001.hgt.part').exists())
            self.assertFalse(request.call_args.kwargs['allow_redirects'])

    def test_empty_cache_can_complete_an_ordinary_same_tile_link(self):
        compressed = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed, mode='wb') as archive:
            archive.write(bytes(2_884_802))

        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            'rf.terrain.requests.get',
            return_value=FakeResponse(compressed.getvalue()),
        ):
            root = Path(tmp) / 'srtm'
            work = Path(tmp) / 'work'
            work.mkdir()
            paths = ensure_tiles_for_link(
                root,
                51.10,
                -0.90,
                51.20,
                -0.80,
                self.log,
            )
            vrt = build_link_vrt(51.10, -0.90, 51.20, -0.80, str(work), root)
            result = compute_path_loss(
                51.10,
                -0.90,
                0.0,
                51.20,
                -0.80,
                0.0,
                vrt,
            )

            self.assertEqual(len(paths), 1)
            self.assertTrue(Path(vrt).is_file())
            self.assertGreater(result.path_loss_db, 0)
            self.assertGreater(len(result.profile), 2)

    def test_corrupt_download_is_retryable_and_leaves_no_promoted_file(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            'rf.terrain.requests.get',
            return_value=FakeResponse(b'not-gzip'),
        ):
            root = Path(tmp)
            with self.assertRaises(RetryableTerrainError):
                download_tile(root, 51, -1, self.log)
            self.assertFalse((root / 'N51W001.hgt').exists())
            self.assertFalse((root / 'N51W001.hgt.gz.part').exists())
            self.assertFalse((root / 'N51W001.hgt.part').exists())

    def test_link_requires_both_endpoint_tiles(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch(
            'rf.terrain.download_tile',
            return_value=None,
        ):
            with self.assertRaises(PermanentOutOfScope):
                ensure_tiles_for_link(Path(tmp), 51.1, -1.2, 52.2, -1.3, self.log)

    def test_radius_window_avoids_reading_the_full_tile_rectangle(self):
        # Three one-degree SRTM1 tiles in each direction. A 20 km coverage
        # radius should need only a small central window, plus filter context.
        geotransform = (-3.0, 1.0 / 3600.0, 0.0, 54.0, 0.0, -1.0 / 3600.0)
        full_size = 10_801
        radius_m = 20_000.0
        x_offset, y_offset, x_size, y_size, shifted = raster_window_for_radius(
            geotransform,
            full_size,
            full_size,
            52.5,
            -1.5,
            radius_m,
            margin_pixels=5,
        )

        self.assertGreater(x_offset, 0)
        self.assertGreater(y_offset, 0)
        self.assertLess(x_size, full_size // 3)
        self.assertLess(y_size, full_size // 3)
        self.assertAlmostEqual(
            shifted[0],
            geotransform[0] + x_offset * geotransform[1],
        )
        self.assertAlmostEqual(
            shifted[3],
            geotransform[3] + y_offset * geotransform[5],
        )

        inv = gdal.InvGeoTransform(shifted)
        d_lat = radius_m / 111_320.0
        d_lon = radius_m / (
            111_320.0 * math.cos(math.radians(52.5))
        )
        for sample_lon, sample_lat in (
            (-1.5 - d_lon, 52.5 - d_lat),
            (-1.5 + d_lon, 52.5 + d_lat),
        ):
            px, py = gdal.ApplyGeoTransform(
                inv,
                sample_lon,
                sample_lat,
            )
            self.assertGreaterEqual(px, 4)
            self.assertGreaterEqual(py, 4)
            self.assertLess(px, x_size - 4)
            self.assertLess(py, y_size - 4)


if __name__ == '__main__':
    unittest.main()
