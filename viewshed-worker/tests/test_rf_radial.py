import time
import unittest
from unittest import mock

import numpy as np
from shapely.geometry import Point, shape

import worker
from rf.terrain import tiles_for_radius


class RfRadialBudgetTests(unittest.TestCase):
    def test_in_place_dtm_filter_matches_out_of_place_result(self):
        rng = np.random.default_rng(20260801)
        elevation = rng.integers(0, 500, size=(32, 31)).astype(np.float32)
        expected = worker._min_filter(elevation, size=9)
        actual = elevation.copy()

        returned = worker.approximate_dtm_in_place(actual)

        self.assertIs(returned, actual)
        np.testing.assert_array_equal(actual, expected)

    def test_native_int16_dtm_filter_matches_float32_result(self):
        rng = np.random.default_rng(20260802)
        native = rng.integers(0, 500, size=(32, 31), dtype=np.int16)
        expected = worker._min_filter(native.astype(np.float32), size=9)

        returned = worker.approximate_dtm_in_place(native)

        self.assertIs(returned, native)
        self.assertEqual(np.int16, native.dtype)
        np.testing.assert_array_equal(native, expected)

    def test_empty_land_mask_intersection_is_terminal(self):
        with self.assertRaisesRegex(
            worker.PermanentOutOfScope,
            'does not intersect the map land mask',
        ):
            worker.require_mapped_coverage('node-a', None, 'RF')

    def test_rf_search_radius_matches_terrain_fetch_and_ray_cap(self):
        with (
            mock.patch.object(worker, 'RF_MIN_RADIUS_M', 20_000),
            mock.patch.object(worker, 'RF_RADIUS_MULTIPLIER', 1.35),
            mock.patch.object(worker, 'MAX_RADIUS_M', 100_000),
        ):
            self.assertEqual(worker.rf_search_radius_m(5_000), 20_000)
            self.assertEqual(worker.rf_search_radius_m(40_000), 54_000)
            self.assertEqual(worker.rf_search_radius_m(90_000), 100_000)

    def test_rf_expansion_fetches_a_tile_needed_beyond_geometric_horizon(self):
        with (
            mock.patch.object(worker, 'RF_MIN_RADIUS_M', 20_000),
            mock.patch.object(worker, 'RF_RADIUS_MULTIPLIER', 1.35),
            mock.patch.object(worker, 'MAX_RADIUS_M', 100_000),
        ):
            base_radius_m = 16_000
            expanded_radius_m = worker.rf_search_radius_m(base_radius_m)

        base_tiles = set(tiles_for_radius(51.81, -0.5, base_radius_m))
        expanded_tiles = set(tiles_for_radius(51.81, -0.5, expanded_radius_m))
        self.assertNotIn((52, -1), base_tiles)
        self.assertIn((52, -1), expanded_tiles)

    def test_strength_bands_are_exclusive_and_reconstruct_outer_coverage(self):
        nested = {
            'green': Point(0, 0).buffer(1),
            'amber': Point(0, 0).buffer(2),
            'red': Point(0, 0).buffer(3),
        }

        with (
            mock.patch.object(worker, 'UK_MAINLAND', None),
            mock.patch.object(worker, 'SIMPLIFY_DEG', 0),
        ):
            result = worker.build_exclusive_strength_geoms(nested)

        green = shape(result['green'])
        amber = shape(result['amber'])
        red = shape(result['red'])
        self.assertAlmostEqual(green.intersection(amber).area, 0.0, places=9)
        self.assertAlmostEqual(green.intersection(red).area, 0.0, places=9)
        self.assertAlmostEqual(amber.intersection(red).area, 0.0, places=9)
        self.assertLessEqual(
            green.union(amber).union(red).symmetric_difference(
                nested['red']
            ).area / nested['red'].area,
            1e-8,
        )

    def test_simplified_nearby_boundaries_remain_an_exact_partition(self):
        nested = {
            'green': Point(0, 0).buffer(1.00, resolution=64),
            'amber': Point(0, 0).buffer(1.02, resolution=64),
            'red': Point(0, 0).buffer(1.04, resolution=64),
        }

        with (
            mock.patch.object(worker, 'UK_MAINLAND', None),
            mock.patch.object(worker, 'SIMPLIFY_DEG', 0.1),
        ):
            outer = worker.clip_and_simplify_polygon(nested['red'])
            result = worker.build_exclusive_strength_geoms(
                nested,
                red_outer_geom=outer,
            )

        bands = [shape(result[name]) for name in ('green', 'amber', 'red')]
        self.assertTrue(all(band.is_valid for band in bands))
        self.assertAlmostEqual(bands[0].intersection(bands[1]).area, 0.0, places=12)
        self.assertAlmostEqual(bands[0].intersection(bands[2]).area, 0.0, places=12)
        self.assertAlmostEqual(bands[1].intersection(bands[2]).area, 0.0, places=12)
        self.assertLessEqual(
            bands[0].union(bands[1]).union(bands[2]).symmetric_difference(
                shape(outer)
            ).area / shape(outer).area,
            1e-8,
        )

    def test_deadline_is_checked_before_radial_allocation(self):
        with self.assertRaises(worker.JobDeadlineExceeded):
            worker.resolve_rf_radial_boundaries(
                'node-a',
                51.5,
                -0.5,
                np.full((20, 20), 100.0, dtype=np.float32),
                (-1.0, 0.05, 0.0, 52.0, 0.0, -0.05),
                110.0,
                1000.0,
                deadline_monotonic=time.monotonic() - 1,
            )

    def test_lease_cancellation_is_checked_inside_radial_work(self):
        checks = 0

        def cancel_after_start():
            nonlocal checks
            checks += 1
            if checks >= 3:
                raise RuntimeError('lease lost')

        with (
            mock.patch.object(worker, 'RF_N_RAYS', 8),
            mock.patch.object(worker, 'RF_MIN_RADIUS_M', 1000),
            mock.patch.object(worker, 'MAX_RADIUS_M', 2000),
            mock.patch.object(worker, 'RF_RADIAL_STEP_M', 100.0),
            mock.patch.object(
                worker,
                'support_penalty_db',
                side_effect=lambda _node_id, lats, _lons: np.zeros(
                    len(lats),
                    dtype=np.float32,
                ),
            ),
            self.assertRaisesRegex(worker.Cancelled, 'lease lost'),
        ):
            worker.resolve_rf_radial_boundaries(
                'node-a',
                51.5,
                -0.5,
                np.full((100, 100), 100.0, dtype=np.float32),
                (-1.0, 0.01, 0.0, 52.0, 0.0, -0.01),
                110.0,
                1000.0,
                deadline_monotonic=time.monotonic() + 30,
                cancellation_check=cancel_after_start,
            )

        self.assertGreaterEqual(checks, 3)

    def test_flat_terrain_produces_closed_boundary_for_each_band(self):
        with (
            mock.patch.object(worker, 'RF_N_RAYS', 12),
            mock.patch.object(worker, 'RF_MIN_RADIUS_M', 1000),
            mock.patch.object(worker, 'MAX_RADIUS_M', 2000),
            mock.patch.object(worker, 'RF_RADIAL_STEP_M', 100.0),
            mock.patch.object(
                worker,
                'support_penalty_db',
                side_effect=lambda _node_id, lats, _lons: np.zeros(
                    len(lats),
                    dtype=np.float32,
                ),
            ),
        ):
            boundaries, radius = worker.resolve_rf_radial_boundaries(
                'node-a',
                51.5,
                -0.5,
                np.full((100, 100), 100.0, dtype=np.float32),
                (-1.0, 0.01, 0.0, 52.0, 0.0, -0.01),
                110.0,
                1000.0,
                deadline_monotonic=time.monotonic() + 30,
            )

        self.assertGreater(radius, 0)
        self.assertTrue(boundaries)
        for boundary in boundaries.values():
            self.assertEqual(13, len(boundary))
            self.assertEqual(boundary[0], boundary[-1])

    def test_window_offsets_preserve_full_raster_pixel_sampling(self):
        rng = np.random.default_rng(20260802)
        full = rng.integers(0, 500, size=(200, 220)).astype(np.float32)
        x_offset = 40
        y_offset = 50
        window = full[y_offset:150, x_offset:180]
        geotransform = (-2.0, 0.01, 0.0, 53.0, 0.0, -0.01)

        with (
            mock.patch.object(worker, 'RF_N_RAYS', 12),
            mock.patch.object(worker, 'RF_MIN_RADIUS_M', 1000),
            mock.patch.object(worker, 'MAX_RADIUS_M', 2000),
            mock.patch.object(worker, 'RF_RADIAL_STEP_M', 100.0),
            mock.patch.object(
                worker,
                'support_penalty_db',
                side_effect=lambda _node_id, lats, _lons: np.zeros(
                    len(lats),
                    dtype=np.float32,
                ),
            ),
        ):
            full_result = worker.resolve_rf_radial_boundaries(
                'node-a',
                52.0,
                -1.0,
                full,
                geotransform,
                110.0,
                1000.0,
            )
            window_result = worker.resolve_rf_radial_boundaries(
                'node-a',
                52.0,
                -1.0,
                window,
                geotransform,
                110.0,
                1000.0,
                raster_x_offset=x_offset,
                raster_y_offset=y_offset,
            )

        self.assertEqual(full_result, window_result)


if __name__ == '__main__':
    unittest.main()
