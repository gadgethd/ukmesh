import json
import pathlib
import unittest

import numpy as np

from rf.loss import (
    PathLossResult,
    PrefixPathLossResult,
    compute_path_loss_from_profile,
    compute_prefix_path_losses,
)


class PathLossResultTests(unittest.TestCase):
    def test_named_result_preserves_profile_and_viability(self):
        distances = np.asarray([0.0, 500.0, 1000.0], dtype=np.float32)
        heights = np.asarray([100.0, 100.0, 100.0], dtype=np.float32)

        result = compute_path_loss_from_profile(
            distances,
            heights,
            110.0,
            110.0,
        )

        self.assertIsInstance(result, PathLossResult)
        self.assertGreater(result.path_loss_db, 0)
        self.assertIsInstance(result.viable, bool)
        self.assertEqual(
            result.profile,
            ((0.0, 100.0), (500.0, 100.0), (1000.0, 100.0)),
        )

    def test_profile_materialization_can_be_disabled_for_hot_radial_loops(self):
        result = compute_path_loss_from_profile(
            np.asarray([0.0, 100.0], dtype=np.float32),
            np.asarray([10.0, 10.0], dtype=np.float32),
            20.0,
            20.0,
            include_profile=False,
        )

        self.assertEqual(result.profile, ())

    def test_vectorized_prefixes_match_legacy_prefix_calculation(self):
        rng = np.random.default_rng(20260729)
        distances = np.arange(100.0, 20_100.0, 100.0, dtype=np.float64)
        heights = (
            110.0
            + np.sin(distances / 1700.0) * 35.0
            + rng.integers(-4, 5, size=len(distances))
        )
        receiver_heights = heights + 2.0

        result = compute_prefix_path_losses(
            distances,
            heights,
            125.0,
            receiver_heights,
            endpoint_batch_size=17,
        )
        expected = np.asarray([
            compute_path_loss_from_profile(
                distances[:index + 1],
                heights[:index + 1],
                125.0,
                receiver_heights[index],
                include_profile=False,
            ).path_loss_db
            for index in range(len(distances))
        ])

        self.assertIsInstance(result, PrefixPathLossResult)
        np.testing.assert_allclose(result.path_loss_db, expected, rtol=1e-10, atol=1e-10)

    def test_vectorized_prefix_memory_batch_does_not_change_results(self):
        distances = np.arange(100.0, 10_100.0, 100.0)
        heights = 80.0 + np.cos(distances / 1300.0) * 25.0
        receiver_heights = heights + 1.5

        tiny = compute_prefix_path_losses(
            distances,
            heights,
            95.0,
            receiver_heights,
            endpoint_batch_size=3,
        )
        wide = compute_prefix_path_losses(
            distances,
            heights,
            95.0,
            receiver_heights,
            endpoint_batch_size=128,
        )

        np.testing.assert_allclose(tiny.path_loss_db, wide.path_loss_db)
        np.testing.assert_array_equal(tiny.viable, wide.viable)

    def test_multi_ray_batches_match_independent_ray_results(self):
        distances = np.arange(100.0, 15_100.0, 100.0)
        heights = np.vstack([
            90.0 + np.sin(distances / 900.0) * 12.0,
            120.0 + np.cos(distances / 1300.0) * 31.0,
            65.0 + np.sin(distances / 500.0) * 7.0,
        ])
        receiver_heights = heights + 2.5

        batched = compute_prefix_path_losses(
            distances,
            heights,
            135.0,
            receiver_heights,
            endpoint_batch_size=19,
            ray_batch_size=2,
        )
        independent = [
            compute_prefix_path_losses(
                distances,
                heights[index],
                135.0,
                receiver_heights[index],
                endpoint_batch_size=19,
            )
            for index in range(len(heights))
        ]

        np.testing.assert_allclose(
            batched.path_loss_db,
            np.vstack([result.path_loss_db for result in independent]),
        )
        np.testing.assert_array_equal(
            batched.viable,
            np.vstack([result.viable for result in independent]),
        )
        np.testing.assert_allclose(
            batched.max_fresnel_v,
            np.vstack([result.max_fresnel_v for result in independent]),
        )

    def test_model_v7_golden_profile(self):
        fixture_path = (
            pathlib.Path(__file__).parent
            / 'fixtures'
            / 'rf_radial_golden.json'
        )
        fixture = json.loads(fixture_path.read_text(encoding='utf-8'))
        distances = np.asarray(fixture['distance_m'], dtype=np.float64)
        heights = np.asarray(fixture['terrain_height_m'], dtype=np.float64)
        expected = np.asarray([
            float('inf') if value is None else value
            for value in fixture['expected_path_loss_db']
        ])

        result = compute_prefix_path_losses(
            distances,
            heights,
            float(fixture['transmitter_height_m']),
            heights + float(fixture['receiver_clearance_m']),
        )

        self.assertEqual(7, fixture['model_version'])
        np.testing.assert_allclose(
            result.path_loss_db,
            expected,
            rtol=0,
            atol=float(fixture['absolute_tolerance_db']),
        )


if __name__ == '__main__':
    unittest.main()
