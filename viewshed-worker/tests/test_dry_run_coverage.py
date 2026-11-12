from worker_context import WorkerContext
import io
import unittest

from shapely.geometry import box, mapping
from shapely.ops import unary_union

import dry_run_coverage
import coverage_model as worker


class DryRunCoverageTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def test_input_parser_and_support_context_match_worker_shapes(self):
        nodes, links = dry_run_coverage.parse_input(
            io.StringIO(
                "N\tnode-a\t51.5\t-1.0\n"
                "N\tnode-b\t51.6\t-1.1\n"
                "L\tnode-a\tnode-b\t51.5\t-1.0\t51.6\t-1.1\n"
            ),
            context=self.context,
        )

        dry_run_coverage.install_support_context(nodes, links, context=self.context)

        self.assertEqual(2, len(nodes))
        self.assertEqual(1, len(links))
        self.assertEqual(["node-a", "node-b"], list(self.context.rf.support.node_ids))
        self.assertGreater(self.context.rf.support.max_link_km_by_node["node-a"], 0)

    def test_result_validator_accepts_exclusive_reconstructing_bands(self):
        green = box(0, 0, 1, 1)
        amber = box(1, 0, 2, 1)
        red = box(2, 0, 3, 1)
        outer = unary_union((green, amber, red))
        result = worker.Computed(
            geom=mapping(outer),
            strength_geoms={
                "green": mapping(green),
                "amber": mapping(amber),
                "red": mapping(red),
            },
            radius_m=3_000,
            elevation_m=100,
        )

        self.assertEqual(
            [], dry_run_coverage.validate_result(result, context=self.context)
        )

    def test_result_validator_accepts_omitted_zero_area_exclusive_band(self):
        green = box(0, 0, 1, 1)
        amber = box(1, 0, 2, 1)
        outer = unary_union((green, amber))
        result = worker.Computed(
            geom=mapping(outer),
            strength_geoms={
                "green": mapping(green),
                "amber": mapping(amber),
            },
            radius_m=1_000,
            elevation_m=100,
        )

        self.assertEqual(
            [], dry_run_coverage.validate_result(result, context=self.context)
        )

    def test_result_validator_rejects_incomplete_unexpected_and_overlapping_bands(self):
        square = box(0, 0, 1, 1)
        incomplete = worker.Computed(
            geom=mapping(box(0, 0, 2, 1)),
            strength_geoms={"green": mapping(square)},
            radius_m=1_000,
            elevation_m=100,
        )
        unexpected = worker.Computed(
            geom=mapping(square),
            strength_geoms={"blue": mapping(square)},
            radius_m=1_000,
            elevation_m=100,
        )
        overlapping = worker.Computed(
            geom=mapping(square),
            strength_geoms={
                name: mapping(square) for name in dry_run_coverage.EXPECTED_BANDS
            },
            radius_m=1_000,
            elevation_m=100,
        )

        self.assertIn(
            "band_reconstruction",
            dry_run_coverage.validate_result(incomplete, context=self.context),
        )
        self.assertIn(
            "bands", dry_run_coverage.validate_result(unexpected, context=self.context)
        )
        self.assertIn(
            "band_overlap",
            dry_run_coverage.validate_result(overlapping, context=self.context),
        )

    def test_job_selection_is_bounded_and_spans_the_input(self):
        nodes = [
            dry_run_coverage.NodeInput(str(index), 51.5, -1.0) for index in range(20)
        ]

        selected = dry_run_coverage.select_jobs(nodes, 4)

        self.assertEqual(4, len(selected))
        self.assertIs(selected[0], nodes[0])
        self.assertIs(selected[-1], nodes[-1])

    def test_completion_gate_only_accepts_declared_terminal_outcomes(self):
        report = {
            "input_nodes": 3,
            "outcomes": {"computed": 2, "permanent": 1},
            "validation_issues": {},
        }

        self.assertFalse(dry_run_coverage.report_is_complete(report))
        self.assertTrue(
            dry_run_coverage.report_is_complete(report, accept_permanent=True)
        )
        report["outcomes"]["error:RuntimeError"] = 1
        report["input_nodes"] = 4
        self.assertFalse(
            dry_run_coverage.report_is_complete(report, accept_permanent=True)
        )


if __name__ == "__main__":
    unittest.main()
