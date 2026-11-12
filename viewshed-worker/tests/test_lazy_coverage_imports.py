from rf_snapshots import RfRefreshState
from worker_context import WorkerContext
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

import numpy as np
import coverage_model as worker
import rf_context


class LazyCoverageImportsTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def test_fresh_link_process_retains_gdal_without_coverage_dependencies(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import json, sys, coverage_model as worker
assert worker.gdal.VersionInfo()
from worker_context import WorkerContext, UNLOADED_MAINLAND
assert WorkerContext().mainland is UNLOADED_MAINLAND
print(json.dumps({name: name in sys.modules for name in (
    'osgeo.gdal', 'shapely', 'scipy.ndimage', 'scipy.spatial')}))
""",
            ],
            cwd=Path(worker.__file__).parent,
            env={**os.environ, "WORKER_MODE": "link"},
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(
            json.loads(result.stdout),
            {
                "osgeo.gdal": True,
                "shapely": False,
                "scipy.ndimage": False,
                "scipy.spatial": False,
            },
        )

    def test_tree_is_reused_and_rebuilt_after_context_invalidation(self):
        with mock.patch.object(self.context, "rf", RfRefreshState()):
            self.context.rf.install_support(
                np.array([[0.0, 0.0], [10.0, 0.0]]), ["a", "b"], {}, 0
            )
            first = rf_context.get_support_tree(context=self.context)
            self.assertIs(first, rf_context.get_support_tree(context=self.context))
            self.assertEqual(first.query([1.0, 0.0])[1], 0)
            self.context.rf.install_support(
                np.array([[10.0, 0.0], [0.0, 0.0]]), ["a", "b"], {}, 1
            )
            second = rf_context.get_support_tree(context=self.context)
            self.assertIsNot(first, second)
            self.assertEqual(second.query([1.0, 0.0])[1], 1)

    def test_mainland_loads_once_and_explicitly_disabled_mask_stays_disabled(self):
        polygon = object()
        with (
            mock.patch.object(self.context, "mainland", self.context.mainland),
            mock.patch.object(
                worker,
                "load_uk_mainland",
                return_value=polygon,
            ) as load,
        ):
            self.assertIs(worker.get_uk_mainland(context=self.context), polygon)
            self.assertIs(worker.get_uk_mainland(context=self.context), polygon)
            load.assert_called_once()
            self.context.mainland = None
            self.assertIsNone(worker.get_uk_mainland(context=self.context))
            load.assert_called_once()
