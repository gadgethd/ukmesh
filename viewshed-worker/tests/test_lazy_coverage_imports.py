import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

import numpy as np
import worker


class LazyCoverageImportsTests(unittest.TestCase):
    def test_fresh_link_process_retains_gdal_without_coverage_dependencies(self):
        result = subprocess.run(
            [sys.executable, '-c', '''
import json, sys, worker
assert worker.gdal.VersionInfo()
assert worker.UK_MAINLAND is worker._UNLOADED_MAINLAND
print(json.dumps({name: name in sys.modules for name in (
    'osgeo.gdal', 'shapely', 'scipy.ndimage', 'scipy.spatial')}))
'''],
            cwd=Path(worker.__file__).parent,
            env={**os.environ, 'WORKER_MODE': 'link'},
            check=True, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(json.loads(result.stdout), {
            'osgeo.gdal': True, 'shapely': False,
            'scipy.ndimage': False, 'scipy.spatial': False,
        })

    def test_tree_is_reused_and_rebuilt_after_context_invalidation(self):
        with mock.patch.dict(worker.SUPPORT_CONTEXT, {
            'tree': None, 'coordinates': np.array([[0., 0.], [10., 0.]]),
        }):
            first = worker.get_support_tree()
            self.assertIs(first, worker.get_support_tree())
            self.assertEqual(first.query([1., 0.])[1], 0)
            worker.SUPPORT_CONTEXT['coordinates'] = np.array([[10., 0.], [0., 0.]])
            worker.SUPPORT_CONTEXT['tree'] = None
            second = worker.get_support_tree()
            self.assertIsNot(first, second)
            self.assertEqual(second.query([1., 0.])[1], 1)

    def test_mainland_loads_once_and_explicitly_disabled_mask_stays_disabled(self):
        polygon = object()
        with mock.patch.object(worker, 'UK_MAINLAND', worker._UNLOADED_MAINLAND), mock.patch.object(
            worker, 'load_uk_mainland', return_value=polygon,
        ) as load:
            self.assertIs(worker.get_uk_mainland(), polygon)
            self.assertIs(worker.get_uk_mainland(), polygon)
            load.assert_called_once()
            worker.UK_MAINLAND = None
            self.assertIsNone(worker.get_uk_mainland())
            load.assert_called_once()
