"""Focused unit test for BUG-006 side-effect marker + replay logic.

Runs without GDAL by importing only the pure helper functions via a stub
module shim. Covers:
- side_effects_complete / mark_side_effects_complete round-trip
- replay path publishes both notifications and re-enqueues link jobs
- success path marks completion
"""
import json
import sys
import types
import unittest
from unittest import mock

# ---- Stub the heavy third-party modules so we can import worker.py's helpers ----
for mod_name in ('osgeo', 'osgeo.gdal', 'osgeo.ogr', 'osgeo.osr'):
    sys.modules.setdefault(mod_name, types.ModuleType(mod_name))
sys.modules.setdefault('psycopg2', types.ModuleType('psycopg2'))
sys.modules.setdefault('psycopg2.extras', types.ModuleType('psycopg2.extras'))

import worker  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.published = []

    def exists(self, key):
        return 1 if key in self.data else 0

    def set(self, key, value, ex=None):
        self.data[key] = value

    def publish(self, channel, message):
        self.published.append((channel, json.loads(message)))


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._idx = 0
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchone(self):
        if self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            return row
        return None


class FakeDb:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return FakeCursor(self.rows)


class SideEffectMarkerTest(unittest.TestCase):
    def test_marker_round_trip(self):
        r = FakeRedis()
        node = 'A' * 64
        self.assertFalse(worker.side_effects_complete(r, node))
        worker.mark_side_effects_complete(r, node)
        self.assertTrue(worker.side_effects_complete(r, node))

    def test_marker_read_failure_treated_as_incomplete(self):
        r = FakeRedis()
        r.exists = mock.Mock(side_effect=RuntimeError('redis down'))
        self.assertFalse(worker.side_effects_complete(r, 'B' * 64))

    def test_replay_publishes_notifications_and_marks_complete(self):
        r = FakeRedis()
        node = 'C' * 64
        geom = {'type': 'Polygon', 'coordinates': []}
        strength = {'s1': {'type': 'Polygon', 'coordinates': []}}
        db = FakeDb([
            (geom, strength, 5000.0, 12.0),   # coverage row
            (54.0, -1.5),                     # node position for link replay
        ])
        with mock.patch.object(worker, 'enqueue_physical_link_jobs_for_node', return_value=2) as enqueue:
            worker.replay_coverage_side_effects(db, r, node)
        channels = [ch for ch, _ in r.published]
        self.assertEqual(channels, [worker.LIVE_CHANNEL, worker.LIVE_CHANNEL])
        types_ = [m['type'] for _, m in r.published]
        self.assertIn('coverage_update', types_)
        self.assertIn('node_upsert', types_)
        self.assertTrue(worker.side_effects_complete(r, node))
        enqueue.assert_called_once()

    def test_replay_without_position_skips_links_but_notifies(self):
        r = FakeRedis()
        node = 'D' * 64
        geom = {'type': 'Polygon', 'coordinates': []}
        db = FakeDb([
            (geom, None, None, 5.0),  # coverage row, NULL strength/radius
            (None, None),             # node position missing
        ])
        with mock.patch.object(worker, 'enqueue_physical_link_jobs_for_node') as enqueue:
            worker.replay_coverage_side_effects(db, r, node)
        self.assertEqual(len(r.published), 2)
        self.assertTrue(worker.side_effects_complete(r, node))
        enqueue.assert_not_called()

    def test_replay_missing_coverage_row_is_noop(self):
        r = FakeRedis()
        db = FakeDb([None])
        worker.replay_coverage_side_effects(db, r, 'E' * 64)
        self.assertEqual(r.published, [])
        self.assertFalse(worker.side_effects_complete(r, 'E' * 64))


if __name__ == '__main__':
    unittest.main()
