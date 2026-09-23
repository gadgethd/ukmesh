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
        self.publish_count = 0
        self.fail_publish_at = None
        self.fail_marker_set = False

    def exists(self, key):
        return 1 if key in self.data else 0

    def set(self, key, value, ex=None):
        if self.fail_marker_set and key.startswith('viewshed:side-effects:'):
            self.fail_marker_set = False
            raise RuntimeError('injected completion-marker failure')
        self.data[key] = value

    def publish(self, channel, message):
        self.publish_count += 1
        if self.publish_count == self.fail_publish_at:
            raise RuntimeError('injected notification failure')
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


class SequenceDb:
    """Return one row set per cursor so process_job retry paths are realistic."""
    def __init__(self, cursor_rows):
        self.cursor_rows = iter(cursor_rows)

    def cursor(self):
        return FakeCursor(next(self.cursor_rows))


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

    def test_post_commit_faults_replay_on_retry_at_every_boundary(self):
        """Coverage is already committed; any incomplete Redis effect must retry."""
        node = 'F' * 64
        geom = {'type': 'Polygon', 'coordinates': []}
        strength = {'s1': {'type': 'Polygon', 'coordinates': []}}

        for boundary in ('link_admission', 'coverage_notification', 'node_notification', 'marker'):
            with self.subTest(boundary=boundary):
                r = FakeRedis()
                r.fail_publish_at = {
                    'coverage_notification': 1,
                    'node_notification': 2,
                }.get(boundary)
                r.fail_marker_set = boundary == 'marker'
                db = SequenceDb([
                    (None, 2, None), (geom, strength, 5_000.0, 12.0), (54.0, -1.5),
                    (None, 2, None), (geom, strength, 5_000.0, 12.0), (54.0, -1.5),
                ])
                enqueue_calls = 0

                def enqueue(*args, **kwargs):
                    nonlocal enqueue_calls
                    enqueue_calls += 1
                    if boundary == 'link_admission' and enqueue_calls == 1:
                        raise RuntimeError('injected link admission failure')
                    return 1

                job = {'node_id': node, 'lat': 54.0, 'lon': -1.5}
                with mock.patch.object(worker, 'already_calculated', return_value=True), \
                     mock.patch.object(worker, 'WORKER_MODE', 'all'), \
                     mock.patch.object(worker, 'enqueue_physical_link_jobs_for_node', side_effect=enqueue):
                    with self.assertRaises(RuntimeError):
                        worker.process_job(db, r, job)
                    self.assertFalse(worker.side_effects_complete(r, node))

                    # The next delivery takes the already-calculated branch and
                    # reconciles the missing side effects before returning.
                    worker.process_job(db, r, job)

                self.assertTrue(worker.side_effects_complete(r, node))
                expected_notifications = {
                    'node_notification': 3,
                    'marker': 4,
                }.get(boundary, 2)
                self.assertEqual(len(r.published), expected_notifications)


if __name__ == '__main__':
    unittest.main()
