from worker_context import WorkerContext
import unittest
from unittest import mock

import coverage_jobs as worker
import worker_runtime as runtime
from rf.terrain import RetryableTerrainError


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=()):
        self.db.statements.append((query, params))
        if "SELECT n.name, n.role" in query:
            self._row = ("fixture", 2, None)
        else:
            self._row = None

    def fetchone(self):
        return self._row


class FakeDb:
    def __init__(self):
        self.statements = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class FakeRedis:
    def __init__(self):
        self.removed = []

    def srem(self, key, value):
        self.removed.append((key, value))


class CoverageOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def test_link_idle_wait_is_blocking_but_preserves_five_second_reaping(self):
        now = 1000.0
        with (
            mock.patch.object(
                self.context.topology, "snapshot", mock.Mock(updated_at=now)
            ),
            mock.patch.object(self.context.rf, "support", mock.Mock(updated_at=now)),
            mock.patch.object(
                self.context.rf, "calibration", mock.Mock(updated_at=now)
            ),
        ):
            self.assertEqual(
                runtime.link_idle_wait_seconds(now, now, context=self.context), 5.0
            )
            self.assertAlmostEqual(
                runtime.link_idle_wait_seconds(now, now - 4.75, context=self.context),
                0.25,
            )

    def test_transient_calculation_failure_writes_no_success_marker(self):
        db = FakeDb()
        redis = FakeRedis()
        job = {"node_id": "a" * 64, "lat": 51.5, "lon": -1.2}

        with (
            mock.patch.object(worker, "already_calculated", return_value=False),
            mock.patch.object(
                worker,
                "calculate_viewshed",
                side_effect=RetryableTerrainError("fixture timeout"),
            ),
        ):
            with self.assertRaises(RetryableTerrainError):
                worker.process_job(db, redis, job, context=self.context)

        self.assertEqual(db.commits, 0)
        self.assertFalse(
            any("INSERT INTO core.node_coverage" in query for query, _ in db.statements)
        )
        self.assertEqual(redis.removed, [])

    def test_out_of_scope_job_records_explicit_permanent_status(self):
        db = FakeDb()
        redis = FakeRedis()
        job = {"node_id": "b" * 64, "lat": 0.0, "lon": 0.0}

        with self.assertRaises(worker.PermanentOutOfScope):
            worker.process_job(db, redis, job, context=self.context)

        inserts = [
            query
            for query, _ in db.statements
            if "INSERT INTO core.node_coverage" in query
        ]
        self.assertEqual(len(inserts), 1)
        self.assertIn("'permanent'", inserts[0])
        self.assertEqual(db.commits, 1)


if __name__ == "__main__":
    unittest.main()
