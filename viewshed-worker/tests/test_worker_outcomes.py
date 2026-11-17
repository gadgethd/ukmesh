from worker_context import WorkerContext
from contextlib import ExitStack
from dataclasses import replace
import json
from types import SimpleNamespace
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

    def test_link_idle_wait_ignores_unused_support_and_preserves_reaping(self):
        now = 1000.0
        with (
            mock.patch.object(
                self.context.topology, "snapshot", mock.Mock(updated_at=now)
            ),
            mock.patch.object(self.context.rf, "support", mock.Mock(updated_at=0.0)),
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

    def test_link_idle_wait_honors_topology_and_calibration_deadlines(self):
        now = 1000.0
        for owner, interval in (
            (self.context.topology, self.context.settings.LINK_TOPOLOGY_REFRESH_S),
            (self.context.rf, runtime.CALIBRATION_REFRESH_S),
        ):
            with (
                self.subTest(owner=type(owner).__name__),
                mock.patch.object(
                    self.context.topology, "snapshot", mock.Mock(updated_at=now)
                ),
                mock.patch.object(
                    self.context.rf, "calibration", mock.Mock(updated_at=now)
                ),
                mock.patch.object(self.context.rf, "support", mock.Mock(updated_at=0.0)),
            ):
                attribute = (
                    "snapshot" if owner is self.context.topology else "calibration"
                )
                with mock.patch.object(
                    owner, attribute, mock.Mock(updated_at=now - interval + 0.125)
                ):
                    self.assertAlmostEqual(
                        runtime.link_idle_wait_seconds(now, now, context=self.context),
                        0.125,
                    )

    def test_mode_loop_refreshes_only_consumed_snapshots_and_dispatches_jobs(self):
        for mode in ("link", "viewshed", "all"):
            with self.subTest(mode=mode), ExitStack() as stack:
                context = WorkerContext(
                    settings=replace(
                        self.context.settings, WORKER_MODE=mode, VIEWSHED_ENABLED=True
                    )
                )
                db = FakeDb()
                redis = mock.Mock()
                redis.exists.return_value = True
                redis.blpop.side_effect = KeyboardInterrupt
                job = {"planned_job_id": "fixture-planned"}
                redis.brpop.return_value = (context.settings.JOB_QUEUE, json.dumps(job))
                link_queue = mock.Mock()
                claimed_link = ("fixture-link", "token", {}, 1)
                link_queue.claim.side_effect = [claimed_link, None]
                link_queue.reap.return_value = 0
                coverage_queue = mock.Mock()
                coverage_queue.claim.return_value = None
                coverage_queue.reap.return_value = 0
                support_refresh = mock.Mock()
                calibration_refresh = mock.Mock()
                physical_job = mock.Mock()
                planned_job = mock.Mock(side_effect=KeyboardInterrupt)
                topology_refresh = stack.enter_context(
                    mock.patch.object(context.topology, "refresh")
                )
                for name, value in {
                    "gdal": mock.Mock(),
                    "threading": mock.Mock(),
                    "wait_for_db": mock.Mock(return_value=db),
                    "redis_connection": mock.Mock(return_value=redis),
                    "heartbeat": mock.Mock(),
                    "link_queue_v3": link_queue,
                    "viewshed_queue_v2": coverage_queue,
                    "refresh_radio_neighbor_reports": mock.Mock(),
                    "refresh_rf_calibration": calibration_refresh,
                    "refresh_support_context": support_refresh,
                    "recover_planned_coverage_jobs": mock.Mock(),
                    "release_viewshed_queue_payload": mock.Mock(),
                    "process_claimed_link_job": physical_job,
                    "process_planned_job": planned_job,
                }.items():
                    stack.enter_context(mock.patch.object(runtime, name, value))
                stack.enter_context(
                    mock.patch.object(runtime.time, "time", return_value=1000.0)
                )
                stack.enter_context(
                    mock.patch.object(
                        runtime.multiprocessing,
                        "current_process",
                        return_value=SimpleNamespace(name="MainProcess"),
                    )
                )

                with self.assertRaises(KeyboardInterrupt):
                    runtime.worker_loop(context=context)

                self.assertEqual(
                    calibration_refresh.call_args_list,
                    [
                        mock.call(db, force=True, context=context),
                        mock.call(db, context=context),
                    ],
                )
                if mode == "link":
                    support_refresh.assert_not_called()
                    planned_job.assert_not_called()
                    redis.blpop.assert_called_once()
                else:
                    self.assertEqual(
                        support_refresh.call_args_list,
                        [
                            mock.call(db, force=True, context=context),
                            mock.call(db, context=context),
                        ],
                    )
                    planned_job.assert_called_once_with(db, redis, job, context=context)
                if mode in ("all", "link"):
                    self.assertEqual(
                        topology_refresh.call_args_list,
                        [mock.call(db, force=True), mock.call(db)],
                    )
                    physical_job.assert_called_once_with(
                        db, redis, claimed_link, context=context
                    )
                else:
                    topology_refresh.assert_not_called()
                    physical_job.assert_not_called()

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
