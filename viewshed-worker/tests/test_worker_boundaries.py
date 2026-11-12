from worker_context import WorkerContext

"""Worker state and publication boundaries that must survive structural changes."""
from dataclasses import FrozenInstanceError
from unittest import mock
import unittest
import json
import time
import numpy as np
import link_topology
import link_model
import link_jobs
from queue_lease import LeaseGuard, LeaseProtocol
from rf_snapshots import RfRefreshState
from worker_types import LinkUpdate


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def test_configuration_and_published_context_are_immutable(self):
        with self.assertRaises(FrozenInstanceError):
            self.context.settings.RF_N_RAYS = 1
        state = RfRefreshState()
        state.install_support(np.array([[1.0, 2.0]]), ["a"], {"a": 10.0}, 123.0)
        old = state.support
        with self.assertRaises(TypeError):
            old.max_link_km_by_node["a"] = 20.0
        with self.assertRaises(ValueError):
            old.coordinates[0, 0] = 5.0
        state.install_support(np.array([[3.0, 4.0]]), ["b"], {}, 124.0)
        self.assertEqual(old.node_ids, ("a",))
        self.assertEqual(state.support.node_ids, ("b",))

    def test_runtime_contexts_keep_calibration_and_configuration_independent(self):
        from worker_settings import parse_settings
        from rf_context import current_usable_path_loss_db
        from rf.loss import compute_path_loss_from_profile

        first = WorkerContext(settings=parse_settings({"WORKER_MODE": "link"}))
        second = WorkerContext(settings=parse_settings({"WORKER_MODE": "viewshed"}))
        values = first.rf.calibration.values()
        values["usable_path_loss_db"] = 10.0
        first.rf.install_calibration(values)
        self.assertEqual(first.settings.WORKER_MODE, "link")
        self.assertEqual(second.settings.WORKER_MODE, "viewshed")
        profile = (np.array([0.0, 1000.0]), np.array([0.0, 0.0]), 10.0, 10.0)
        denied = compute_path_loss_from_profile(
            *profile, usable_threshold_db=current_usable_path_loss_db(context=first)
        )
        allowed = compute_path_loss_from_profile(
            *profile, usable_threshold_db=current_usable_path_loss_db(context=second)
        )
        self.assertFalse(denied.viable)
        self.assertTrue(allowed.viable)
        self.assertEqual(second.rf.calibration.samples, 0)

    def test_rebuild_topology_cannot_reuse_or_mutate_public_snapshot(self):
        public = link_topology.TopologyCache()
        rebuild = link_topology.TopologyCache("link_rebuild_0123456789abcdef")
        db = mock.MagicMock()
        db.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ("a", "b")
        ]
        with mock.patch.object(
            link_topology,
            "load_positioned_repeaters",
            side_effect=[{"public": {"lat": 1.0}}, {"rebuild": {"lat": 2.0}}],
        ):
            public.refresh(db, force=True)
            rebuild.refresh(db, force=True)
        old = public.snapshot
        rebuild.include_pair("x", "y")
        self.assertEqual(set(public.snapshot.nodes), {"public"})
        self.assertEqual(set(rebuild.snapshot.nodes), {"rebuild"})
        self.assertNotIn(("x", "y"), old.physical_pairs)
        with self.assertRaises(TypeError):
            public.snapshot.nodes["public"]["lat"] = 4.0


class LinkCommitTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def fixture(self):
        db = mock.MagicMock()
        cur = db.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None
        guard = mock.MagicMock(unsafe=True)
        guard.__enter__.return_value = guard
        return db, cur, guard

    def test_lease_loss_rolls_back_before_ack(self):
        db, _cur, guard = self.fixture()
        previous = self.context.topology.snapshot

        def provisional_pair(_db, _job, topology, **_kwargs):
            topology.include_pair("a", "b")
            return []

        with (
            mock.patch.object(link_jobs, "LeaseGuard", return_value=guard),
            mock.patch.object(
                link_jobs, "process_link_job", side_effect=provisional_pair
            ),
            mock.patch.object(
                link_jobs.link_queue_v3, "owns_lease", return_value=False
            ),
            mock.patch.object(link_jobs.link_queue_v3, "nack", return_value="retry"),
            mock.patch.object(link_jobs.link_queue_v3, "ack") as ack,
        ):
            with self.assertRaisesRegex(RuntimeError, "LINK_LEASE_LOST"):
                link_jobs.process_claimed_link_job(
                    db,
                    object(),
                    ("job", "token", {"type": "observe"}, 1),
                    context=self.context,
                )
        db.commit.assert_not_called()
        db.rollback.assert_called_once()
        ack.assert_not_called()
        self.assertIs(self.context.topology.snapshot, previous)

    def test_committed_redelivery_does_not_write_graph_again(self):
        db, cur, guard = self.fixture()
        cur.fetchone.return_value = (1,)
        with (
            mock.patch.object(link_jobs, "LeaseGuard", return_value=guard),
            mock.patch.object(link_jobs, "process_link_job") as process,
            mock.patch.object(link_jobs.link_queue_v3, "ack", return_value=True) as ack,
        ):
            link_jobs.process_claimed_link_job(
                db,
                object(),
                ("job", "token", {"type": "observe"}, 2),
                context=self.context,
            )
        process.assert_not_called()
        ack.assert_called_once()
        db.commit.assert_called_once()

    def test_rebuild_has_explicit_scoped_context_and_commits_before_ack(self):
        db, _cur, guard = self.fixture()
        events = []
        db.commit.side_effect = lambda: events.append("commit")
        with (
            mock.patch.object(link_jobs, "LeaseGuard", return_value=guard),
            mock.patch.object(link_jobs, "process_link_job") as process,
            mock.patch.object(link_jobs.link_queue_v3, "owns_lease", return_value=True),
            mock.patch.object(
                link_jobs.link_queue_v3,
                "ack",
                side_effect=lambda *_: events.append("ack") or True,
            ),
        ):
            generation = "link_rebuild_0123456789abcdef"
            link_jobs.process_claimed_link_job(
                db,
                object(),
                ("job", "token", {"generation": generation}, 1),
                context=self.context,
            )
        self.assertEqual(events, ["commit", "ack"])
        self.assertEqual(process.call_args.args[2].scope, generation)
        self.assertIsNot(process.call_args.args[2], self.context.topology)

    def test_live_updates_wait_for_the_lease_fence_commit_and_guard_exit(self):
        for job_type in ("observe", "physical_pair"):
            for failure in ("lease", "commit", "guard_exit", None):
                with self.subTest(job_type=job_type, failure=failure):
                    self.context = WorkerContext()
                    a, b = "aa" * 32, "bb" * 32
                    self.context.topology.snapshot = link_topology.TopologySnapshot(
                        {a: {"lat": 51.5, "lon": -1.2}, b: {"lat": 51.6, "lon": -1.3}},
                        frozenset(),
                        time.time(),
                    )
                    previous = self.context.topology.snapshot
                    db, _cur, guard = self.fixture()
                    events = []
                    db.commit.side_effect = (
                        RuntimeError("commit failed")
                        if failure == "commit"
                        else lambda: events.append("commit")
                    )

                    def exit_guard(*_args):
                        if failure == "guard_exit":
                            raise RuntimeError("LINK_LEASE_LOST:job")
                        events.append("guard_exit")
                        return False

                    guard.__exit__.side_effect = exit_guard
                    client = mock.Mock()
                    published = []

                    def publish(channel, payload):
                        events.append("publish")
                        published.append(
                            (
                                channel,
                                payload,
                                db.autocommit,
                                self.context.topology.snapshot,
                            )
                        )

                    client.publish.side_effect = publish
                    job = {
                        "type": job_type,
                        "node_a_id": a,
                        "node_b_id": b,
                        "rx_node_id": b,
                        "src_node_id": a,
                        "path_hashes": [a[:4]],
                        "path_hash_size_bytes": 2,
                    }
                    with (
                        mock.patch.object(link_jobs, "LeaseGuard", return_value=guard),
                        mock.patch.object(
                            link_model,
                            "ensure_physical_link_metrics",
                            return_value=(8, 90.0, True, 7, 1, 8),
                        ),
                        mock.patch.object(
                            link_model,
                            "upsert_link_pair",
                            return_value=(8, None, 90.0, True, 7, 1, 8),
                        ),
                        mock.patch.object(
                            link_jobs.link_queue_v3,
                            "owns_lease",
                            return_value=failure != "lease",
                        ),
                        mock.patch.object(
                            link_jobs.link_queue_v3, "nack", return_value="retry"
                        ),
                        mock.patch.object(
                            link_jobs.link_queue_v3,
                            "ack",
                            side_effect=lambda *_: events.append("ack") or True,
                        ) as ack,
                    ):
                        if failure:
                            with self.assertRaises(RuntimeError):
                                link_jobs.process_claimed_link_job(
                                    db,
                                    client,
                                    ("job", "token", job, 1),
                                    context=self.context,
                                )
                            client.publish.assert_not_called()
                            ack.assert_not_called()
                            if failure != "guard_exit":
                                db.rollback.assert_called_once()
                                self.assertIs(self.context.topology.snapshot, previous)
                        else:
                            link_jobs.process_claimed_link_job(
                                db,
                                client,
                                ("job", "token", job, 1),
                                context=self.context,
                            )
                            self.assertEqual(
                                events, ["commit", "guard_exit", "publish", "ack"]
                            )
                            channel, payload, autocommit, snapshot = published[0]
                            self.assertEqual(
                                channel, self.context.settings.LIVE_CHANNEL
                            )
                            self.assertTrue(autocommit)
                            self.assertIsNot(snapshot, previous)
                            message = json.loads(payload)
                            self.assertEqual(message["type"], "link_update")
                            self.assertIsInstance(message["ts"], int)
                            self.assertEqual(
                                message["data"],
                                {
                                    "node_a_id": a,
                                    "node_b_id": b,
                                    "observed_count": 8,
                                    "itm_path_loss_db": 90.0,
                                    "itm_viable": True,
                                    "count_a_to_b": 7,
                                    "count_b_to_a": 1,
                                    "multibyte_observed_count": 8,
                                },
                            )
                            db.rollback.assert_not_called()

    def test_failed_live_hint_does_not_retry_an_already_committed_graph(self):
        db, cur, guard = self.fixture()
        client = mock.Mock()
        client.publish.side_effect = ConnectionError("live channel unavailable")
        update = LinkUpdate("a", "b", 8, 90.0, True, 7, 1, 8)
        with (
            mock.patch.object(link_jobs, "LeaseGuard", return_value=guard),
            mock.patch.object(
                link_jobs, "process_link_job", return_value=[update]
            ) as process,
            mock.patch.object(link_jobs.link_queue_v3, "owns_lease", return_value=True),
            mock.patch.object(link_jobs.link_queue_v3, "nack") as nack,
            mock.patch.object(link_jobs.link_queue_v3, "ack", return_value=True) as ack,
        ):
            with self.assertLogs("worker", level="WARNING") as logs:
                link_jobs.process_claimed_link_job(
                    db, client, ("job", "token", {}, 1), context=self.context
                )
            self.assertIn(
                "Committed link job job live notification failed", logs.output[0]
            )
            db.rollback.assert_not_called()
            ack.assert_called_once()
            nack.assert_not_called()
            cur.fetchone.return_value = (1,)
            link_jobs.process_claimed_link_job(
                db, client, ("job", "new-token", {}, 2), context=self.context
            )
            process.assert_called_once()
            client.publish.assert_called_once()
            self.assertEqual(ack.call_count, 2)


class LeaseGuardTests(unittest.TestCase):
    def setUp(self):
        self.context = WorkerContext()

    def test_rejected_renewal_fences_the_current_owner(self):
        client = mock.Mock()
        client.eval.return_value = 0
        protocol = LeaseProtocol("link", 120000, "lua", "states", "tokens", "leases")
        guard = LeaseGuard(lambda: client, "job", "token", protocol)
        with mock.patch.object(guard.stop_event, "wait", return_value=False):
            guard._run()
        with self.assertRaisesRegex(RuntimeError, "LINK_LEASE_LOST:job"):
            guard.assert_owned()
        client.close.assert_called_once()

    def test_initial_connection_failure_can_recover_without_losing_lease(self):
        client = mock.Mock()
        client.eval.return_value = 1
        factory = mock.Mock(side_effect=[ConnectionError("fixture"), client])
        guard = LeaseGuard(
            factory,
            "job",
            "token",
            LeaseProtocol("viewshed", 900000, "lua", "states", "tokens", "leases"),
        )
        with mock.patch.object(
            guard.stop_event, "wait", side_effect=[False, False, True]
        ):
            guard._run()
        guard.assert_owned()
        self.assertEqual(factory.call_count, 2)
        client.eval.assert_called_once()
        client.close.assert_called_once()
