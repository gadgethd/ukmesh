from worker_context import WorkerContext

"""Worker state and publication boundaries that must survive structural changes."""
from dataclasses import FrozenInstanceError
from unittest import mock
import unittest
import numpy as np
import link_topology
import link_jobs
from queue_lease import LeaseGuard, LeaseProtocol
from rf_snapshots import RfRefreshState


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

        def provisional_pair(_db, _redis, _job, topology, **_kwargs):
            topology.include_pair("a", "b")

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
        self.assertEqual(process.call_args.args[3].scope, generation)
        self.assertIsNot(process.call_args.args[3], self.context.topology)


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
