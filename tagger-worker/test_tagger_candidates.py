"""Exercise the actual poll SQL without contacting the classification service."""

from datetime import datetime, timedelta, timezone
import io
import json
import os
import time
from unittest.mock import patch
import unittest
from urllib.parse import urlsplit
from uuid import uuid4
import psycopg2
from psycopg2 import sql

from tagger_config import TaggerSettings
import tagger_client
import tagger_repository as repository
import tagger_service
import tagger_worker

URL = os.environ.get("TEST_INGEST_DATABASE_URL")


class ClassifierClientTests(unittest.TestCase):
    def test_local_backend_normalizes_batch_response(self):
        settings = TaggerSettings.from_env({
            "DATABASE_URL": "unused",
            "TAGGER_BACKEND": "local",
            "TAGGER_URL": "http://127.0.0.1:8000/v1/tag/batch",
            "TAGGER_API_KEY": "fixture-key",
        })
        response = {"results": [{
            "kind": {"value": "chat", "p": 0.9},
            "topic": {"value": "general", "p": 0.8},
            "speaker": {"value": "human", "p": 0.7},
            "flags": {"mentions_location": 0.2, "directed": 0.3, "safety": 0.1},
        }]}
        with patch("tagger_client.urllib.request.urlopen", return_value=io.BytesIO(
            json.dumps(response).encode()
        )) as send:
            result, _ = tagger_client.classify("example node", "example message", settings)
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, settings.api_url)
        self.assertEqual(request.get_header("Authorization"), "Bearer fixture-key")
        self.assertEqual(json.loads(request.data), {
            "items": [{"sender": "example node", "message": "example message"}],
        })
        self.assertEqual(tagger_client.flatten(result["answers"]), (
            {"kind": "chat", "topic": "general", "speaker": "human",
             "mentions_location": 0.2, "directed": 0.3, "safety": 0.1},
            {"kind": 0.9, "topic": 0.8, "speaker": 0.7},
        ))

    def test_jev_remains_default_with_existing_request_contract(self):
        settings = TaggerSettings.from_env({
            "DATABASE_URL": "unused",
            "TYPESAFE_API_URL": "http://127.0.0.1:8000/tag",
            "TYPESAFE_API_KEY": "fixture-key",
        })
        with patch("tagger_client.urllib.request.urlopen", return_value=io.BytesIO(
            b'{"answers": {}}'
        )) as send:
            result, _ = tagger_client.classify(None, "x" * 4001, settings)
        self.assertEqual(json.loads(send.call_args.args[0].data), {
            "state": f"Message from node 'unknown': \"{'x' * 4000}\"",
            "model": "jev-latest",
            "questions": tagger_client.QUESTIONS,
        })
        self.assertEqual(result, {"answers": {}})


@unittest.skipUnless(URL, "TEST_INGEST_DATABASE_URL is required")
class TaggerDatabaseFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if urlsplit(URL).hostname not in ("127.0.0.1", "localhost"):
            raise RuntimeError("use an owned loopback database fixture")
        cls.admin = psycopg2.connect(URL)
        cls.admin.autocommit = True
        cls.database = "audit_tagger_" + uuid4().hex
        with cls.admin.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database))
            )
        cls.conn = psycopg2.connect(URL, dbname=cls.database)
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            cur.execute("""CREATE SCHEMA mesh; CREATE SCHEMA ops;
              CREATE TABLE mesh.packets (packet_hash text, time timestamptz, payload jsonb,
              packet_type int, network text, visibility_ok boolean, is_private boolean, rx_node_id text);
              CREATE TABLE ops.privacy_rematerialization_queue (status text);
              CREATE TABLE ops.public_visibility_state (singleton boolean, generation int);
              CREATE TABLE ops.packet_visibility_materialization_state (singleton boolean, visibility_generation int);
              INSERT INTO ops.public_visibility_state VALUES (true, 1);
              INSERT INTO ops.packet_visibility_materialization_state VALUES (true, 1);""")
            repository.bootstrap_schema(cur)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        with cls.admin.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(cls.database)
                )
            )
        cls.admin.close()



class CandidatePagingTests(TaggerDatabaseFixture):
    def setUp(self):
        self.cursor_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        with self.conn.cursor() as cur:
            cur.execute(
                "TRUNCATE mesh.packets, mesh.message_tags, ops.tagger_retry, ops.privacy_rematerialization_queue; UPDATE ops.public_visibility_state SET generation=1; UPDATE ops.packet_visibility_materialization_state SET visibility_generation=1"
            )

    def add(self, packet_hash, time, receiver=None):
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mesh.packets VALUES (%s,%s,
              '{"decrypted":{"sender":"fixture","message":"fixture"}}',5,'ukmesh',true,false,%s)""",
                (packet_hash, time, receiver),
            )

    def candidates(self):
        with self.conn.cursor() as cur:
            return [
                row[0]
                for row in repository.candidates(
                    cur, self.cursor_time, 120, ["ukmesh"], 200
                )
            ]

    def test_completed_overlap_cannot_starve_new_messages(self):
        for index in range(200):
            key = "completed-" + str(index)
            self.add(
                key, self.cursor_time - timedelta(seconds=100, milliseconds=-index)
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mesh.message_tags (packet_hash, packet_time, tags) VALUES (%s, now(), '{}')", (key,)
                )
        self.add("new-message", self.cursor_time + timedelta(seconds=1))
        self.assertEqual(self.candidates(), ["new-message"])

    def test_retry_queue_owns_both_pending_and_exhausted_retries(self):
        for key, exhausted in [("retrying", False), ("exhausted", True)]:
            self.add(key, self.cursor_time + timedelta(seconds=1))
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ops.tagger_retry (packet_hash, packet_time, exhausted) VALUES (%s, now(), %s)", (key, exhausted)
                )
        self.assertEqual(self.candidates(), [])

    def classify(self, _sender, _message):
        raise AssertionError("unexpected external classifier call")

    def settings(self):
        return TaggerSettings.from_env({"DATABASE_URL": URL})

    def test_private_retry_never_reaches_classifier(self):
        self.add("private-retry", self.cursor_time)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE mesh.packets SET is_private=true, visibility_ok=false")
            with patch.object(
                self,
                "classify",
                return_value=({"answers": {"kind": {"choice": "chat"}}}, 1),
            ) as classifier:
                self.assertEqual(
                    tagger_service.tag_message(
                        cur,
                        "private-retry",
                        self.cursor_time,
                        None,
                        "ukmesh",
                        self.settings(),
                        classifier,
                        sleep=lambda _: None,
                    )[0],
                    "skipped",
                )
                classifier.assert_not_called()

    def test_unavailable_policy_keeps_retry_retryable(self):
        self.add("blocked-retry", self.cursor_time)
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.packet_visibility_materialization_state SET visibility_generation=2"
            )
            with patch.object(
                self, "classify", return_value=({"answers": {}}, 1)
            ) as classifier:
                with self.assertRaisesRegex(RuntimeError, "visibility"):
                    tagger_service.tag_message(
                        cur,
                        "blocked-retry",
                        self.cursor_time,
                        None,
                        "ukmesh",
                        self.settings(),
                        classifier,
                        sleep=lambda _: None,
                    )
                classifier.assert_not_called()

    def test_allowed_retry_still_gets_classified_and_persisted(self):
        self.add("public-retry", self.cursor_time)
        with self.conn.cursor() as cur:
            with patch.object(
                self,
                "classify",
                return_value=({"answers": {"kind": {"choice": "chat"}}}, 1),
            ) as classifier:
                self.assertEqual(
                    tagger_service.tag_message(
                        cur,
                        "public-retry",
                        self.cursor_time,
                        None,
                        "ukmesh",
                        self.settings(),
                        classifier,
                        sleep=lambda _: None,
                    )[0],
                    "tagged",
                )
                classifier.assert_called_once()
                cur.execute(
                    "SELECT tags FROM mesh.message_tags WHERE packet_hash='public-retry'"
                )
                self.assertEqual(cur.fetchone()[0], {"kind": "chat"})

    def test_privacy_revocation_during_backoff_prevents_another_external_attempt(self):
        self.add("revoked-during-retry", self.cursor_time)

        def revoke_and_fail(_sender, _message):
            with self.conn.cursor() as update:
                update.execute(
                    "UPDATE mesh.packets SET is_private=true, visibility_ok=false"
                )
            raise RuntimeError("fixture classifier unavailable")

        with (
            self.conn.cursor() as cur,
            patch.object(self, "classify", side_effect=revoke_and_fail) as classifier,
        ):
            self.assertEqual(
                tagger_service.tag_message(
                    cur,
                    "revoked-during-retry",
                    self.cursor_time,
                    None,
                    "ukmesh",
                    self.settings(),
                    classifier,
                    sleep=lambda _: None,
                )[0],
                "skipped",
            )
            classifier.assert_called_once()

    def test_another_public_receiver_cannot_authorize_a_private_copy(self):
        self.add("shared-hash", self.cursor_time, "private-receiver")
        self.add("shared-hash", self.cursor_time, "public-receiver")
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE mesh.packets SET is_private=true, visibility_ok=false WHERE rx_node_id='private-receiver'"
            )
            with patch.object(self, "classify") as classifier:
                self.assertEqual(
                    tagger_service.tag_message(
                        cur,
                        "shared-hash",
                        self.cursor_time,
                        "private-receiver",
                        "ukmesh",
                        self.settings(),
                        classifier,
                        sleep=lambda _: None,
                    )[0],
                    "skipped",
                )
                classifier.assert_not_called()

    def test_retry_lookup_distinguishes_unavailable_policy_from_private_packet(self):
        self.add("queued-retry", self.cursor_time)
        with self.conn.cursor() as cur:
            self.assertEqual(
                repository.public_packet_keys(
                    cur, "queued-retry", self.cursor_time, self.settings().networks
                ),
                ("queued-retry", self.cursor_time, None, "ukmesh"),
            )
            cur.execute(
                "INSERT INTO ops.privacy_rematerialization_queue VALUES ('processing')"
            )
            with self.assertRaisesRegex(RuntimeError, "visibility"):
                repository.public_packet_keys(
                    cur, "queued-retry", self.cursor_time, self.settings().networks
                )
            cur.execute("TRUNCATE ops.privacy_rematerialization_queue")
            cur.execute("UPDATE mesh.packets SET is_private=true, visibility_ok=false")
            self.assertIsNone(
                repository.public_packet_keys(
                    cur, "queued-retry", self.cursor_time, self.settings().networks
                )
            )


class SchemaOwnershipTests(TaggerDatabaseFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.role = "audit_tagger_" + uuid4().hex
        cls.runtime_dsn = psycopg2.extensions.make_dsn(
            URL, dbname=cls.database, user=cls.role, password="fixture-tagger-only"
        )
        with cls.admin.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD %s").format(
                    sql.Identifier(cls.role)
                ), ("fixture-tagger-only",),
            )
        with cls.conn.cursor() as cur:
            cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        cls.grant_runtime_access()

    @classmethod
    def grant_runtime_access(cls):
        with cls.conn.cursor() as cur:
            for statement in (
                "GRANT USAGE ON SCHEMA mesh, ops TO {}",
                "GRANT SELECT ON ALL TABLES IN SCHEMA mesh, ops TO {}",
                "GRANT INSERT ON mesh.message_tags TO {}",
                "GRANT INSERT, UPDATE, DELETE ON ops.tagger_state, ops.tagger_retry TO {}",
            ):
                cur.execute(sql.SQL(statement).format(sql.Identifier(cls.role)))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        with psycopg2.connect(URL) as admin:
            admin.autocommit = True
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(cls.role)))

    def test_runtime_without_schema_ownership_can_connect_and_tag(self):
        settings = TaggerSettings.from_env({"DATABASE_URL": self.runtime_dsn})
        conn, cur = tagger_worker.connect_database(settings)
        try:
            ts = datetime.now(timezone.utc)
            with self.conn.cursor() as admin:
                admin.execute(
                    """INSERT INTO mesh.packets VALUES (%s,%s,
                    '{"decrypted":{"sender":"fixture","message":"fixture"}}',
                    5,'ukmesh',true,false,NULL)""", ("owned-schema-message", ts),
                )
            result = tagger_service.tag_message(
                cur, "owned-schema-message", ts, None, "ukmesh", settings,
                lambda _sender, _message: ({"answers": {"kind": {"choice": "chat"}}}, 1),
                sleep=lambda _: None,
            )
            self.assertEqual(result[0], "tagged")
            cur.execute(
                "SELECT tags FROM mesh.message_tags WHERE packet_hash=%s",
                ("owned-schema-message",),
            )
            self.assertEqual(cur.fetchone()[0], {"kind": "chat"})
            cur.execute("SELECT to_regclass('public.message_tags')")
            self.assertIsNone(cur.fetchone()[0])
            for statement in (
                "CREATE TABLE mesh.runtime_ddl (id integer)",
                "ALTER TABLE ops.tagger_state ADD COLUMN runtime_ddl integer",
            ):
                with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute(statement)
        finally:
            cur.close()
            conn.close()

    def test_missing_schema_fails_with_migration_action(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM ops.tagger_state WHERE id=1")
        try:
            with self.assertRaisesRegex(RuntimeError, "Apply UKMesh migrations"):
                tagger_worker.connect_database(
                    TaggerSettings.from_env({"DATABASE_URL": self.runtime_dsn})
                )
        finally:
            with self.conn.cursor() as cur:
                cur.execute("INSERT INTO ops.tagger_state (id) VALUES (1)")

    def test_explicit_owner_bootstrap_initializes_without_classifier_key(self):
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE mesh.message_tags, ops.tagger_state, ops.tagger_retry")
        try:
            settings = TaggerSettings.from_env({
                "DATABASE_URL": psycopg2.extensions.make_dsn(URL, dbname=self.database)
            })
            with patch.object(TaggerSettings, "from_env", return_value=settings):
                tagger_worker.main(["--bootstrap-schema"])
            with self.conn.cursor() as cur:
                repository.validate_schema(cur)
        finally:
            self.grant_runtime_access()

    def test_normal_worker_startup_uses_only_runtime_permissions(self):
        settings = TaggerSettings.from_env({
            "DATABASE_URL": self.runtime_dsn, "TYPESAFE_API_KEY": "fixture-key"
        })
        with (
            patch.object(TaggerSettings, "from_env", return_value=settings),
            patch.object(tagger_worker, "_running", False),
            patch.object(tagger_worker.signal, "signal"),
        ):
            tagger_worker.main([])

    def test_reconnect_replaces_and_closes_its_database_session(self):
        settings = TaggerSettings.from_env({
            "DATABASE_URL": self.runtime_dsn, "TYPESAFE_API_KEY": "fixture-key",
            "DATABASE_APPLICATION_NAME": self.role,
        })
        attempts = []

        def poll(cur, *_args):
            cur.execute("SELECT pg_backend_pid()")
            attempts.append(cur.fetchone()[0])
            if len(attempts) == 1:
                raise RuntimeError("fixture poll interruption")
            tagger_worker._running = False
            return []

        with (
            patch.object(TaggerSettings, "from_env", return_value=settings),
            patch.object(tagger_worker, "_running", True),
            patch.object(tagger_worker.signal, "signal"),
            patch.object(tagger_worker.time, "sleep"),
            patch.object(repository, "candidates", side_effect=poll),
        ):
            tagger_worker.main([])
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(attempts[0], attempts[1])
        for _ in range(100):
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name=%s",
                    (self.role,),
                )
                if cur.fetchone()[0] == 0:
                    break
            time.sleep(0.01)
        else:
            self.fail("worker left an owned PostgreSQL session open after reconnect/shutdown")
