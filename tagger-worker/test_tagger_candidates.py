"""Exercise the actual poll SQL without contacting the classification service."""

from datetime import datetime, timedelta, timezone
import os
from unittest.mock import patch
import unittest
from urllib.parse import urlsplit
from uuid import uuid4
import psycopg2
from psycopg2 import sql

from tagger_config import TaggerSettings
import tagger_repository as repository
import tagger_service

URL = os.environ.get("TEST_INGEST_DATABASE_URL")


@unittest.skipUnless(URL, "TEST_INGEST_DATABASE_URL is required")
class CandidatePagingTests(unittest.TestCase):
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
            # The worker uses public compatibility names for packets/tags on
            # its current default search_path; privacy/retry state is read from
            # the post-move `ops` schema by fully qualified name.
            cur.execute("""CREATE SCHEMA IF NOT EXISTS ops;
              CREATE TABLE packets (packet_hash text, time timestamptz, payload jsonb,
              packet_type int, network text, visibility_ok boolean, is_private boolean, rx_node_id text);
              CREATE TABLE message_tags (packet_hash text PRIMARY KEY, packet_time timestamptz, tagged_at timestamptz,
                model text, latency_ms int, tags jsonb, confidence jsonb);
              CREATE TABLE ops.tagger_retry (packet_hash text PRIMARY KEY, exhausted boolean);
              CREATE TABLE ops.privacy_rematerialization_queue (status text);
              CREATE TABLE ops.public_visibility_state (singleton boolean, generation int);
              CREATE TABLE ops.packet_visibility_materialization_state (singleton boolean, visibility_generation int);
              INSERT INTO ops.public_visibility_state VALUES (true, 1);
              INSERT INTO ops.packet_visibility_materialization_state VALUES (true, 1);""")

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

    def setUp(self):
        self.cursor_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        with self.conn.cursor() as cur:
            cur.execute(
                "TRUNCATE packets, message_tags, ops.tagger_retry, ops.privacy_rematerialization_queue; UPDATE ops.public_visibility_state SET generation=1; UPDATE ops.packet_visibility_materialization_state SET visibility_generation=1"
            )

    def add(self, packet_hash, time, receiver=None):
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO packets VALUES (%s,%s,
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
                    "INSERT INTO message_tags (packet_hash) VALUES (%s)", (key,)
                )
        self.add("new-message", self.cursor_time + timedelta(seconds=1))
        self.assertEqual(self.candidates(), ["new-message"])

    def test_retry_queue_owns_both_pending_and_exhausted_retries(self):
        for key, exhausted in [("retrying", False), ("exhausted", True)]:
            self.add(key, self.cursor_time + timedelta(seconds=1))
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ops.tagger_retry VALUES (%s,%s)", (key, exhausted)
                )
        self.assertEqual(self.candidates(), [])

    def classify(self, _state):
        raise AssertionError("unexpected external classifier call")

    def settings(self):
        return TaggerSettings.from_env({"DATABASE_URL": URL})

    def test_private_retry_never_reaches_classifier(self):
        self.add("private-retry", self.cursor_time)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE packets SET is_private=true, visibility_ok=false")
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
                    "SELECT tags FROM message_tags WHERE packet_hash='public-retry'"
                )
                self.assertEqual(cur.fetchone()[0], {"kind": "chat"})

    def test_privacy_revocation_during_backoff_prevents_another_external_attempt(self):
        self.add("revoked-during-retry", self.cursor_time)

        def revoke_and_fail(_state):
            with self.conn.cursor() as update:
                update.execute(
                    "UPDATE packets SET is_private=true, visibility_ok=false"
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
                "UPDATE packets SET is_private=true, visibility_ok=false WHERE rx_node_id='private-receiver'"
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
            cur.execute("UPDATE packets SET is_private=true, visibility_ok=false")
            self.assertIsNone(
                repository.public_packet_keys(
                    cur, "queued-retry", self.cursor_time, self.settings().networks
                )
            )
