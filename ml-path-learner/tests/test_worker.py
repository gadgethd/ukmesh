import os
import pathlib
import sys
import time
import unittest
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import worker  # noqa: E402


class CursorTests(unittest.TestCase):
    def test_more_than_one_batch_at_same_timestamp_is_total_and_stable(self):
        observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            {
                'observed_at': observed_at,
                'packet_hash': f'packet-{index:05d}',
                'network': 'ukmesh',
                'rx_node_id': 'receiver',
                'topic': 'mesh/packet',
                'raw_hex': f'{index:08x}',
            }
            for index in range(worker.GOLD_BATCH + 137)
        ]
        cursors = sorted(worker.cursor_from_row(row) for row in rows)

        checkpoint = worker.ExtractionCursor(
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            '',
            '',
            '',
            '',
            '',
        )
        processed = []
        while True:
            batch = [cursor for cursor in cursors if cursor > checkpoint][
                :worker.GOLD_BATCH
            ]
            if not batch:
                break
            processed.extend(batch)
            checkpoint = batch[-1]

        self.assertEqual(len(rows), len(processed))
        self.assertEqual(len(processed), len(set(processed)))
        self.assertEqual(cursors, processed)


@unittest.skipUnless(
    os.environ.get('TEST_DATABASE_URL'),
    'TEST_DATABASE_URL is required for PostgreSQL integration tests',
)
class PostgreSQLLearnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_url = os.environ['TEST_DATABASE_URL']

    def setUp(self):
        self.schema = f'ml_test_{uuid.uuid4().hex}'
        self.db = self._connect()
        with self.db.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{self.schema}"')
            cur.execute(f'SET search_path TO "{self.schema}"')
            cur.execute(
                """
                CREATE TABLE ml_learner_state (
                  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                  cursor_observed_at TIMESTAMPTZ NOT NULL
                    DEFAULT '1970-01-01 00:00:00+00',
                  cursor_packet_hash TEXT NOT NULL DEFAULT '',
                  cursor_network TEXT NOT NULL DEFAULT '',
                  cursor_rx_node_id TEXT NOT NULL DEFAULT '',
                  cursor_topic TEXT NOT NULL DEFAULT '',
                  cursor_raw_hex TEXT NOT NULL DEFAULT '',
                  leader_token TEXT,
                  lease_expires_at TIMESTAMPTZ,
                  heartbeat_at TIMESTAMPTZ,
                  run_started_at TIMESTAMPTZ,
                  run_deadline_at TIMESTAMPTZ,
                  next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  last_trained_at TIMESTAMPTZ,
                  next_training_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  model_version TEXT NOT NULL DEFAULT 'lightgbm-path-v1',
                  data_version TEXT NOT NULL DEFAULT 'gold-multibyte-v2',
                  last_terminal_reason TEXT,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                INSERT INTO ml_learner_state (singleton) VALUES (TRUE);

                CREATE TABLE nodes (
                  node_id TEXT NOT NULL,
                  network TEXT NOT NULL,
                  lat DOUBLE PRECISION,
                  lon DOUBLE PRECISION,
                  elevation_m DOUBLE PRECISION,
                  last_seen TIMESTAMPTZ,
                  iata TEXT
                );

                CREATE TABLE packets (
                  time TIMESTAMPTZ NOT NULL,
                  packet_hash TEXT NOT NULL,
                  network TEXT NOT NULL,
                  rx_node_id TEXT,
                  topic TEXT NOT NULL,
                  raw_hex TEXT,
                  path_hashes TEXT[],
                  path_hash_size_bytes INTEGER
                );

                CREATE TABLE ml_gold_paths (
                  id BIGSERIAL PRIMARY KEY,
                  packet_hash TEXT NOT NULL,
                  network TEXT NOT NULL,
                  observed_at TIMESTAMPTZ NOT NULL,
                  hop_position INTEGER NOT NULL,
                  true_node_id TEXT NOT NULL,
                  hash_2char TEXT NOT NULL,
                  hash_4char TEXT,
                  hash_6char TEXT,
                  path_hash_size_bytes INTEGER,
                  observer_ids TEXT[],
                  rx_region TEXT,
                  UNIQUE (packet_hash, hop_position, true_node_id)
                );
                """
            )
        self.db.commit()

    def tearDown(self):
        try:
            self.db.rollback()
        except Exception:
            pass
        self.db.close()
        admin = self._connect(search_path=False)
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        admin.commit()
        admin.close()

    def _connect(self, search_path=True):
        connection = psycopg2.connect(
            self.database_url,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        connection.autocommit = False
        if search_path and hasattr(self, 'schema'):
            with connection.cursor() as cur:
                cur.execute(f'SET search_path TO "{self.schema}"')
            connection.commit()
        return connection

    def _claim(self, db):
        original_url = worker.DATABASE_URL
        original_lease = worker.ML_LEASE_SECONDS
        worker.DATABASE_URL = self.database_url
        worker.ML_LEASE_SECONDS = 600
        try:
            return worker.claim_leadership(db)
        finally:
            worker.DATABASE_URL = original_url
            worker.ML_LEASE_SECONDS = original_lease

    def _expire_lease(self):
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE ml_learner_state
                   SET lease_expires_at = NOW() - INTERVAL '1 second',
                       next_run_at = NOW() - INTERVAL '1 second'
                """
            )
        self.db.commit()

    def test_single_leader_and_expired_leader_reclamation(self):
        first = self._claim(self.db)
        self.assertIsNotNone(first)

        contender_db = self._connect()
        try:
            self.assertIsNone(self._claim(contender_db))
            self._expire_lease()
            reclaimed = self._claim(contender_db)
            self.assertIsNotNone(reclaimed)
            self.assertNotEqual(first.token, reclaimed.token)
        finally:
            contender_db.close()

    def test_failed_batch_does_not_advance_checkpoint_or_skip_later_rows(self):
        observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO nodes
                  (node_id, network, lat, lon, elevation_m, last_seen, iata)
                VALUES
                  ('AAAA0001', 'ukmesh', 51.0, -1.0, 100, NOW(), 'GB-S'),
                  ('BBBB0001', 'ukmesh', 51.1, -1.1, 120, NOW(), 'GB-S')
                """
            )
            packet_rows = [
                (
                    observed_at,
                    f'p{index:05d}',
                    'ukmesh',
                    'RX000001',
                    'mesh/packet',
                    f'{index:08x}',
                    ['AAAA', 'BBBB'],
                    2,
                )
                for index in range(worker.GOLD_BATCH + 1)
            ]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO packets
                  (time, packet_hash, network, rx_node_id, topic, raw_hex,
                   path_hashes, path_hash_size_bytes)
                VALUES %s
                """,
                packet_rows,
                page_size=1000,
            )
            cur.execute(
                """
                CREATE INDEX packets_cursor_test_idx
                  ON packets (
                    time, packet_hash, network, COALESCE(rx_node_id, ''),
                    topic, COALESCE(raw_hex, '')
                  )
                """
            )
        self.db.commit()

        guard = self._claim(self.db)
        self.assertIsNotNone(guard)
        self.assertEqual(worker.GOLD_BATCH, worker.extract_gold_paths(self.db, guard))

        checkpoint_after_first = worker.get_checkpoint(self.db)
        self.db.rollback()
        self.assertEqual('p04999', checkpoint_after_first.packet_hash)

        with self.db.cursor() as cur:
            cur.execute(
                """
                CREATE FUNCTION reject_test_packet() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.packet_hash = 'p05000' THEN
                    RAISE EXCEPTION 'injected row failure';
                  END IF;
                  RETURN NEW;
                END
                $$;
                CREATE TRIGGER reject_test_packet_trigger
                  BEFORE INSERT ON ml_gold_paths
                  FOR EACH ROW EXECUTE FUNCTION reject_test_packet();
                """
            )
        self.db.commit()

        with self.assertRaisesRegex(Exception, 'injected row failure'):
            worker.extract_gold_paths(self.db, guard)
        self.db.rollback()
        checkpoint_after_failure = worker.get_checkpoint(self.db)
        self.db.rollback()
        self.assertEqual(checkpoint_after_first, checkpoint_after_failure)

        self._expire_lease()
        restarted_db = self._connect()
        try:
            restarted_guard = self._claim(restarted_db)
            self.assertIsNotNone(restarted_guard)
            with restarted_db.cursor() as cur:
                cur.execute('DROP TRIGGER reject_test_packet_trigger ON ml_gold_paths')
                cur.execute('DROP FUNCTION reject_test_packet()')
            restarted_db.commit()

            self.assertEqual(
                1,
                worker.extract_gold_paths(restarted_db, restarted_guard),
            )
            self.assertEqual(
                0,
                worker.extract_gold_paths(restarted_db, restarted_guard),
            )
            with restarted_db.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS total_rows,
                           COUNT(DISTINCT (packet_hash, hop_position, true_node_id))
                             AS distinct_rows
                      FROM ml_gold_paths
                    """
                )
                counts = cur.fetchone()
            restarted_db.rollback()
            self.assertEqual(2 * (worker.GOLD_BATCH + 1), counts['total_rows'])
            self.assertEqual(counts['total_rows'], counts['distinct_rows'])
        finally:
            restarted_db.close()

    def test_training_rows_are_bounded_in_sql_by_whole_packets(self):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO nodes
                  (node_id, network, lat, lon, elevation_m, last_seen, iata)
                VALUES
                  ('AA000001', 'ukmesh', 51.0, -1.0, 100, NOW(), 'GB-S'),
                  ('AA000002', 'ukmesh', 51.1, -1.1, 120, NOW(), 'GB-S')
                """
            )
            gold_rows = []
            for packet_index in range(100):
                for hop_position, node_id in enumerate(('AA000001', 'AA000002')):
                    gold_rows.append(
                        (
                            f'train-{packet_index:04d}',
                            'ukmesh',
                            datetime(2026, 1, 1, tzinfo=timezone.utc),
                            hop_position,
                            node_id,
                            'AA',
                            'AA00',
                            'AA0000',
                            2,
                            ['RX000001'],
                            'GB-S',
                        )
                    )
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO ml_gold_paths
                  (packet_hash, network, observed_at, hop_position, true_node_id,
                   hash_2char, hash_4char, hash_6char, path_hash_size_bytes,
                   observer_ids, rx_region)
                VALUES %s
                """,
                gold_rows,
                page_size=500,
            )
        self.db.commit()

        original_min = worker.MIN_GOLD_ROWS
        original_max = worker.MAX_TRAINING_GOLD_ROWS
        worker.MIN_GOLD_ROWS = 1
        worker.MAX_TRAINING_GOLD_ROWS = 20
        try:
            guard = worker.RunGuard('unit-test', time.monotonic() + 60)
            X, y, meta, gold_ids = worker.build_training_data(self.db, guard)
        finally:
            worker.MIN_GOLD_ROWS = original_min
            worker.MAX_TRAINING_GOLD_ROWS = original_max
            self.db.rollback()

        self.assertIsNotNone(X)
        self.assertEqual(20, len(set(gold_ids.tolist())))
        self.assertEqual(len(X), len(y))
        self.assertEqual(len(X), len(meta))


if __name__ == '__main__':
    unittest.main()
