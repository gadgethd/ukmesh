-- Complete the ledger for history predating 058. The runner wraps this file
-- and its schema_migrations record in one transaction. Take the writer lock
-- in its own statement so the following commands see commits made while we
-- waited (Read Committed); sequence allocation cannot race a packet batch.
SELECT pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'));

CREATE TABLE IF NOT EXISTS ingest_observation_ledger_coverage (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  version INTEGER NOT NULL CHECK (version = 1),
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pre-039 compressed history can have no UUID. Assign one per observation
-- once, atomically with its ledger entry. This is a one-time maintenance
-- migration, not a hot-path update; never change packet/path/stat totals.
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
UPDATE packets SET observation_id = gen_random_uuid()
WHERE observation_id IS NULL;

INSERT INTO ingest_observation_ledger
  (observation_id, observed_at, packet_hash, network)
SELECT DISTINCT ON (observation_id)
       observation_id, time, packet_hash, network
FROM packets
ORDER BY observation_id, time, packet_hash, network
ON CONFLICT (observation_id) DO NOTHING;

-- A public completeness reader may only enable the ledger after this fence
-- commits. An interrupted backfill exposes neither partial identities nor
-- a false coverage claim. Backfilled seq values follow all prior commits;
-- existing seq cursors discover them too, irrespective of observed_at.
INSERT INTO ingest_observation_ledger_coverage (singleton, version)
VALUES (TRUE, 1)
ON CONFLICT (singleton) DO NOTHING;
