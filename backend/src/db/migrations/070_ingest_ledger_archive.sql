-- Keep exact observation identities forever while moving cold metadata into
-- compressed chunks. The public history view retains every seq and field.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE IF NOT EXISTS ingest_observation_tombstones (
  observation_id UUID PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS ingest_observation_archive (
  seq BIGINT NOT NULL,
  observation_id UUID NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  packet_hash TEXT NOT NULL,
  network TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SELECT create_hypertable('ingest_observation_archive', 'archived_at',
  chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ingest_observation_archive_seq_idx
  ON ingest_observation_archive (network, seq);
ALTER TABLE ingest_observation_archive SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'network',
  timescaledb.compress_orderby = 'seq'
);
SELECT add_compression_policy('ingest_observation_archive', INTERVAL '14 days', if_not_exists => TRUE);
CREATE OR REPLACE VIEW ingest_observation_history AS
SELECT seq, observation_id, observed_at, packet_hash, network, created_at FROM ingest_observation_ledger
UNION ALL
SELECT seq, observation_id, observed_at, packet_hash, network, created_at FROM ingest_observation_archive;

CREATE OR REPLACE FUNCTION guard_archived_observation_identity()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  -- A unique UUID guard also arbitrates Repeatable Read writers whose snapshot
  -- predates compaction. A stale transaction gets a conflict/serialization
  -- failure rather than reintroducing an archived identity.
  INSERT INTO ingest_observation_tombstones(observation_id)
  VALUES (NEW.observation_id) ON CONFLICT DO NOTHING;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS ingest_observation_archive_guard ON ingest_observation_ledger;
CREATE TRIGGER ingest_observation_archive_guard BEFORE INSERT ON ingest_observation_ledger
FOR EACH ROW EXECUTE FUNCTION guard_archived_observation_identity();

CREATE OR REPLACE PROCEDURE compact_ingest_observation_ledger(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE cutoff TIMESTAMPTZ := NOW() - INTERVAL '60 days';
BEGIN
  -- One bounded transaction per job. Never remove UUID dedupe guards, sequence
  -- history, aggregate inputs, packet_paths, or raw packet retention evidence.
  PERFORM pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'));
  WITH candidates AS MATERIALIZED (
    SELECT * FROM ingest_observation_ledger ORDER BY seq LIMIT 5000 FOR UPDATE
  ), cold AS MATERIALIZED (
    SELECT * FROM candidates WHERE created_at < cutoff
  ), archived AS (
    INSERT INTO ingest_observation_archive
      (seq, observation_id, observed_at, packet_hash, network, created_at)
    SELECT seq, observation_id, observed_at, packet_hash, network, created_at FROM cold
    RETURNING observation_id
  ), guarded AS (
    INSERT INTO ingest_observation_tombstones (observation_id)
    SELECT observation_id FROM archived
    ON CONFLICT (observation_id) DO UPDATE SET observation_id = EXCLUDED.observation_id
    RETURNING observation_id
  )
  DELETE FROM ingest_observation_ledger ledger USING guarded
  WHERE ledger.observation_id = guarded.observation_id;
END;
$$;
-- At most 1.44 million cold rows/day; no scan/backfill during deployment.
SELECT add_job('compact_ingest_observation_ledger', INTERVAL '5 minutes', initial_start => NOW() + INTERVAL '5 minutes')
WHERE NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs
 WHERE proc_name = 'compact_ingest_observation_ledger');
