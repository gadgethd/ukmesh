-- 118_move_mesh_ingest_observation.sql
-- P1-4 mesh domain: ingestion observation ledger, archive, tombstones and coverage.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.ingest_observation_ledger SET SCHEMA mesh;
ALTER TABLE public.ingest_observation_archive SET SCHEMA mesh;
ALTER TABLE public.ingest_observation_tombstones SET SCHEMA mesh;
ALTER TABLE public.ingest_observation_ledger_coverage SET SCHEMA mesh;
ALTER VIEW public.ingest_observation_history SET SCHEMA mesh;
CREATE VIEW public.ingest_observation_history AS SELECT * FROM mesh.ingest_observation_history;

CREATE OR REPLACE PROCEDURE public.compact_ingest_observation_ledger(IN job_id integer, IN config jsonb)
 LANGUAGE plpgsql
AS $procedure$
DECLARE cutoff TIMESTAMPTZ := NOW() - INTERVAL '60 days';
BEGIN
  -- One bounded transaction per job. Never remove UUID dedupe guards, sequence
  -- history, aggregate inputs, packet_paths, or raw packet retention evidence.
  PERFORM pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'));
  WITH candidates AS MATERIALIZED (
    SELECT * FROM mesh.ingest_observation_ledger ORDER BY seq LIMIT 5000 FOR UPDATE
  ), cold AS MATERIALIZED (
    SELECT * FROM candidates WHERE created_at < cutoff
  ), archived AS (
    INSERT INTO mesh.ingest_observation_archive
      (seq, observation_id, observed_at, packet_hash, network, created_at)
    SELECT seq, observation_id, observed_at, packet_hash, network, created_at FROM cold
    RETURNING observation_id
  ), guarded AS (
    INSERT INTO mesh.ingest_observation_tombstones (observation_id)
    SELECT observation_id FROM archived
    ON CONFLICT (observation_id) DO UPDATE SET observation_id = EXCLUDED.observation_id
    RETURNING observation_id
  )
  DELETE FROM mesh.ingest_observation_ledger ledger USING guarded
  WHERE ledger.observation_id = guarded.observation_id;
END;
$procedure$;

CREATE OR REPLACE FUNCTION public.guard_archived_observation_identity()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- A unique UUID guard also arbitrates Repeatable Read writers whose snapshot
  -- predates compaction. A stale transaction gets a conflict/serialization
  -- failure rather than reintroducing an archived identity.
  INSERT INTO mesh.ingest_observation_tombstones(observation_id)
  VALUES (NEW.observation_id) ON CONFLICT DO NOTHING;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN NEW;
END;
$function$;
