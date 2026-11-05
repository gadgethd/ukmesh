-- 135_packet_identity_backfill_chunks.sql
-- P2-5 follow-up: record per-chunk work and an honest completion claim.
--
-- The backfill now visits compressed chunks too, so the state row has to say
-- which chunks were seen, which of them were decompressed by the run, and how
-- many compressed chunks were left unvisited. `packets_partial` exists so a
-- run that stopped early (--limit, error, unvisited chunk) can never be
-- mistaken for a completed phase.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE ops.packet_identity_backfill_state
  ADD COLUMN IF NOT EXISTS chunk_results JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS skipped_compressed INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS status_reason TEXT,
  ADD COLUMN IF NOT EXISTS decompressed_chunks TEXT[] NOT NULL DEFAULT '{}'::text[];

-- The original CHECK was created inline in migration 130, so its generated
-- name is not part of the contract; find it by definition instead of guessing.
DO $$
DECLARE
  constraint_name text;
BEGIN
  FOR constraint_name IN
    SELECT conname
      FROM pg_constraint
     WHERE conrelid = 'ops.packet_identity_backfill_state'::regclass
       AND contype = 'c'
       AND pg_get_constraintdef(oid) LIKE '%packets_complete%'
  LOOP
    EXECUTE format('ALTER TABLE ops.packet_identity_backfill_state DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

ALTER TABLE ops.packet_identity_backfill_state
  DROP CONSTRAINT IF EXISTS packet_identity_backfill_state_status_check;
ALTER TABLE ops.packet_identity_backfill_state
  ADD CONSTRAINT packet_identity_backfill_state_status_check
  CHECK (status IN ('idle', 'running', 'packets_complete', 'packets_partial', 'complete', 'failed'));

COMMENT ON COLUMN ops.packet_identity_backfill_state.chunk_results IS
  'P2-5: per-chunk result objects (schema, name, compressed_at_start, rows_selected, resolved, unresolved, mismatch, complete).';
COMMENT ON COLUMN ops.packet_identity_backfill_state.skipped_compressed IS
  'P2-5: compressed-at-start chunks that still held unvisited rows when the run stopped; zero is required for packets_complete.';
COMMENT ON COLUMN ops.packet_identity_backfill_state.status_reason IS
  'P2-5: why a run ended in packets_partial or failed.';
COMMENT ON COLUMN ops.packet_identity_backfill_state.decompressed_chunks IS
  'P2-5: chunks this backfill decompressed and must recompress; persisted before reading so a crash cannot orphan a decompression.';
