-- 131_packet_identity_backfill_cursor.sql
-- P2-5: make the resumable identity backfill cursor unique and precision-safe.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE ops.packet_identity_backfill_state
  ADD COLUMN IF NOT EXISTS last_observation_id UUID;

COMMENT ON COLUMN ops.packet_identity_backfill_state.last_observation_id IS
  'Unique observation cursor paired with last_time; replaces the non-unique (time, hash, rx_node_id) cursor.';
