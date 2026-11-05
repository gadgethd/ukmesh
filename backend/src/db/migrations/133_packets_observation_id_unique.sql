-- 133_packets_observation_id_unique.sql
-- P2-5 follow-up: enforce the identity backfill's (time, observation_id) page
-- key on the table itself. Ingest deduplicates observations through
-- mesh.ingest_observation_ledger, but that is an application invariant until
-- the database enforces it where the backfill reads.
--
-- TimescaleDB requires every unique index on a hypertable to include the
-- partitioning column, so (time, observation_id) is the strongest key that
-- can be enforced here; NULL observation ids stay excluded by the backfill.
--
-- This fails closed if historical rows already share a key. Check first with:
--   SELECT time, observation_id, count(*)
--     FROM mesh.packets
--    WHERE observation_id IS NOT NULL
--    GROUP BY time, observation_id
--   HAVING count(*) > 1;
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '600s';

CREATE UNIQUE INDEX IF NOT EXISTS packets_time_observation_id_uidx
  ON mesh.packets (time, observation_id);

COMMENT ON INDEX mesh.packets_time_observation_id_uidx IS
  'P2-5: unique pagination key for the resumable packet identity backfill.';
