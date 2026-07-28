ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS last_mqtt_observer_seen_at TIMESTAMPTZ;

-- The observer rollup is small and contains the best currently available
-- bootstrap signal. Older stale rows were cleaned manually before rollout.
UPDATE nodes n
   SET last_mqtt_observer_seen_at = latest.last_seen
  FROM (
    SELECT rx_node_id, MAX(last_seen) AS last_seen
      FROM observer_region_observer_sightings
     GROUP BY rx_node_id
  ) latest
 WHERE n.node_id = latest.rx_node_id
   AND (
     n.last_mqtt_observer_seen_at IS NULL
     OR n.last_mqtt_observer_seen_at < latest.last_seen
   );

CREATE INDEX IF NOT EXISTS nodes_mqtt_observer_stale_idx
  ON nodes (last_mqtt_observer_seen_at)
  WHERE role IS NULL OR role = 2;

CREATE TABLE IF NOT EXISTS maintenance_removed_records (
  archive_id   BIGSERIAL PRIMARY KEY,
  batch_id     TEXT NOT NULL,
  source_table TEXT NOT NULL,
  record_data  JSONB NOT NULL,
  reason       TEXT NOT NULL,
  archived_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS maintenance_removed_records_batch_idx
  ON maintenance_removed_records (batch_id, source_table);
