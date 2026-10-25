-- Preserve the MQTT status envelope alongside extracted node telemetry.
ALTER TABLE node_status_samples
  ADD COLUMN IF NOT EXISTS status TEXT;
