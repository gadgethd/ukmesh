-- Packet-sharing rules require an explicit owner selection.
-- Existing rows are intentionally preserved so active test selections keep working.

ALTER TABLE owner_packet_share_rules
  ALTER COLUMN enabled SET DEFAULT FALSE;
