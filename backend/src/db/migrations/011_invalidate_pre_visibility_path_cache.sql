-- Existing path-history geometry was rendered before private-node visibility
-- became part of cache identity. It is derived data and will be rebuilt by the
-- path-history worker from privacy-filtered packets.
DELETE FROM path_history_cache;

-- Public incident JSON can contain origin estimates derived from private
-- observers. The analyzer recreates this derived table from filtered evidence.
TRUNCATE TABLE spam_message_incident_members, spam_message_incidents;
