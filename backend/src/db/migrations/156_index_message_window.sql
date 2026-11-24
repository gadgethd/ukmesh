-- Spam analysis and its logical-message count scan a 24-hour window dominated
-- by non-message packets. On the retained 10x fixture the soak plan fetched
-- ~293k heap rows across two chunks (33,586 shared read blocks, more than the
-- 128MB buffer pool) to keep 326 type-5 rows, breaching the 5s analysis budget
-- under ingest load and aborting the connection. A partial type-5 index lets
-- both statements fetch only message rows. The predicate stays at packet_type
-- so the count query, which has no visibility fence, shares the access path;
-- payload and privacy fences remain in the queries unchanged.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX IF NOT EXISTS idx_packets_message_time
  ON mesh.packets (network, time DESC)
  WHERE packet_type = 5;
