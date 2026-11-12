-- P2-5 idempotent alias census.
-- Never resolves short/empty legacy values; canonical 64-hex aliases resolve only
-- when exactly one UID is present for the hash. The snapshot replaces any prior
-- disposition for the same source alias, so an unresolved -> resolved or
-- resolved -> ambiguous transition cannot leave a contradictory stale row.
BEGIN;
SET LOCAL lock_timeout = '30s';
SET LOCAL statement_timeout = '900s';

CREATE TEMP TABLE p2_alias_census ON COMMIT DROP AS
WITH raw AS (
  SELECT network, 'packets'::text AS source_namespace, packet_hash, packet_uid
    FROM mesh.packets
   WHERE packet_hash IS NOT NULL
  UNION ALL
  SELECT network, 'packet_paths'::text, packet_hash, packet_uid
    FROM mesh.packet_paths
   WHERE packet_hash IS NOT NULL
), grouped AS (
  SELECT network,
         source_namespace,
         packet_hash,
         length(packet_hash) AS char_len,
         (array_agg(DISTINCT packet_uid) FILTER (WHERE packet_uid IS NOT NULL))[1] AS uid,
         count(DISTINCT packet_uid) FILTER (WHERE packet_uid IS NOT NULL) AS uid_count
    FROM raw
   GROUP BY network, source_namespace, packet_hash, length(packet_hash)
)
SELECT
  network,
  source_namespace,
  'packet_hash'::text AS alias_kind,
  CASE char_len WHEN 0 THEN 'empty' WHEN 36 THEN 'text'
                WHEN 8 THEN 'hex' WHEN 16 THEN 'hex' WHEN 64 THEN 'hex' ELSE 'text' END AS alias_encoding,
  packet_hash AS alias_value,
  CASE char_len WHEN 0 THEN 0 WHEN 8 THEN 4 WHEN 16 THEN 8 WHEN 36 THEN 16 WHEN 64 THEN 32 END
    AS original_byte_length,
  CASE WHEN char_len = 64 AND uid_count = 1 THEN uid ELSE NULL END AS packet_uid,
  CASE WHEN char_len = 64 AND uid_count = 1 THEN 'wire-sha256-v1' ELSE NULL END AS identity_version,
  CASE char_len
    WHEN 0 THEN 'decoder-error-empty'
    WHEN 8 THEN 'legacy-message-hash-32bit'
    WHEN 16 THEN 'upstream-display-hash'
    WHEN 36 THEN 'uuid-fallback'
    WHEN 64 THEN 'canonical-sha256'
    ELSE 'unknown-legacy-hash'
  END AS provenance,
  CASE
    WHEN char_len = 0 THEN 'invalid'
    WHEN char_len IN (8, 16, 36) THEN 'unresolved'
    WHEN char_len = 64 AND uid_count = 1 THEN 'resolved'
    WHEN char_len = 64 AND uid_count > 1 THEN 'ambiguous'
    ELSE 'unresolved'
  END AS resolution_status
FROM grouped;

DELETE FROM mesh.packet_identity_aliases a
USING p2_alias_census c
WHERE a.network = c.network
  AND a.source_namespace = c.source_namespace
  AND a.alias_kind = c.alias_kind
  AND a.alias_value = c.alias_value;

INSERT INTO mesh.packet_identity_aliases (
  network, source_namespace, alias_kind, alias_encoding, alias_value,
  original_byte_length, packet_uid, identity_version, provenance, resolution_status
)
SELECT network, source_namespace, alias_kind, alias_encoding, alias_value,
       original_byte_length, packet_uid, identity_version, provenance, resolution_status
  FROM p2_alias_census
ON CONFLICT (network, source_namespace, alias_kind, alias_encoding, alias_value, packet_uid)
DO UPDATE SET
  original_byte_length = EXCLUDED.original_byte_length,
  identity_version = EXCLUDED.identity_version,
  provenance = EXCLUDED.provenance,
  resolution_status = EXCLUDED.resolution_status,
  updated_at = NOW();

COMMIT;
