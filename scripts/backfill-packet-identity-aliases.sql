-- P2-5 idempotent alias census.
-- Never resolves short/empty legacy values; canonical 64-hex aliases resolve only
-- when exactly one UID is present for the hash.
WITH raw AS (
  SELECT network, 'packets'::text AS source_namespace, packet_hash, packet_uid
    FROM mesh.packets
   WHERE length(packet_hash) IN (0, 8, 16, 36, 64)
  UNION ALL
  SELECT network, 'packet_paths'::text, packet_hash, packet_uid
    FROM mesh.packet_paths
   WHERE length(packet_hash) IN (0, 8, 16, 36, 64)
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
INSERT INTO mesh.packet_identity_aliases (
  network, source_namespace, alias_kind, alias_encoding, alias_value,
  original_byte_length, packet_uid, identity_version, provenance, resolution_status
)
SELECT
  network,
  source_namespace,
  'packet_hash',
  CASE char_len WHEN 0 THEN 'empty' WHEN 36 THEN 'text' ELSE 'hex' END,
  packet_hash,
  CASE char_len WHEN 0 THEN 0 WHEN 8 THEN 4 WHEN 16 THEN 8 WHEN 36 THEN 16 WHEN 64 THEN 32 END,
  CASE WHEN char_len = 64 AND uid_count = 1 THEN uid ELSE NULL END,
  CASE WHEN char_len = 64 AND uid_count = 1 THEN 'wire-sha256-v1' ELSE NULL END,
  CASE char_len
    WHEN 0 THEN 'decoder-error-empty'
    WHEN 8 THEN 'legacy-message-hash-32bit'
    WHEN 16 THEN 'upstream-display-hash'
    WHEN 36 THEN 'uuid-fallback'
    ELSE 'canonical-sha256'
  END,
  CASE
    WHEN char_len IN (0, 8, 16, 36) THEN 'unresolved'
    WHEN uid_count = 1 THEN 'resolved'
    WHEN uid_count > 1 THEN 'ambiguous'
    ELSE 'unresolved'
  END
FROM grouped
ON CONFLICT (network, source_namespace, alias_kind, alias_encoding, alias_value, packet_uid)
DO UPDATE SET
  original_byte_length = EXCLUDED.original_byte_length,
  identity_version = EXCLUDED.identity_version,
  provenance = EXCLUDED.provenance,
  resolution_status = EXCLUDED.resolution_status,
  updated_at = NOW();
