-- 127_packet_identity.sql
-- P2-4: nullable canonical content identity, provenance, and many-to-many aliases.
-- No backfill and no UNIQUE(packet_uid): equal wire content legitimately has many observations.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '300s';

ALTER TABLE mesh.packets
  ADD COLUMN IF NOT EXISTS packet_uid BYTEA,
  ADD COLUMN IF NOT EXISTS identity_version TEXT,
  ADD COLUMN IF NOT EXISTS identity_provenance TEXT;

ALTER TABLE mesh.packet_paths
  ADD COLUMN IF NOT EXISTS packet_uid BYTEA;

CREATE INDEX IF NOT EXISTS packets_packet_uid_idx
  ON mesh.packets(packet_uid)
  WHERE packet_uid IS NOT NULL;

CREATE INDEX IF NOT EXISTS packet_paths_packet_uid_idx
  ON mesh.packet_paths(packet_uid)
  WHERE packet_uid IS NOT NULL;

CREATE TABLE IF NOT EXISTS mesh.packet_identity_aliases (
  alias_id BIGSERIAL PRIMARY KEY,
  network TEXT NOT NULL,
  source_namespace TEXT NOT NULL,
  alias_kind TEXT NOT NULL,
  alias_encoding TEXT NOT NULL,
  alias_value TEXT NOT NULL,
  original_byte_length INTEGER,
  packet_uid BYTEA,
  identity_version TEXT,
  provenance TEXT NOT NULL,
  resolution_status TEXT NOT NULL CHECK (resolution_status IN ('resolved', 'unresolved', 'ambiguous', 'invalid')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE NULLS NOT DISTINCT (
    network, source_namespace, alias_kind, alias_encoding, alias_value, packet_uid
  )
);

CREATE INDEX IF NOT EXISTS packet_identity_aliases_lookup_idx
  ON mesh.packet_identity_aliases(network, source_namespace, alias_kind, alias_value);

CREATE INDEX IF NOT EXISTS packet_identity_aliases_uid_idx
  ON mesh.packet_identity_aliases(packet_uid)
  WHERE packet_uid IS NOT NULL;

COMMENT ON COLUMN mesh.packets.packet_uid IS
  'P2-4 canonical wire-content SHA-256 (32 bytes); nullable until the row is eligible.';
COMMENT ON COLUMN mesh.packets.identity_version IS
  'Algorithm/version label for packet_uid, e.g. wire-sha256-v1.';
COMMENT ON COLUMN mesh.packets.identity_provenance IS
  'Why packet_uid is present or absent; never infer a UID from a legacy short hash.';
COMMENT ON TABLE mesh.packet_identity_aliases IS
  'Many-to-many legacy/source aliases. Short/empty values are never expanded or resolved without retained evidence.';
