-- 071_backend_schemas.sql
-- Phase P1-2: create the five target namespaces only.
-- This migration deliberately does not move tables, create roles, grant
-- privileges, or copy data. Ownership transfer is P1-12; grants are 072.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mesh;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS archive;
CREATE SCHEMA IF NOT EXISTS ops;

COMMENT ON SCHEMA core IS 'canonical identity, owners, ACL and node/observer metadata';
COMMENT ON SCHEMA mesh IS 'packets, observations, paths, channel messages and aliases';
COMMENT ON SCHEMA analytics IS 'read models for Beacon, CoreScope and Planner';
COMMENT ON SCHEMA archive IS 'anonymised per-hop aggregates and archive manifest';
COMMENT ON SCHEMA ops IS 'migration ledger bridge, privacy registries and operational state';

-- P1-2 privacy registries. These are policy tables, never content stores.
CREATE TABLE IF NOT EXISTS ops.channel_blocklist (
  channel_hash TEXT PRIMARY KEY,
  channel_name TEXT,
  hashtag TEXT,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  added_by TEXT NOT NULL
);
COMMENT ON TABLE ops.channel_blocklist IS
  'Channel publication policy keyed by full channel identity; no message content.';

CREATE TABLE IF NOT EXISTS ops.node_privacy (
  node_id TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  marker TEXT,
  expanded_from TEXT,
  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  added_by TEXT NOT NULL
);
COMMENT ON TABLE ops.node_privacy IS
  'Node identity privacy from private_node_prefixes and the 🚫 name marker; no packet content.';

-- Seed the node privacy registry from the two live sources that already
-- protect planner exports. This is identity metadata only; no packet content.
INSERT INTO ops.node_privacy (node_id, reason, source, marker, added_by)
SELECT node_id, 'private_node_prefixes registry', 'public.private_node_prefixes', NULL,
       '071_backend_schemas.sql'
FROM public.private_node_prefixes
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO ops.node_privacy (node_id, reason, source, marker, added_by)
SELECT DISTINCT node_id, '🚫 name marker', 'public.nodes.name', '🚫',
       '071_backend_schemas.sql'
FROM public.nodes
WHERE name LIKE '%🚫%'
ON CONFLICT (node_id) DO NOTHING;


