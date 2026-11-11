-- 065: unattributed region-scoped packet clusters.
--
-- Some scoped traffic can never be attributed to a public hashtag region name.
-- Private regions ($name) derive their transport key from stored key material,
-- so the candidate-name test that resolves public regions cannot match them by
-- design; a public region with a non-geographic name is equally unguessable.
-- A 2026-10 brute-force pass (74.8k UK place names nationally, plus per-station
-- dictionaries of every place name within 60 km and mesh-style variants) found
-- no candidate above the chance floor, which confirms these are not simply
-- names missing from the probe list.
--
-- Rather than invent a name, the regions:rebuild tool groups unattributed
-- packets into geographic clusters (observers plus resolved relay hops) so the
-- dashboard can show *where* unattributed scoped traffic is heard. Refreshed
-- wholesale by src/tools/rebuildRegionParticipation.ts inside the same
-- transaction as the other region tables.

CREATE TABLE IF NOT EXISTS region_scope_unattributed (
  cluster_id  TEXT PRIMARY KEY CHECK (char_length(cluster_id) BETWEEN 1 AND 16),
  lat         DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
  lon         DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
  packets     INTEGER NOT NULL DEFAULT 0 CHECK (packets >= 0),
  observers   INTEGER NOT NULL DEFAULT 0 CHECK (observers >= 0),
  first_seen  TIMESTAMPTZ,
  last_seen   TIMESTAMPTZ,
  top_nodes   JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS region_scope_unattributed_packets_idx
  ON region_scope_unattributed (packets DESC);
