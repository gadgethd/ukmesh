-- 119_compat_mesh_ingest_ledger.sql
-- Read-only compatibility views for external consumers still using pre-move names.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30s';

CREATE VIEW public.ingest_observation_ledger AS SELECT * FROM mesh.ingest_observation_ledger;
CREATE VIEW public.ingest_observation_archive AS SELECT * FROM mesh.ingest_observation_archive;
CREATE VIEW public.ingest_observation_tombstones AS SELECT * FROM mesh.ingest_observation_tombstones;
