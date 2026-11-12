-- 120_compat_mesh_ingest_coverage.sql
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30s';

CREATE VIEW public.ingest_observation_ledger_coverage AS SELECT * FROM mesh.ingest_observation_ledger_coverage;
