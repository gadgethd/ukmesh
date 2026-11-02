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
