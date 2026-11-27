-- 137_owner_role_grants.sql
-- Grant the owner portal role the table privileges it needs on core.owner_*.
--
-- The owner portal connects as meshcore_owner (OWNER_DATABASE_URL) and its
-- queries live in backend/src/db/ownerAuth.ts. Migration 072 grants only schema
-- USAGE and defers table grants to the migration that creates each table, but
-- 073 (owner_accounts/owner_account_nodes) and 075 (owner_sessions) never added
-- them: the production grants were applied out of band. That drift left
-- core.owner_sessions with no grant at all, so every owner login failed with
-- "permission denied for table owner_sessions" and the UI reported "Owner
-- session store is temporarily unavailable". Restore drills rebuild from
-- migrations, so the grants must be repeatable rather than manual.
--
-- Idempotent: GRANT is a no-op when the privilege already exists, and each
-- table is skipped when the migration hasn't created it yet.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
  target text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'meshcore_owner') THEN
    RAISE EXCEPTION 'bootstrap role meshcore_owner is missing';
  END IF;

  FOR target IN
    SELECT unnest(ARRAY[
      'core.owner_accounts',
      'core.owner_account_nodes',
      'core.owner_sessions'
    ])
  LOOP
    IF to_regclass(target) IS NOT NULL THEN
      EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO meshcore_owner', target);
    END IF;
  END LOOP;
END
$$;
