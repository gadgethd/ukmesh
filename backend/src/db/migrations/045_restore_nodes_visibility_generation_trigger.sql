SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- On fresh databases, migration 015 (which created the nodes generation
-- trigger) is superseded by 044 and never runs, so the trigger is missing.
-- Without it, a node-privacy transition bumps the public generation through
-- bump_visibility_for_identity_table() (which deliberately leaves the
-- materialization generation stale — fail closed) and nothing re-fences it,
-- so every public packet read returns empty forever.
--
-- Recreate the trigger idempotently. It executes the current
-- bump_public_visibility_generation() (042's version bumps BOTH the public
-- generation and the materialization fence atomically), which is exactly the
-- fresh-database contract 042's fence assumed. Existing databases that ran
-- 015 already have this trigger and are unaffected.
DROP TRIGGER IF EXISTS nodes_public_visibility_generation ON nodes;
CREATE TRIGGER nodes_public_visibility_generation
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION bump_public_visibility_generation();
