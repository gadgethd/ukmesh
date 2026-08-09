SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Public snapshot invalidation is a privacy fence, not a node-inventory
-- revision counter. New nodes and ordinary coordinate/role updates arrive
-- continuously and must not make every long-running chart refresh stale.
-- Alias and prefix tables retain their statement triggers from migration 039.
CREATE OR REPLACE FUNCTION bump_public_visibility_generation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF old_private IS DISTINCT FROM new_private THEN
    UPDATE public_visibility_state
       SET generation = generation + 1,
           updated_at = NOW()
     WHERE singleton = TRUE;
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;
