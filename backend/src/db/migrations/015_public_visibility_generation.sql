CREATE TABLE IF NOT EXISTS public_visibility_state (
  singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  generation   BIGINT NOT NULL DEFAULT 1,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public_visibility_state (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

ALTER TABLE path_history_cache
  ADD COLUMN IF NOT EXISTS visibility_generation BIGINT NOT NULL DEFAULT 0;

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

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_public_visibility_generation ON nodes;
CREATE TRIGGER nodes_public_visibility_generation
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION bump_public_visibility_generation();

-- Every pre-migration snapshot was computed without a generation binding.
DELETE FROM path_history_cache;
