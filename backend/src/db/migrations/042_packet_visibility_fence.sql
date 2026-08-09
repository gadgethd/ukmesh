SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS packet_visibility_materialization_state (
  singleton             BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  visibility_generation BIGINT NOT NULL,
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Serialize a real node-privacy transition with packet classification. Packet
-- inserts take a compatible KEY SHARE lock while pinning the generation; this
-- trigger takes UPDATE before the existing AFTER trigger rewrites affected
-- packets. Whichever transaction starts second therefore observes the first.
CREATE OR REPLACE FUNCTION lock_packet_visibility_for_node_privacy_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  old_private BOOLEAN := TG_OP <> 'INSERT' AND COALESCE(OLD.name, '') LIKE '%🚫%';
  new_private BOOLEAN := TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%';
  privacy_scope_changed BOOLEAN;
BEGIN
  privacy_scope_changed := old_private IS DISTINCT FROM new_private
    OR ((old_private OR new_private)
      AND TG_OP = 'UPDATE'
      AND OLD.network IS DISTINCT FROM NEW.network);
  IF privacy_scope_changed THEN
    PERFORM generation
      FROM public_visibility_state
     WHERE singleton = TRUE
     FOR UPDATE;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_packet_visibility_serialization ON nodes;
CREATE TRIGGER nodes_packet_visibility_serialization
BEFORE INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION lock_packet_visibility_for_node_privacy_change();

-- Migration 016 synchronously classified historical rows and has maintained
-- affected rows in the nodes privacy trigger ever since. Fence that complete
-- materialization at the generation observed when this migration commits.
INSERT INTO packet_visibility_materialization_state
  (singleton, visibility_generation, updated_at)
SELECT TRUE, generation, NOW()
FROM public_visibility_state
WHERE singleton = TRUE
ON CONFLICT (singleton) DO UPDATE SET
  visibility_generation = EXCLUDED.visibility_generation,
  updated_at = EXCLUDED.updated_at;

CREATE OR REPLACE FUNCTION bump_public_visibility_generation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
  next_generation BIGINT;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF old_private IS DISTINCT FROM new_private THEN
    -- nodes_private_prefix_materialization sorts before this trigger and has
    -- already updated every affected packet in the same transaction.
    UPDATE public_visibility_state
       SET generation = generation + 1,
           updated_at = NOW()
     WHERE singleton = TRUE
     RETURNING generation INTO next_generation;

    INSERT INTO packet_visibility_materialization_state
      (singleton, visibility_generation, updated_at)
    VALUES (TRUE, next_generation, NOW())
    ON CONFLICT (singleton) DO UPDATE SET
      visibility_generation = EXCLUDED.visibility_generation,
      updated_at = EXCLUDED.updated_at;
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION bump_visibility_for_identity_table()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  next_generation BIGINT;
BEGIN
  -- Nested prefix maintenance is completed and fenced by the outer nodes
  -- trigger. A direct prefix-table mutation has no packet rewrite, so advance
  -- only the public generation and deliberately leave the materialization
  -- generation stale (all materialized reads then fail closed).
  IF TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;

  UPDATE public_visibility_state
     SET generation = generation + 1,
         updated_at = NOW()
   WHERE singleton = TRUE
   RETURNING generation INTO next_generation;

  -- Alias changes affect canonical joins, not the packet privacy bits. The
  -- bits remain valid at the new generation and can be fenced immediately.
  IF TG_TABLE_NAME = 'node_identity_aliases' THEN
    INSERT INTO packet_visibility_materialization_state
      (singleton, visibility_generation, updated_at)
    VALUES (TRUE, next_generation, NOW())
    ON CONFLICT (singleton) DO UPDATE SET
      visibility_generation = EXCLUDED.visibility_generation,
      updated_at = EXCLUDED.updated_at;
  END IF;
  RETURN NULL;
END;
$$;
