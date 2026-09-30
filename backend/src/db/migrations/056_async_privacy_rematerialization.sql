SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Node privacy transitions now enqueue stored-row rematerialization. The
-- worker advances public_visibility_state.generation and
-- packet_visibility_materialization_state.visibility_generation together only
-- after a pass completes. While work is pending, both remain equal at the
-- previous generation: public read gates remain open and node-name filters
-- reflect the new state. Neither generation may advance independently here.
-- The BEFORE nodes lock trigger and identity-table generation trigger retain
-- their existing behavior.
CREATE TABLE IF NOT EXISTS privacy_rematerialization_queue (
  id BIGSERIAL PRIMARY KEY,
  node_id TEXT NOT NULL,
  network TEXT,
  target TEXT NOT NULL DEFAULT 'packets' CHECK (target IN ('packets','packet_paths')),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','done','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  UNIQUE (target, node_id)
);
CREATE INDEX IF NOT EXISTS privacy_remat_queue_due_idx
  ON privacy_rematerialization_queue (status, requested_at);

CREATE OR REPLACE FUNCTION sync_private_node_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF TG_OP = 'UPDATE'
     AND OLD.name IS NOT DISTINCT FROM NEW.name
     AND OLD.network IS NOT DISTINCT FROM NEW.network THEN
    RETURN NEW;
  END IF;

  DELETE FROM private_node_prefixes WHERE node_id = changed_node_id;

  IF TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%' THEN
    INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
    VALUES
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 1, UPPER(LEFT(NEW.node_id, 2))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 2, UPPER(LEFT(NEW.node_id, 4))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 3, UPPER(LEFT(NEW.node_id, 6)))
    ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
      prefix = EXCLUDED.prefix,
      updated_at = NOW();
  END IF;

  IF old_private IS DISTINCT FROM new_private THEN
    INSERT INTO privacy_rematerialization_queue (node_id, network, target)
    VALUES (changed_node_id, changed_network, 'packets')
    ON CONFLICT (target, node_id) DO UPDATE SET
      requested_at = NOW(),
      network = EXCLUDED.network,
      status = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                    THEN 'processing' ELSE 'pending' END,
      attempts = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                      THEN privacy_rematerialization_queue.attempts ELSE 0 END,
      finished_at = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                         THEN privacy_rematerialization_queue.finished_at ELSE NULL END;
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_private_prefix_materialization ON nodes;
CREATE TRIGGER nodes_private_prefix_materialization
AFTER INSERT OR DELETE OR UPDATE ON nodes
FOR EACH ROW
EXECUTE FUNCTION sync_private_node_prefixes();

CREATE OR REPLACE FUNCTION rematerialize_packet_path_privacy()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := TG_OP <> 'INSERT' AND COALESCE(OLD.name, '') LIKE '%🚫%';
  new_private BOOLEAN := TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%';
BEGIN
  IF old_private IS DISTINCT FROM new_private THEN
    INSERT INTO privacy_rematerialization_queue (node_id, network, target)
    VALUES (changed_node_id, changed_network, 'packet_paths')
    ON CONFLICT (target, node_id) DO UPDATE SET
      requested_at = NOW(),
      network = EXCLUDED.network,
      status = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                    THEN 'processing' ELSE 'pending' END,
      attempts = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                      THEN privacy_rematerialization_queue.attempts ELSE 0 END,
      finished_at = CASE WHEN privacy_rematerialization_queue.status = 'processing'
                         THEN privacy_rematerialization_queue.finished_at ELSE NULL END;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_private_zz_packet_path_materialization ON nodes;
CREATE TRIGGER nodes_private_zz_packet_path_materialization
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION rematerialize_packet_path_privacy();

-- The worker owns the generation pair. Nested prefix maintenance is already
-- ignored by bump_visibility_for_identity_table(); keep that function and its
-- direct identity-mutation behavior unchanged.
CREATE OR REPLACE FUNCTION bump_public_visibility_generation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_public_visibility_generation ON nodes;
CREATE TRIGGER nodes_public_visibility_generation
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION bump_public_visibility_generation();
