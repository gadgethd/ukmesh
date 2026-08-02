SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE packets
  ADD COLUMN IF NOT EXISTS topic_prefix TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS iata TEXT,
  ADD COLUMN IF NOT EXISTS is_private BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS visibility_ok BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS last_rx_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_status_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS observer_iata TEXT;

CREATE TABLE IF NOT EXISTS private_node_prefixes (
  node_id           TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  network           TEXT NOT NULL,
  prefix_size_bytes SMALLINT NOT NULL CHECK (prefix_size_bytes BETWEEN 1 AND 3),
  prefix            TEXT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_id, network, prefix_size_bytes)
);

CREATE INDEX IF NOT EXISTS private_node_prefixes_lookup_idx
  ON private_node_prefixes (network, prefix_size_bytes, prefix);

CREATE OR REPLACE FUNCTION sync_private_node_prefixes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
BEGIN
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

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_private_prefix_materialization ON nodes;
CREATE TRIGGER nodes_private_prefix_materialization
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION sync_private_node_prefixes();

INSERT INTO private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
SELECT n.node_id, n.network, sizes.size_bytes, UPPER(LEFT(n.node_id, sizes.size_bytes * 2))
FROM nodes n
CROSS JOIN (VALUES (1), (2), (3)) AS sizes(size_bytes)
WHERE n.name LIKE '%🚫%'
ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
  prefix = EXCLUDED.prefix,
  updated_at = NOW();

CREATE INDEX IF NOT EXISTS packets_public_visibility_idx
  ON packets (network, time DESC)
  WHERE visibility_ok IS TRUE;
