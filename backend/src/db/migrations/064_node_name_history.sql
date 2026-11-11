SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- No historical scan: the first rename after installation also captures OLD.name.
-- Keep history independent of node cleanup so rediscovery retains former names.
CREATE TABLE node_name_history (
  node_id TEXT NOT NULL,
  name TEXT NOT NULL CHECK (BTRIM(name) <> ''),
  -- Compact key: a long MQTT origin must not exceed PostgreSQL's index tuple limit.
  name_hash BYTEA NOT NULL CHECK (name_hash = sha256(convert_to(name, 'UTF8'))),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_id, name_hash)
);

CREATE FUNCTION retain_node_name_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.name IS NOT DISTINCT FROM OLD.name THEN RETURN NEW; END IF;
    IF OLD.name IS NOT NULL AND BTRIM(OLD.name) <> '' THEN
      INSERT INTO node_name_history (node_id, name, name_hash)
      VALUES (OLD.node_id, OLD.name, sha256(convert_to(OLD.name, 'UTF8')))
      ON CONFLICT (node_id, name_hash) DO UPDATE SET last_seen_at = NOW();
    END IF;
  END IF;
  IF NEW.name IS NOT NULL AND BTRIM(NEW.name) <> '' THEN
    INSERT INTO node_name_history (node_id, name, name_hash)
    VALUES (NEW.node_id, NEW.name, sha256(convert_to(NEW.name, 'UTF8')))
    ON CONFLICT (node_id, name_hash) DO UPDATE SET last_seen_at = NOW();
  END IF;
  RETURN NEW;
END;
$$;

-- Covers both upsertNode paths, MQTT status, RF adverts and direct SQL renames.
CREATE TRIGGER nodes_retain_name_history
AFTER INSERT OR UPDATE OF name ON nodes
FOR EACH ROW EXECUTE FUNCTION retain_node_name_history();
