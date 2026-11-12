-- 099_repair_node_name_history_trigger.sql
-- 093 moved node_name_history to core; the nodes trigger function was missed.
CREATE OR REPLACE FUNCTION public.retain_node_name_history()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.name IS NOT DISTINCT FROM OLD.name THEN RETURN NEW; END IF;
    IF OLD.name IS NOT NULL AND BTRIM(OLD.name) <> '' THEN
      INSERT INTO core.node_name_history (node_id, name, name_hash)
      VALUES (OLD.node_id, OLD.name, sha256(convert_to(OLD.name, 'UTF8')))
      ON CONFLICT (node_id, name_hash) DO UPDATE SET last_seen_at = NOW();
    END IF;
  END IF;
  IF NEW.name IS NOT NULL AND BTRIM(NEW.name) <> '' THEN
    INSERT INTO core.node_name_history (node_id, name, name_hash)
    VALUES (NEW.node_id, NEW.name, sha256(convert_to(NEW.name, 'UTF8')))
    ON CONFLICT (node_id, name_hash) DO UPDATE SET last_seen_at = NOW();
  END IF;
  RETURN NEW;
END;
$function$;
