-- Receipts and graph writes share a transaction. A rebuild worker can reuse
-- its physical-pair snapshot only when no peer has committed a newer revision.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE ops.link_rebuild_runs ADD COLUMN topology_revision bigint NOT NULL DEFAULT 0;

CREATE FUNCTION ops.note_link_topology_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  UPDATE ops.link_rebuild_runs SET topology_revision = topology_revision + 1
    WHERE generation = NEW.generation;
  RETURN NEW;
END;
$function$;

CREATE TRIGGER link_rebuild_topology_revision
  AFTER INSERT ON ops.link_job_commits
  FOR EACH ROW WHEN (NEW.generation IS NOT NULL)
  EXECUTE FUNCTION ops.note_link_topology_revision();
