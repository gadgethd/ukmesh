-- Calendar intervals compare with 30-day months but subtract by the calendar.
-- Keep151 immutable; constrain its owner capability to fixed day/time durations
-- and check the actual cutoff so month/DST edges cannot shorten retention.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION ops.prune_expired_hypertable(target regclass, retention interval)
RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog
AS $maintenance$
DECLARE
  target_schema text;
  target_name text;
  boundary record;
  expired boolean;
  affected bigint := 0;
BEGIN
  -- Interval comparison treats one month as30days, while timestamp subtraction
  -- follows the calendar. These policies use day/time durations, so reject
  -- calendar components and compare their actual cutoff as well.
  IF target IS NULL OR retention IS NULL
    OR extract(year FROM retention) <> 0 OR extract(month FROM retention) <> 0
    OR retention < interval '30 days'
    OR now() - retention > now() - interval '30 days' THEN
    RAISE EXCEPTION 'Unsupported automatic retention policy' USING ERRCODE = '42501';
  END IF;
  IF target = 'mesh.packets'::regclass THEN
    target_schema := 'mesh'; target_name := 'packets';
  ELSIF target = 'core.node_status_samples'::regclass THEN
    target_schema := 'core'; target_name := 'node_status_samples';
  ELSE
    RAISE EXCEPTION 'Unsupported automatic retention relation' USING ERRCODE = '42501';
  END IF;
  IF NOT EXISTS (SELECT FROM timescaledb_information.hypertables
    WHERE hypertable_schema = target_schema AND hypertable_name = target_name) THEN
    RAISE EXCEPTION 'Automatic retention requires a registered hypertable' USING ERRCODE = '42809';
  END IF;

  PERFORM public.drop_chunks(target, older_than => retention);
  SELECT chunk_schema, chunk_name, is_compressed INTO boundary
    FROM timescaledb_information.chunks
    WHERE hypertable_schema = target_schema AND hypertable_name = target_name
      AND range_end > now() - retention
    ORDER BY range_end ASC LIMIT 1;
  IF NOT FOUND THEN RETURN 0; END IF;

  -- Identifiers come only from the registered target's extension catalog;
  -- neither the caller nor a writable search_path can choose another object.
  EXECUTE format('SELECT EXISTS (SELECT FROM %I.%I WHERE time < now() - $1)',
    boundary.chunk_schema, boundary.chunk_name) INTO expired USING retention;
  IF NOT expired THEN RETURN 0; END IF;
  PERFORM public.decompress_chunk(
    format('%I.%I', boundary.chunk_schema, boundary.chunk_name)::regclass,
    if_compressed => true);
  EXECUTE format('DELETE FROM %I.%I WHERE time < now() - $1',
    boundary.chunk_schema, boundary.chunk_name) USING retention;
  GET DIAGNOSTICS affected = ROW_COUNT;
  IF boundary.is_compressed THEN
    PERFORM public.compress_chunk(
      format('%I.%I', boundary.chunk_schema, boundary.chunk_name)::regclass,
      if_not_compressed => true);
  END IF;
  RETURN affected;
END
$maintenance$;

REVOKE ALL ON FUNCTION ops.prune_expired_hypertable(regclass, interval) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.prune_expired_hypertable(regclass, interval) TO meshcore_worker;
