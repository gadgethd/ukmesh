import type { QueryResultRow } from 'pg';
import { publicPacketPrivacySql } from '../api/utils/networkFilters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type MultibyteFactBackfillResult = {
  visibilityGeneration: number;
  windowStart: Date;
  cutoff: Date;
  affectedRows: number;
};

export type MultibyteFactChunk = {
  chunkSchema: string;
  chunkName: string;
  rangeStart: Date;
  rangeEnd: Date;
  wasCompressed: boolean;
};

export function selectMultibyteFactChunkBatch(
  chunks: readonly MultibyteFactChunk[],
  startIndex: number,
  limit: number,
): MultibyteFactChunk[] {
  if (!Number.isSafeInteger(startIndex) || startIndex < 0) {
    throw new Error('INVALID_MULTIBYTE_FACT_CHUNK_INDEX');
  }
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 4) {
    throw new Error('INVALID_MULTIBYTE_FACT_CHUNK_LIMIT');
  }
  return chunks.slice(startIndex, startIndex + limit);
}

export async function listMultibyteFactChunks(
  query: QueryFn,
  input: { windowStart: Date; cutoff: Date },
): Promise<MultibyteFactChunk[]> {
  const result = await query<{
    chunk_schema: string;
    chunk_name: string;
    range_start: Date;
    range_end: Date;
    is_compressed: boolean;
  }>(
    `SELECT chunk_schema, chunk_name, range_start, range_end, is_compressed
       FROM timescaledb_information.chunks
      WHERE hypertable_schema = 'public'
        AND hypertable_name = 'packets'
        AND range_end >= $1::timestamptz
        AND range_start <= $2::timestamptz
      ORDER BY range_start ASC`,
    [input.windowStart.toISOString(), input.cutoff.toISOString()],
  );
  return result.rows.map((row) => ({
    chunkSchema: row.chunk_schema,
    chunkName: row.chunk_name,
    rangeStart: new Date(Math.max(input.windowStart.getTime(), new Date(row.range_start).getTime())),
    rangeEnd: new Date(Math.min(input.cutoff.getTime(), new Date(row.range_end).getTime())),
    wasCompressed: row.is_compressed,
  }));
}

export async function setMultibyteFactChunkCompression(
  query: QueryFn,
  chunk: Pick<MultibyteFactChunk, 'chunkSchema' | 'chunkName'>,
  compressed: boolean,
): Promise<void> {
  if (!/^[_a-z][_a-z0-9]*$/i.test(chunk.chunkSchema)
    || !/^[_a-z][_a-z0-9]*$/i.test(chunk.chunkName)) {
    throw new Error('INVALID_MULTIBYTE_FACT_CHUNK_NAME');
  }
  const qualifiedName = `"${chunk.chunkSchema}"."${chunk.chunkName}"`;
  await query(
    `SELECT ${compressed ? 'compress_chunk' : 'decompress_chunk'}($1::regclass, TRUE)`,
    [qualifiedName],
  );
}

export function multibyteObservationIdBatchSql(): string {
  return `WITH candidates AS MATERIALIZED (
    SELECT p.tableoid AS source_tableoid, p.ctid AS source_ctid
    FROM packets p
    WHERE p.time >= $1::timestamptz
      AND p.time <= $2::timestamptz
      AND p.observation_id IS NULL
      AND p.path_hash_size_bytes BETWEEN 2 AND 3
      AND COALESCE(cardinality(p.path_hashes), 0) > 0
    ORDER BY p.time ASC
    LIMIT $3::integer
  ), updated AS (
    UPDATE packets p
       SET observation_id = gen_random_uuid()
      FROM candidates
     WHERE p.tableoid = candidates.source_tableoid
       AND p.ctid = candidates.source_ctid
       AND p.time >= $1::timestamptz
       AND p.time <= $2::timestamptz
    RETURNING 1
  )
  SELECT COUNT(*)::integer AS affected_rows FROM updated`;
}

export async function backfillMultibyteObservationIds(
  query: QueryFn,
  input: { windowStart: Date; cutoff: Date; batchSize: number },
): Promise<number> {
  if (!(input.windowStart instanceof Date) || !Number.isFinite(input.windowStart.getTime())) {
    throw new Error('INVALID_MULTIBYTE_FACT_WINDOW_START');
  }
  if (!(input.cutoff instanceof Date) || !Number.isFinite(input.cutoff.getTime())) {
    throw new Error('INVALID_MULTIBYTE_FACT_CUTOFF');
  }
  if (!Number.isSafeInteger(input.batchSize) || input.batchSize < 1 || input.batchSize > 10_000) {
    throw new Error('INVALID_MULTIBYTE_OBSERVATION_ID_BATCH_SIZE');
  }
  const result = await query<{ affected_rows: number }>(multibyteObservationIdBatchSql(), [
    input.windowStart.toISOString(),
    input.cutoff.toISOString(),
    input.batchSize,
  ]);
  return Number(result.rows[0]?.affected_rows ?? 0);
}

export function multibyteFactBackfillSql(): string {
  const publicVisibility = publicPacketPrivacySql('p');
  return `WITH source_rows AS MATERIALIZED (
    SELECT
      p.observation_id,
      p.time AS observed_at,
      p.packet_hash,
      p.network,
      p.rx_node_id,
      p.src_node_id,
      p.topic,
      p.topic_prefix,
      p.hop_count,
      p.path_hashes,
      p.path_hash_size_bytes,
      ${publicVisibility} AS current_visibility_ok
    FROM packets p
    WHERE p.time >= $1::timestamptz
      AND p.time <= $2::timestamptz
      AND p.path_hash_size_bytes BETWEEN 2 AND 3
      AND COALESCE(cardinality(p.path_hashes), 0) > 0
      AND p.observation_id IS NOT NULL
  ), decoded AS MATERIALIZED (
    SELECT source_rows.*, path_decode.*
    FROM source_rows
    CROSS JOIN LATERAL meshcore_decode_multibyte_path(
      source_rows.rx_node_id,
      source_rows.hop_count,
      source_rows.path_hash_size_bytes,
      source_rows.path_hashes
    ) path_decode
  ), written AS (
    INSERT INTO multibyte_path_facts (
      observation_id, observed_at, packet_hash, network, rx_node_id, src_node_id,
      topic, topic_prefix, path_hashes, path_hash_size_bytes,
      visibility_ok, is_private, visibility_generation,
      fully_decoded, decoded_hops, decoded_path, decoded_node_ids
    )
    SELECT
      observation_id, observed_at, packet_hash, network, rx_node_id, src_node_id,
      topic, topic_prefix, path_hashes, path_hash_size_bytes,
      current_visibility_ok, NOT current_visibility_ok, $3::bigint,
      fully_decoded, decoded_hops, decoded_path, decoded_node_ids
    FROM decoded
    ON CONFLICT (observation_id) DO UPDATE SET
      observed_at = EXCLUDED.observed_at,
      packet_hash = EXCLUDED.packet_hash,
      network = EXCLUDED.network,
      rx_node_id = EXCLUDED.rx_node_id,
      src_node_id = EXCLUDED.src_node_id,
      topic = EXCLUDED.topic,
      topic_prefix = EXCLUDED.topic_prefix,
      path_hashes = EXCLUDED.path_hashes,
      path_hash_size_bytes = EXCLUDED.path_hash_size_bytes,
      visibility_ok = EXCLUDED.visibility_ok,
      is_private = EXCLUDED.is_private,
      visibility_generation = EXCLUDED.visibility_generation,
      fully_decoded = EXCLUDED.fully_decoded,
      decoded_hops = EXCLUDED.decoded_hops,
      decoded_path = EXCLUDED.decoded_path,
      decoded_node_ids = EXCLUDED.decoded_node_ids,
      updated_at = NOW()
    RETURNING 1
  ), state AS (
    INSERT INTO multibyte_path_fact_state (
      singleton, visibility_generation, covered_from, covered_through, row_count, updated_at
    )
    SELECT TRUE, $3::bigint, $1::timestamptz, $2::timestamptz, COUNT(*)::bigint, NOW()
    FROM multibyte_path_facts
    WHERE visibility_generation = $3::bigint
      AND observed_at >= $1::timestamptz
      AND observed_at <= $2::timestamptz
    ON CONFLICT (singleton) DO UPDATE SET
      visibility_generation = EXCLUDED.visibility_generation,
      covered_from = CASE
        WHEN multibyte_path_fact_state.visibility_generation = EXCLUDED.visibility_generation
          THEN LEAST(multibyte_path_fact_state.covered_from, EXCLUDED.covered_from)
        ELSE EXCLUDED.covered_from
      END,
      covered_through = CASE
        WHEN multibyte_path_fact_state.visibility_generation = EXCLUDED.visibility_generation
          THEN GREATEST(multibyte_path_fact_state.covered_through, EXCLUDED.covered_through)
        ELSE EXCLUDED.covered_through
      END,
      row_count = EXCLUDED.row_count,
      updated_at = NOW()
    RETURNING 1
  )
  SELECT COUNT(*)::integer AS affected_rows
  FROM written
  CROSS JOIN state`;
}

export async function backfillMultibytePathFacts(
  query: QueryFn,
  input: { windowStart: Date; cutoff: Date; visibilityGeneration: number },
): Promise<MultibyteFactBackfillResult> {
  if (!(input.windowStart instanceof Date) || !Number.isFinite(input.windowStart.getTime())) {
    throw new Error('INVALID_MULTIBYTE_FACT_WINDOW_START');
  }
  if (!(input.cutoff instanceof Date) || !Number.isFinite(input.cutoff.getTime())) {
    throw new Error('INVALID_MULTIBYTE_FACT_CUTOFF');
  }
  if (input.cutoff < input.windowStart) throw new Error('INVALID_MULTIBYTE_FACT_WINDOW');
  if (!Number.isSafeInteger(input.visibilityGeneration) || input.visibilityGeneration < 1) {
    throw new Error('INVALID_MULTIBYTE_FACT_VISIBILITY_GENERATION');
  }

  const result = await query<{ affected_rows: number }>(multibyteFactBackfillSql(), [
    input.windowStart.toISOString(),
    input.cutoff.toISOString(),
    input.visibilityGeneration,
  ]);
  return {
    ...input,
    affectedRows: Number(result.rows[0]?.affected_rows ?? 0),
  };
}

export async function multibyteFactsCoverWindow(
  query: QueryFn,
  input: { windowStart: Date; visibilityGeneration: number },
): Promise<boolean> {
  const result = await query<{ ready: boolean }>(
    `SELECT EXISTS (
       SELECT 1
       FROM multibyte_path_fact_state
       WHERE singleton = TRUE
         AND visibility_generation = $1::bigint
         AND covered_from <= $2::timestamptz
     ) AS ready`,
    [input.visibilityGeneration, input.windowStart.toISOString()],
  );
  return result.rows[0]?.ready === true;
}

export async function ensureMultibyteFactsCoverWindow(
  query: QueryFn,
  input: { windowStart: Date; cutoff: Date; visibilityGeneration: number },
): Promise<{ backfilled: boolean; affectedRows: number }> {
  if (await multibyteFactsCoverWindow(query, input)) {
    return { backfilled: false, affectedRows: 0 };
  }
  const result = await backfillMultibytePathFacts(query, input);
  return { backfilled: true, affectedRows: result.affectedRows };
}
