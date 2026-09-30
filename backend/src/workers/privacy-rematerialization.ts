import 'node:process';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import { observeWorkerOutcome } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';

const { Pool } = pg;

function boundedEnvNumber(name: string, fallback: number, min: number, max: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? Math.min(max, Math.max(min, Math.trunc(value))) : fallback;
}

const INTERVAL_MS = boundedEnvNumber('PRIVACY_REMAT_INTERVAL_MS', 120_000, 1_000, 3_600_000);
export const MAX_ATTEMPTS = boundedEnvNumber('PRIVACY_REMAT_MAX_ATTEMPTS', 8, 1, 100);
const CHUNK_PAUSE_MS = boundedEnvNumber('PRIVACY_REMAT_CHUNK_PAUSE_MS', 500, 0, 60_000);
const CHUNK_STATEMENT_TIMEOUT_MS = boundedEnvNumber(
  'PRIVACY_REMAT_CHUNK_STATEMENT_TIMEOUT_MS', 900_000, 1_000, 3_600_000,
);

type Target = 'packets' | 'packet_paths';
type QueueRow = {
  id: string;
  node_id: string;
  network: string | null;
  target: Target;
  attempts: number;
  requested_at: Date;
  started_at: Date;
};
type Chunk = { range_start: Date; range_end: Date };

// The two assignment expressions below are copied verbatim from migration 049.
// Keep packet privacy semantics here aligned with the deployed DB function.
const PACKET_UPDATE_SQL = `UPDATE packets p
     SET is_private = EXISTS (
           SELECT 1
           FROM private_node_prefixes pp
           WHERE (
               pp.network = p.network
               OR (
                 pp.network IN ('ukmesh', 'northeast', 'teesside')
                 AND p.network IN ('ukmesh', 'northeast', 'teesside')
               )
             )
             AND (
               pp.node_id IN (p.rx_node_id, p.src_node_id)
               OR (
                 p.path_hash_size_bytes = pp.prefix_size_bytes
                 AND EXISTS (
                   SELECT 1
                   FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS packet_prefix
                   WHERE UPPER(packet_prefix) = pp.prefix
                 )
               )
             )
         ),
         visibility_ok = (
           (COALESCE(cardinality(p.path_hashes), 0) = 0 OR p.path_hash_size_bytes BETWEEN 1 AND 3)
           AND NOT EXISTS (
             SELECT 1
             FROM private_node_prefixes pp
             WHERE (
                 pp.network = p.network
                 OR (
                   pp.network IN ('ukmesh', 'northeast', 'teesside')
                   AND p.network IN ('ukmesh', 'northeast', 'teesside')
                 )
               )
               AND (
                 pp.node_id IN (p.rx_node_id, p.src_node_id)
                 OR (
                   p.path_hash_size_bytes = pp.prefix_size_bytes
                   AND EXISTS (
                     SELECT 1
                     FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS packet_prefix
                     WHERE UPPER(packet_prefix) = pp.prefix
                   )
                 )
               )
           )
         )
   WHERE p.time >= $1 AND p.time < $2
     AND (
       p.rx_node_id = $3 OR p.src_node_id = $3
       OR ((p.network = $4 OR (p.network IN ('ukmesh', 'northeast', 'teesside')
         AND $4 IN ('ukmesh', 'northeast', 'teesside')))
         AND EXISTS (
           SELECT 1 FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS pp
           WHERE UPPER(pp) IN (UPPER(LEFT($3, 2)), UPPER(LEFT($3, 4)), UPPER(LEFT($3, 6)))
         ))
     )`;

const PATH_UPDATE_SQL = `UPDATE packet_paths path
       SET is_private = meshcore_path_matches_private(
             path.network, path.rx_node_id, path.src_node_id,
             path.path_hashes, path.path_hash_size_bytes
           ),
           visibility_ok = meshcore_path_is_valid(
             path.path_hashes, path.path_hash_size_bytes
           ) AND NOT meshcore_path_matches_private(
             path.network, path.rx_node_id, path.src_node_id,
             path.path_hashes, path.path_hash_size_bytes
           )
     WHERE path.time >= $1 AND path.time < $2
       AND (path.rx_node_id = $3 OR path.src_node_id = $3
         OR ((path.network = $4 OR (path.network IN ('ukmesh', 'northeast', 'teesside')
           AND $4 IN ('ukmesh', 'northeast', 'teesside')))
           AND path.path_hashes && ARRAY[
             UPPER(LEFT($3, 2)), UPPER(LEFT($3, 4)), UPPER(LEFT($3, 6))
           ]::text[]))`;

async function claim(pool: pg.Pool, maxAttempts: number): Promise<QueueRow | null> {
  const result = await pool.query<QueueRow>(`
    UPDATE privacy_rematerialization_queue
       SET status = 'processing', started_at = NOW(), attempts = attempts + 1,
           finished_at = NULL, last_error = NULL
     WHERE id = (
       SELECT id FROM privacy_rematerialization_queue
        WHERE status = 'pending'
           OR (status = 'failed' AND attempts < $1
             AND finished_at < NOW() - (attempts * attempts || ' minutes')::interval)
        ORDER BY requested_at LIMIT 1 FOR UPDATE SKIP LOCKED
     )
     RETURNING id::text, node_id, network, target, attempts, requested_at, started_at
  `, [maxAttempts]);
  return result.rows[0] ?? null;
}

async function processChunk(pool: pg.Pool, row: QueueRow, chunk: Chunk, timeoutMs: number): Promise<void> {
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL lock_timeout = '5s'");
      await client.query(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
      await client.query('SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0');
      await client.query(row.target === 'packets' ? PACKET_UPDATE_SQL : PATH_UPDATE_SQL, [
        chunk.range_start, chunk.range_end, row.node_id, row.network,
      ]);
      await client.query('COMMIT');
      console.log(`[privacy-remat] chunk complete id=${row.id} target=${row.target} start=${new Date(chunk.range_start).toISOString()}`);
      observeWorkerOutcome('privacy_remat', 'chunk', 'success');
      return;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      if (attempt === 3) throw error;
      observeWorkerOutcome('privacy_remat', 'chunk', 'retry');
      await delay(attempt * 1_000);
    } finally {
      client.release();
    }
  }
}

async function complete(pool: pg.Pool, row: QueueRow): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const completion = await client.query<{ status: string }>(`
      UPDATE privacy_rematerialization_queue
         SET status = CASE WHEN requested_at <= started_at THEN 'done' ELSE 'pending' END,
             finished_at = CASE WHEN requested_at <= started_at THEN NOW() ELSE NULL END,
             last_error = NULL
       WHERE id = $1 AND status = 'processing'
       RETURNING status
    `, [row.id]);
    if (completion.rowCount !== 1) throw new Error('PRIVACY_REMAT_CLAIM_LOST');

    // A re-request that arrived during this pass still needs a complete pass;
    // only a fully completed request may claim a new materialized generation.
    if (completion.rows[0]?.status === 'done') await client.query(`
      WITH next_generation AS (
        UPDATE public_visibility_state
           SET generation = generation + 1,
               updated_at = NOW()
         WHERE singleton = TRUE
         RETURNING generation
      )
      INSERT INTO packet_visibility_materialization_state
        (singleton, visibility_generation, updated_at)
      SELECT TRUE, generation, NOW() FROM next_generation
      ON CONFLICT (singleton) DO UPDATE SET
        visibility_generation = EXCLUDED.visibility_generation,
        updated_at = EXCLUDED.updated_at
    `);
    await client.query('COMMIT');
    console.log(`[privacy-remat] pass complete id=${row.id} status=${completion.rows[0]?.status}`);
    observeWorkerOutcome('privacy_remat', 'pass', 'success');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function processNextPrivacyRematerialization(
  pool: pg.Pool,
  options: { maxAttempts?: number; chunkPauseMs?: number; chunkStatementTimeoutMs?: number } = {},
): Promise<boolean> {
  const row = await claim(pool, options.maxAttempts ?? MAX_ATTEMPTS);
  if (!row) return false;
  console.log(`[privacy-remat] pass start id=${row.id} target=${row.target} attempt=${row.attempts}`);
  try {
    const chunks = await pool.query<Chunk>(`
      SELECT range_start, range_end
        FROM timescaledb_information.chunks
       WHERE hypertable_schema = 'public' AND hypertable_name = $1
       ORDER BY range_start
    `, [row.target]);
    for (const [index, chunk] of chunks.rows.entries()) {
      await processChunk(pool, row, chunk, options.chunkStatementTimeoutMs ?? CHUNK_STATEMENT_TIMEOUT_MS);
      if (index < chunks.rows.length - 1) await delay(options.chunkPauseMs ?? CHUNK_PAUSE_MS);
    }
    await complete(pool, row);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await pool.query(`
      UPDATE privacy_rematerialization_queue
         SET status = 'failed', last_error = $2, finished_at = NOW()
       WHERE id = $1 AND status = 'processing'
    `, [row.id, message.slice(0, 2_000)]);
    observeWorkerOutcome('privacy_remat', 'pass', 'failure');
    console.error(`[privacy-remat] pass failed id=${row.id} target=${row.target}: ${message}`);
  }
  return true;
}

async function main(): Promise<void> {
  startWorkerMetrics();
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    application_name: process.env['DATABASE_APPLICATION_NAME'] ?? 'meshcore-privacy-remat',
    max: 2,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
  });
  pool.on('error', (error) => console.error('[privacy-remat] pool error:', error.message));
  const shutdown = new AbortController();
  process.once('SIGTERM', () => shutdown.abort());
  process.once('SIGINT', () => shutdown.abort());
  try {
    while (!shutdown.signal.aborted) {
      try {
        await processNextPrivacyRematerialization(pool);
      } catch (error) {
        console.error('[privacy-remat] iteration failed:', error);
      }
      if (!shutdown.signal.aborted) {
        try { await delay(INTERVAL_MS, undefined, { signal: shutdown.signal }); }
        catch (error) {
          if (!shutdown.signal.aborted) throw error;
        }
      }
    }
  } finally {
    await pool.end();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error('[privacy-remat] fatal startup error:', error);
    process.exitCode = 1;
  });
}
