import 'node:process';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import { randomUUID } from 'node:crypto';
import { observeWorkerOutcome } from '../metrics/workers.js';
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
  claim_token: string;
};
type Chunk = { range_start: Date; range_end: Date };

// A compressed chunk has no usable ctid. These values identify the class of
// rows with the same result, including historical duplicate/null observation
// IDs. Ignore current flag values until UPDATE, where they may have changed.
function chunkUpdateSql(target: Target): string {
  const pathMatch = target === 'packets'
    ? `EXISTS (SELECT 1 FROM unnest(p.path_hashes) AS hop
        WHERE UPPER(hop) IN (UPPER(LEFT($3, 2)), UPPER(LEFT($3, 4)), UPPER(LEFT($3, 6))))`
    : `p.path_hashes && ARRAY[UPPER(LEFT($3, 2)), UPPER(LEFT($3, 4)), UPPER(LEFT($3, 6))]::text[]`;
  const input = 'c.network, c.rx_node_id, c.src_node_id, c.path_hashes, c.path_hash_size_bytes';
  const classification = target === 'packets'
    ? `public.meshcore_packet_visibility(${input}) AS flags`
    : `(SELECT public.meshcore_path_matches_private(${input}) AS is_private OFFSET 0) AS flags`;
  const visible = target === 'packets' ? 'flags.visibility_ok'
    : 'public.meshcore_path_is_valid(c.path_hashes, c.path_hash_size_bytes) AND NOT flags.is_private';

  return `WITH candidates AS (
      SELECT DISTINCT p.time, p.packet_hash, p.network, p.rx_node_id, p.src_node_id,
             p.path_hashes, p.path_hash_size_bytes
        FROM mesh.${target} p
       WHERE p.time >= $1 AND p.time < $2
         AND (p.rx_node_id = $3 OR p.src_node_id = $3
           OR ((p.network = $4 OR (p.network IN ('ukmesh', 'northeast', 'teesside')
              AND $4 IN ('ukmesh', 'northeast', 'teesside'))) AND ${pathMatch}))
    ), classified AS MATERIALIZED (
      SELECT c.*, flags.is_private, ${visible} AS visibility_ok
        FROM candidates c CROSS JOIN LATERAL ${classification}
    )
    UPDATE mesh.${target} p
       SET is_private = c.is_private, visibility_ok = c.visibility_ok
      FROM classified c
     WHERE p.time >= $1 AND p.time < $2
       AND p.time = c.time AND p.packet_hash = c.packet_hash AND p.network = c.network
       AND p.rx_node_id IS NOT DISTINCT FROM c.rx_node_id
       AND p.src_node_id IS NOT DISTINCT FROM c.src_node_id
       AND p.path_hashes IS NOT DISTINCT FROM c.path_hashes
       AND p.path_hash_size_bytes IS NOT DISTINCT FROM c.path_hash_size_bytes
       AND (p.is_private, p.visibility_ok) IS DISTINCT FROM (c.is_private, c.visibility_ok)`;
}

const CHUNK_UPDATES = {
  packets: chunkUpdateSql('packets'),
  packet_paths: chunkUpdateSql('packet_paths'),
};

export type PrivacyClaimReconciliation = {
  /** Rows whose lease had expired and are now retryable or terminally failed. */
  reclaimed: number;
  /** Reclaimed rows that had already used every attempt. */
  exhausted: number;
};

/**
 * Reclaim privacy-rematerialization rows abandoned by a dead worker.
 *
 * A worker crash leaves `status = 'processing'` with a `claim_token` that no
 * live process will ever clear. Ageing the claim out here is what stops that
 * row from blocking the queue forever. Rows that have attempts left return to
 * the normal failed/backoff path; rows that have exhausted `maxAttempts` stay
 * `failed` with a distinct `last_error` so they are visible as terminal
 * poison instead of being silently re-claimed.
 */
export async function reconcileAbandonedPrivacyClaims(
  pool: Pick<pg.Pool, 'query'>,
  maxAttempts: number = MAX_ATTEMPTS,
): Promise<PrivacyClaimReconciliation> {
  const result = await pool.query<{ attempts: number }>(`
    UPDATE ops.privacy_rematerialization_queue
       SET status = 'failed',
           claim_token = NULL,
           claim_expires_at = NULL,
           finished_at = NOW(),
           last_error = CASE
             WHEN attempts >= $1
               THEN 'worker lease expired; attempts exhausted (' || attempts || '/' || $1 || ')'
             ELSE 'worker lease expired'
           END
     WHERE status = 'processing'
       AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())
     RETURNING attempts
  `, [maxAttempts]);
  return {
    reclaimed: result.rowCount ?? 0,
    exhausted: result.rows.filter((row) => row.attempts >= maxAttempts).length,
  };
}

async function claim(pool: pg.Pool, maxAttempts: number, leaseMs: number): Promise<QueueRow | null> {
  await reconcileAbandonedPrivacyClaims(pool, maxAttempts);
  const result = await pool.query<QueueRow>(`
    UPDATE ops.privacy_rematerialization_queue
       SET status = 'processing', started_at = NOW(), attempts = attempts + 1,
           finished_at = NULL, last_error = NULL, claim_token = $2,
           claim_expires_at = NOW() + ($3::text || ' milliseconds')::interval
     WHERE id = (
       SELECT id FROM ops.privacy_rematerialization_queue
        WHERE status = 'pending'
           OR (status = 'failed' AND attempts < $1
             AND finished_at < NOW() - (attempts * attempts || ' minutes')::interval)
        ORDER BY requested_at LIMIT 1 FOR UPDATE SKIP LOCKED
     )
     RETURNING id::text, node_id, network, target, attempts, requested_at, started_at, claim_token::text
  `, [maxAttempts, randomUUID(), leaseMs]);
  return result.rows[0] ?? null;
}

async function processChunk(pool: pg.Pool, row: QueueRow, chunk: Chunk, timeoutMs: number, leaseMs: number): Promise<void> {
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const renewal = await pool.query(`UPDATE ops.privacy_rematerialization_queue
        SET claim_expires_at = NOW() + ($3::text || ' milliseconds')::interval
        WHERE id = $1 AND claim_token = $2 AND status = 'processing' AND claim_expires_at > NOW()`,
        [row.id, row.claim_token, leaseMs]);
    if (renewal.rowCount !== 1) throw new Error('PRIVACY_REMAT_CLAIM_LOST');

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL lock_timeout = '5s'");
      await client.query(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
      await client.query('SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0');
      await client.query(CHUNK_UPDATES[row.target], [
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
    } finally {
      client.release();
    }
    await delay(attempt * 1_000);
  }
}

async function complete(pool: pg.Pool, row: QueueRow): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const completion = await client.query<{ status: string }>(`
      UPDATE ops.privacy_rematerialization_queue
         SET status = CASE WHEN requested_at <= started_at THEN 'done' ELSE 'pending' END,
             finished_at = CASE WHEN requested_at <= started_at THEN NOW() ELSE NULL END,
             last_error = NULL, claim_token = NULL, claim_expires_at = NULL
       WHERE id = $1 AND status = 'processing' AND claim_token = $2
         AND claim_expires_at > NOW()
       RETURNING status
    `, [row.id, row.claim_token]);
    if (completion.rowCount !== 1) throw new Error('PRIVACY_REMAT_CLAIM_LOST');

    // A re-request still needs a full pass. A node-scoped pass also cannot
    // repair an unrelated direct-prefix fence: carry materialization forward
    // only if it was current before this completion. Full convergence owns
    // recovery of a pre-existing gap (including a missing state row).
    if (completion.rows[0]?.status === 'done') await client.query(`
      WITH previous_generation AS MATERIALIZED (
        SELECT generation FROM ops.public_visibility_state
         WHERE singleton = TRUE FOR UPDATE
      ), next_generation AS (
        UPDATE ops.public_visibility_state visibility
           SET generation = visibility.generation + 1,
               updated_at = NOW()
          FROM previous_generation previous
         WHERE visibility.singleton = TRUE
         RETURNING visibility.generation, previous.generation AS previous_generation
      )
      UPDATE ops.packet_visibility_materialization_state materialized
         SET visibility_generation = next.generation, updated_at = NOW()
        FROM next_generation next
       WHERE materialized.singleton = TRUE
         AND materialized.visibility_generation = next.previous_generation
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
  const timeoutMs = options.chunkStatementTimeoutMs ?? CHUNK_STATEMENT_TIMEOUT_MS;
  const leaseMs = timeoutMs + 120_000 + (options.chunkPauseMs ?? CHUNK_PAUSE_MS);
  const row = await claim(pool, options.maxAttempts ?? MAX_ATTEMPTS, leaseMs);
  if (!row) return false;
  console.log(`[privacy-remat] pass start id=${row.id} target=${row.target} attempt=${row.attempts}`);
  try {
    const chunks = await pool.query<Chunk>(`
      SELECT range_start, range_end
        FROM timescaledb_information.chunks
       WHERE hypertable_schema = 'mesh' AND hypertable_name = $1
       ORDER BY range_start
    `, [row.target]);
    for (const [index, chunk] of chunks.rows.entries()) {
      await processChunk(pool, row, chunk, timeoutMs, leaseMs);
      if (index < chunks.rows.length - 1) await delay(options.chunkPauseMs ?? CHUNK_PAUSE_MS);
    }
    await complete(pool, row);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await pool.query(`
      UPDATE ops.privacy_rematerialization_queue
         SET status = 'failed', last_error = $2, finished_at = NOW(),
             claim_token = NULL, claim_expires_at = NULL
       WHERE id = $1 AND status = 'processing' AND claim_token = $3
    `, [row.id, message.slice(0, 2_000), row.claim_token]);
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
