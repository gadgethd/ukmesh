import pg from 'pg';
import { analyticsStatementTimeoutMs, databaseConfig, databaseUrlForRole } from '../platform/config/database.js';
import { dbQueriesTotal, dbQueryDuration, updateDbPoolMetrics } from '../metrics.js';
import { ingestPool } from './ingest.js';
import type { DatabaseQueryFn } from './contracts.js';

const { Pool } = pg;
const pool = new Pool({
  connectionString: databaseUrlForRole('http'),
  application_name: databaseConfig.applicationName,
  options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  max: databaseConfig.poolMax,
  idleTimeoutMillis: databaseConfig.idleTimeoutMs,
  connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
  statement_timeout: databaseConfig.statementTimeoutMs,
  query_timeout: databaseConfig.statementTimeoutMs,
});

pool.on('error', (err) => {
  console.error('[db] unexpected pool error', err.message);
});

// Dedicated pool for long-running analytical queries.
// Kept separate so slow analytics can never exhaust the OLTP pool and take down the API.
const analyticsPool = new Pool({
  connectionString: databaseUrlForRole('worker'),
  application_name: `${databaseConfig.applicationName}-analytics`,
  options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  max: 2,
  idleTimeoutMillis: databaseConfig.idleTimeoutMs,
  connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
  statement_timeout: analyticsStatementTimeoutMs(databaseConfig.statementTimeoutMs),
  query_timeout: analyticsStatementTimeoutMs(databaseConfig.statementTimeoutMs),
});

analyticsPool.on('error', (err) => {
  console.error('[db] unexpected analytics pool error', err.message);
});

async function queryPool<T extends pg.QueryResultRow = pg.QueryResultRow>(
  targetPool: pg.Pool,
  poolName: 'oltp' | 'analytics',
  querySpec: string | pg.QueryConfig<unknown[]>,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  const startedAt = process.hrtime.bigint();
  let outcome = 'success';
  try {
    updateDbPoolMetrics(poolName, targetPool);
    const execute = (target: pg.Pool | pg.PoolClient) => (
      typeof querySpec === 'string'
        ? target.query<T>(querySpec, params)
        : target.query<T>(querySpec)
    );
    if (!signal) return await execute(targetPool);
    signal.throwIfAborted();
    const client = await targetPool.connect();
    let destroyed = false;
    let abortedCleanup: (() => void) | undefined;
    const aborted = new Promise<never>((_resolve, reject) => {
      const onAbort = () => {
        if (destroyed) return;
        destroyed = true;
        // node-postgres has no AbortSignal query API. Destroy this one
        // checked-out connection so PostgreSQL cancels the active statement and
        // the connection can never return to the pool.
        client.release(true);
        reject(
          signal.reason instanceof Error
            ? signal.reason
            : new Error('database query aborted'),
        );
      };
      signal.addEventListener('abort', onAbort, { once: true });
      abortedCleanup = () => signal.removeEventListener('abort', onAbort);
    });
    try {
      signal.throwIfAborted();
      const result = await Promise.race([
        execute(client),
        aborted,
      ]);
      signal.throwIfAborted();
      return result;
    } finally {
      abortedCleanup?.();
      if (!destroyed) client.release();
    }
  } catch (error) {
    outcome = signal?.aborted ? 'aborted' : 'failure';
    throw error;
  } finally {
    const durationSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
    dbQueriesTotal.inc({ pool: poolName, outcome });
    dbQueryDuration.observe({ pool: poolName, outcome }, durationSeconds);
    updateDbPoolMetrics(poolName, targetPool);
  }
}

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  return queryPool<T>(pool, 'oltp', text, params, signal);
}

export async function withTransaction<T>(work: (transactionQuery: DatabaseQueryFn) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  const transactionQuery: DatabaseQueryFn = async <R extends pg.QueryResultRow = pg.QueryResultRow>(
    text: string,
    params?: unknown[],
  ) => {
    const result = await client.query<R>(text, params);
    return { rows: result.rows };
  };
  try {
    await client.query('BEGIN');
    const result = await work(transactionQuery);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

export async function namedQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
  name: string,
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  if (!/^[a-z0-9_-]{1,63}$/.test(name)) throw new Error('INVALID_PREPARED_STATEMENT_NAME');
  return queryPool<T>(pool, 'oltp', { name, text, values: params }, undefined, signal);
}

export async function analyticsQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  return queryPool<T>(analyticsPool, 'analytics', text, params, signal);
}

export async function initDb(): Promise<void> {
  // P1-9: schema creation and migrations are an explicit migrate-job step.
  // Keeping them on every service/worker startup races replicas, hides drift
  // and lets an application role execute DDL.
  console.warn('[db] startup schema initialisation is disabled; run the migrate job before starting services');
}

export { pool, analyticsPool };

export async function closeDb(): Promise<void> {
  await Promise.all([pool.end(), analyticsPool.end(), ingestPool.end()]);
}
