import pg from 'pg';
import { boundedIntegerSetting } from '../platform/config/boundedNumber.js';
import { databaseConfig } from '../platform/config/database.js';
import { dbQueriesTotal, dbQueryDuration, updateDbPoolMetrics } from '../metrics.js';
import type { DatabaseQueryFn } from './index.js';

export function ingestPoolOptions(env: NodeJS.ProcessEnv = process.env): pg.PoolConfig {
  const timeoutMs = boundedIntegerSetting(
    'MQTT_INGEST_STATEMENT_TIMEOUT_MS', env['MQTT_INGEST_STATEMENT_TIMEOUT_MS'], 10_000, 100, 30_000,
  );
  return {
    connectionString: env['DATABASE_URL'],
    application_name: `${databaseConfig.applicationName}-ingest`,
    options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
    max: 2,
    idleTimeoutMillis: databaseConfig.idleTimeoutMs,
    connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
    // PostgreSQL must cancel the statement before the client read deadline.
    // The analytics/general timeout (including 0) cannot disable this bound.
    statement_timeout: timeoutMs,
    query_timeout: timeoutMs + 1_000,
  };
}

function clientReadTimedOut(error: unknown): boolean {
  return error instanceof Error && error.message === 'Query read timeout';
}

export function createIngestDatabase(pool: pg.Pool) {
  async function execute<T extends pg.QueryResultRow = pg.QueryResultRow>(
    target: pg.Pool | pg.PoolClient, text: string, params?: unknown[],
  ): Promise<pg.QueryResult<T>> {
    const startedAt = process.hrtime.bigint();
    let outcome = 'success';
    try {
      return await target.query<T>(text, params);
    } catch (error) {
      const timedOut = (error as { code?: string } | null)?.code === '57014' || clientReadTimedOut(error);
      outcome = timedOut ? 'timed_out' : 'failure';
      if (timedOut) {
        // Never log query text, parameters, node IDs or packet contents.
        console.error('[ingest-db] query timed out', { timeoutMs: pool.options.statement_timeout });
      }
      throw error;
    } finally {
      dbQueriesTotal.inc({ pool: 'ingest', outcome });
      dbQueryDuration.observe({ pool: 'ingest', outcome }, Number(process.hrtime.bigint() - startedAt) / 1e9);
      updateDbPoolMetrics('ingest', pool);
    }
  }

  const query: DatabaseQueryFn = (text, params) => execute(pool, text, params);
  async function withTransaction<T>(work: (query: DatabaseQueryFn) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    let discard = false;
    try {
      await execute(client, 'BEGIN');
      const result = await work((text, params) => execute(client, text, params));
      await execute(client, 'COMMIT');
      return result;
    } catch (error) {
      // A client read deadline alone does not cancel server work. Destroy that
      // socket instead of queuing ROLLBACK behind a potentially hung statement.
      discard = clientReadTimedOut(error);
      if (!discard) {
        try { await execute(client, 'ROLLBACK'); }
        catch { discard = true; }
      }
      throw error;
    } finally {
      client.release(discard);
    }
  }
  return { query, withTransaction };
}

export const ingestPool = new pg.Pool(ingestPoolOptions());
ingestPool.on('error', (error) => console.error('[ingest-db] unexpected pool error', error.message));
const ingestDatabase = createIngestDatabase(ingestPool);
export const ingestQuery = ingestDatabase.query;
export const withIngestTransaction = ingestDatabase.withTransaction;
