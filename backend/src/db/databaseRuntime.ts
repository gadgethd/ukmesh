import pg from 'pg';
import { analyticsStatementTimeoutMs, loadDatabaseConfig, databaseUrlForRole } from '../platform/config/database.js';
import { databaseOperation, dbExecutionDuration, dbPoolWaitDuration, dbQueriesTotal, dbQueryDuration, updateDbPoolMetrics } from '../metrics/database.js';
import { createIngestDatabase, ingestPoolOptions } from './ingestExecution.js';
import type { DatabaseQueryFn } from './contracts.js';

/** Independent HTTP, analytics and ingest execution. Importing this definition reads no process configuration. */
export function createDatabaseRuntime(env: NodeJS.ProcessEnv) {
  const databaseConfig = loadDatabaseConfig(env);
  const { Pool } = pg;
  const pool = new Pool({
    connectionString: databaseUrlForRole('http', env),
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
    connectionString: databaseUrlForRole('worker', env),
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

  async function checkout(targetPool: pg.Pool, poolName: 'oltp' | 'analytics') {
    const started = process.hrtime.bigint();
    let outcome = 'success';
    try { return await targetPool.connect(); }
    catch (error) { outcome = 'failure'; throw error; }
    finally {
      dbPoolWaitDuration.observe({ pool: poolName, outcome }, Number(process.hrtime.bigint() - started) / 1e9);
      updateDbPoolMetrics(poolName, targetPool);
    }
  }

  async function queryPool<T extends pg.QueryResultRow = pg.QueryResultRow>(
    targetPool: pg.Pool,
    poolName: 'oltp' | 'analytics',
    querySpec: string | pg.QueryConfig<unknown[]>,
    params?: unknown[],
    signal?: AbortSignal,
  ): Promise<pg.QueryResult<T>> {
    const startedAt = process.hrtime.bigint();
    let outcome = 'success';
    let executionStarted: bigint | undefined;
    try {
      updateDbPoolMetrics(poolName, targetPool);
      const execute = (target: pg.PoolClient) => (
        typeof querySpec === 'string'
          ? target.query<T>(querySpec, params)
          : target.query<T>(querySpec)
      );
      signal?.throwIfAborted();
      const client = await checkout(targetPool, poolName);
      let destroyed = false;
      let abortedCleanup: (() => void) | undefined;
      const aborted = signal && new Promise<never>((_resolve, reject) => {
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
        signal?.throwIfAborted();
        executionStarted = process.hrtime.bigint();
        const result = aborted ? await Promise.race([execute(client), aborted]) : await execute(client);
        signal?.throwIfAborted();
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
      if (executionStarted !== undefined) dbExecutionDuration.observe({ pool: poolName, outcome,
        operation: databaseOperation(typeof querySpec === 'string' ? querySpec : querySpec.text) },
      Number(process.hrtime.bigint() - executionStarted) / 1e9);
      updateDbPoolMetrics(poolName, targetPool);
    }
  }

  async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
    text: string,
    params?: unknown[],
    signal?: AbortSignal,
  ): Promise<pg.QueryResult<T>> {
    return queryPool<T>(pool, 'oltp', text, params, signal);
  }

  async function withTransaction<T>(work: (transactionQuery: DatabaseQueryFn) => Promise<T>): Promise<T> {
    const client = await checkout(pool, 'oltp');
    const execute = async <R extends pg.QueryResultRow>(text: string, params?: unknown[]) => {
      const started = process.hrtime.bigint();
      let outcome = 'success';
      try { return await client.query<R>(text, params); }
      catch (error) { outcome = 'failure'; throw error; }
      finally { dbExecutionDuration.observe({ pool: 'oltp', operation: databaseOperation(text), outcome },
        Number(process.hrtime.bigint() - started) / 1e9); }
    };
    const transactionQuery: DatabaseQueryFn = async <R extends pg.QueryResultRow = pg.QueryResultRow>(
      text: string,
      params?: unknown[],
    ) => {
      const result = await execute<R>(text, params);
      return { rows: result.rows };
    };
    try {
      await execute('/* ukmesh:transaction */ BEGIN');
      const result = await work(transactionQuery);
      await execute('/* ukmesh:transaction */ COMMIT');
      return result;
    } catch (error) {
      await execute('/* ukmesh:transaction */ ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async function namedQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
    name: string,
    text: string,
    params?: unknown[],
    signal?: AbortSignal,
  ): Promise<pg.QueryResult<T>> {
    if (!/^[a-z0-9_-]{1,63}$/.test(name)) throw new Error('INVALID_PREPARED_STATEMENT_NAME');
    return queryPool<T>(pool, 'oltp', { name, text, values: params }, undefined, signal);
  }

  async function analyticsQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
    text: string,
    params?: unknown[],
    signal?: AbortSignal,
  ): Promise<pg.QueryResult<T>> {
    return queryPool<T>(analyticsPool, 'analytics', text, params, signal);
  }

  const ingestPool = new pg.Pool(ingestPoolOptions(env));
  ingestPool.on('error', error => console.error('[ingest-db] unexpected pool error', error.message));
  const ingest = createIngestDatabase(ingestPool);
  let closing: Promise<void> | undefined;
  return {
    pool, analyticsPool, ingestPool, query, namedQuery, analyticsQuery, withTransaction,
    ingestQuery: ingest.query,
    withIngestTransaction: ingest.withTransaction,
    close(): Promise<void> {
      return closing ??= Promise.all([pool.end(), analyticsPool.end(), ingestPool.end()]).then(() => undefined);
    },
  };
}
export type DatabaseRuntime = ReturnType<typeof createDatabaseRuntime>;
