import type pg from 'pg';
import { dbPoolWaitDuration, updateDbPoolMetrics } from '../metrics/database.js';

/** Acquisition ends before statement execution; callers retain lease ownership. */
export async function checkoutDatabaseClient(
  pool: pg.Pool,
  poolName: 'oltp' | 'analytics' | 'ingest',
) {
  const started = process.hrtime.bigint();
  let outcome: 'success' | 'failure' = 'success';
  try { return await pool.connect(); }
  catch (error) { outcome = 'failure'; throw error; }
  finally {
    dbPoolWaitDuration.observe({ pool: poolName, outcome }, Number(process.hrtime.bigint() - started) / 1e9);
    updateDbPoolMetrics(poolName, pool);
  }
}
