import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import { databaseUrlForRole } from '../platform/config/database.js';
import { closeMetricsServer } from '../metricsServer.js';
import { regionRebuildSettings } from '../regions/rebuildSettings.js';
import { RegionSnapshotRefresher } from '../regions/snapshotRefresh.js';
import { runRegionSnapshotChild } from '../regions/snapshotChild.js';
import { startWorkerMetrics } from './workerMetrics.js';

async function main(): Promise<void> {
  const settings = regionRebuildSettings();
  const databaseUrl = databaseUrlForRole('worker');
  if (!databaseUrl) throw new Error('MESHCORE_WORKER_DATABASE_URL or DATABASE_URL is required');
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 1,
    application_name: 'meshcore-region-snapshot-poll', connectionTimeoutMillis: 5_000,
    statement_timeout: 5_000, query_timeout: 6_000 });
  pool.on('error', error => console.error('[region-snapshot] pool error:', error.message));
  const shutdown = new AbortController();
  process.once('SIGTERM', () => shutdown.abort());
  process.once('SIGINT', () => shutdown.abort());
  const refresher = new RegionSnapshotRefresher({ settings,
    query: (sql, params) => pool.query(sql, params),
    run: signal => runRegionSnapshotChild(signal, { timeoutMs: settings.runTimeoutMs }),
  });
  startWorkerMetrics();
  try {
    while (!shutdown.signal.aborted) {
      await refresher.runOnce(shutdown.signal);
      await delay(settings.pollMs, undefined, { signal: shutdown.signal });
    }
  } catch (error) {
    if (!shutdown.signal.aborted) throw error;
  } finally {
    await pool.end();
    await closeMetricsServer();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error('[region-snapshot] fatal:', error); process.exitCode = 1; });
}
