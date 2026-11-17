import 'node:process';
import { once } from 'node:events';
import { initDb, closeDb, query } from '../db/runtime.js';
import { backfillHistoricalLinks } from '../ingest/backfillLinks.js';
import { queueLinkJob, closeQueuePublisher } from '../queue/publisher.js';
import { observeWorkerOutcome } from '../metrics/workers.js';
import { startWorkerMetrics } from './workerMetrics.js';
import { closeMetricsServer } from '../metricsServer.js';

async function main() {
  const metrics = startWorkerMetrics();
  if (!metrics.listening) await once(metrics, 'listening');
  await initDb();

  const { rows } = await query<{ count: string }>('SELECT COUNT(*) AS count FROM core.node_links');
  if (Number(rows[0]?.count ?? 0) > 0) {
    observeWorkerOutcome('link_backfill', 'backfill', 'skipped');
    console.log('[backfill] core.node_links already populated, skipping historical backfill');
    return;
  }

  console.log('[backfill] core.node_links empty, starting historical link backfill');
  await backfillHistoricalLinks(async (packetHash, rxNodeId, srcNodeId, path, hopCount, pathHashSizeBytes) => {
    const admission = await queueLinkJob(packetHash, rxNodeId, srcNodeId, path, hopCount, pathHashSizeBytes);
    if (admission && !['accepted', 'coalesced', 'duplicate'].includes(admission.status)) {
      throw new Error(`LINK_BACKFILL_ADMISSION_${admission.status.toUpperCase()}`);
    }
  });
  observeWorkerOutcome('link_backfill', 'backfill', 'success');
}

main()
  .catch((err) => {
    observeWorkerOutcome('link_backfill', 'backfill', 'failure');
    console.error('[backfill] fatal error:', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.all([closeQueuePublisher(), closeDb(), closeMetricsServer()]);
  });
