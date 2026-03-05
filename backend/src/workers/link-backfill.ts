import 'node:process';
import { initDb, pool, query } from '../db/index.js';
import { backfillHistoricalLinks } from '../mqtt/client.js';
import { queueLinkJob, closeQueuePublisher } from '../queue/publisher.js';

async function main() {
  await initDb();

  const { rows } = await query<{ count: string }>('SELECT COUNT(*) AS count FROM node_links');
  if (Number(rows[0]?.count ?? 0) > 0) {
    console.log('[backfill] node_links already populated, skipping historical backfill');
    return;
  }

  console.log('[backfill] node_links empty, starting historical link backfill');
  await backfillHistoricalLinks((rxNodeId, srcNodeId, path, hopCount) => {
    queueLinkJob(rxNodeId, srcNodeId, path, hopCount);
  });
}

main()
  .catch((err) => {
    console.error('[backfill] fatal error:', err);
    process.exit(1);
  })
  .finally(async () => {
    await closeQueuePublisher();
    await pool.end();
    process.exit(0);
  });
