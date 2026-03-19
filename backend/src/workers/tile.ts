import 'node:process';
import { initDb } from '../db/index.js';
import { startTileWorker } from '../tiles/worker.js';

async function main() {
  await initDb();
  startTileWorker();
}

main().catch((err) => {
  console.error('[tile-worker] fatal startup error:', err);
  process.exit(1);
});
