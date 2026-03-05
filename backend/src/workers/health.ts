import 'node:process';
import { initDb } from '../db/index.js';
import { captureWorkerHealthSnapshot } from '../health/status.js';

const SNAPSHOT_INTERVAL_MS = 60 * 1000;

async function captureOnce(tag: 'initial' | 'scheduled') {
  try {
    await captureWorkerHealthSnapshot();
  } catch (err) {
    console.error(`[health] ${tag} snapshot failed`, (err as Error).message);
  }
}

async function main() {
  await initDb();
  await captureOnce('initial');

  setInterval(() => {
    void captureOnce('scheduled');
  }, SNAPSHOT_INTERVAL_MS);
}

main().catch((err) => {
  console.error('[health] fatal startup error:', err);
  process.exit(1);
});
