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

  const scheduleNext = () => {
    setTimeout(() => {
      void captureOnce('scheduled').finally(scheduleNext);
    }, SNAPSHOT_INTERVAL_MS);
  };
  scheduleNext();
}

main().catch((err) => {
  console.error('[health] fatal startup error:', err);
  process.exit(1);
});
