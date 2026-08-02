import 'node:process';
import { initDb } from '../db/index.js';
import { rebuildPathLearningModels } from '../path-learning/rebuild.js';
import { observeWorkerOutcome } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';

const REBUILD_INTERVAL_MS = 60 * 60 * 1000;

let isRunning = false;

async function rebuildOnce(tag: 'initial' | 'scheduled') {
  if (isRunning) {
    observeWorkerOutcome('path_learning', 'rebuild', 'skipped');
    console.warn(`[path-learning] ${tag} rebuild skipped; previous rebuild still running`);
    return;
  }
  isRunning = true;
  try {
    await rebuildPathLearningModels();
    observeWorkerOutcome('path_learning', 'rebuild', 'success');
  } catch (err) {
    observeWorkerOutcome('path_learning', 'rebuild', 'failure');
    console.error(`[path-learning] ${tag} rebuild failed`, (err as Error).message);
  } finally {
    isRunning = false;
  }
}

async function main() {
  startWorkerMetrics();
  await initDb();
  await rebuildOnce('initial');

  setInterval(() => {
    void rebuildOnce('scheduled');
  }, REBUILD_INTERVAL_MS);
}

main().catch((err) => {
  console.error('[path-learning] fatal startup error:', err);
  process.exit(1);
});
