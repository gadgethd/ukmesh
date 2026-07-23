import 'node:process';
import { initDb } from '../db/index.js';
import { rebuildPathLearningModels } from '../path-learning/rebuild.js';

const REBUILD_INTERVAL_MS = 60 * 60 * 1000;

let isRunning = false;

async function rebuildOnce(tag: 'initial' | 'scheduled') {
  if (isRunning) {
    console.warn(`[path-learning] ${tag} rebuild skipped; previous rebuild still running`);
    return;
  }
  isRunning = true;
  try {
    await rebuildPathLearningModels();
  } catch (err) {
    console.error(`[path-learning] ${tag} rebuild failed`, (err as Error).message);
  } finally {
    isRunning = false;
  }
}

async function main() {
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
