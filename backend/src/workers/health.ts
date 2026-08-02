import 'node:process';
import { initDb } from '../db/index.js';
import { captureWorkerHealthSnapshot } from '../health/status.js';
import { cleanupStaleMqttObservers } from '../maintenance/staleMqttObservers.js';
import { pollOwnerAlertRules } from '../owner/alertRules.js';
import { observeWorkerOutcome } from '../metrics.js';
import { startWorkerMetrics } from './workerMetrics.js';

const SNAPSHOT_INTERVAL_MS = 60 * 1000;

function boundedEnvNumber(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

const STALE_OBSERVER_CLEANUP_INTERVAL_MS = boundedEnvNumber(
  'STALE_MQTT_OBSERVER_CLEANUP_INTERVAL_MS',
  6 * 60 * 60 * 1000,
  60 * 60 * 1000,
  24 * 60 * 60 * 1000,
);
const STALE_OBSERVER_CLEANUP_DAYS = boundedEnvNumber(
  'STALE_MQTT_OBSERVER_CLEANUP_DAYS',
  30,
  30,
  365,
);

async function captureOnce(tag: 'initial' | 'scheduled') {
  try {
    await Promise.all([captureWorkerHealthSnapshot(), pollOwnerAlertRules()]);
    observeWorkerOutcome('health', 'snapshot', 'success');
  } catch (err) {
    observeWorkerOutcome('health', 'snapshot', 'failure');
    console.error(`[health] ${tag} snapshot failed`, (err as Error).message);
  }
}

async function cleanupStaleObservers(tag: 'initial' | 'scheduled') {
  try {
    const result = await cleanupStaleMqttObservers({ thresholdDays: STALE_OBSERVER_CLEANUP_DAYS });
    if (result.candidates > 0) {
      console.log(
        `[health] ${tag} stale MQTT observer cleanup archived batch=${result.batchId} ` +
        `nodes=${result.nodes} observerSightings=${result.observerSightings} networkSightings=${result.networkSightings}`,
      );
    }
    observeWorkerOutcome('health', 'cleanup', 'success');
  } catch (err) {
    observeWorkerOutcome('health', 'cleanup', 'failure');
    console.error(`[health] ${tag} stale MQTT observer cleanup failed`, (err as Error).message);
  }
}

async function main() {
  startWorkerMetrics();
  await initDb();
  await captureOnce('initial');
  await cleanupStaleObservers('initial');

  const scheduleNext = () => {
    setTimeout(() => {
      void captureOnce('scheduled').finally(scheduleNext);
    }, SNAPSHOT_INTERVAL_MS);
  };
  scheduleNext();

  const scheduleCleanup = () => {
    setTimeout(() => {
      void cleanupStaleObservers('scheduled').finally(scheduleCleanup);
    }, STALE_OBSERVER_CLEANUP_INTERVAL_MS);
  };
  scheduleCleanup();
}

main().catch((err) => {
  console.error('[health] fatal startup error:', err);
  process.exit(1);
});
