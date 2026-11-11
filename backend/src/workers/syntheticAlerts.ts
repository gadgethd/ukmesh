import { mkdir, open, readFile, rename } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import type { DurableAlertQueue } from './alertDeliveryQueue.js';
import type { SyntheticCheckResult } from './syntheticPersistence.js';

type State = { failures: number; alerting: boolean };

/** Notification acceptance is durable and independent of the analytics DB. */
export async function createSyntheticAlertEvaluator(options: {
  queue: Pick<DurableAlertQueue, 'enqueue'>; statePath: string; threshold: number;
}) {
  let states: Record<string, State> = {};
  try { states = JSON.parse(await readFile(options.statePath, 'utf8')); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  return async (results: SyntheticCheckResult[]) => {
    for (const result of results) {
      const state = states[result.name] ?? { failures: 0, alerting: false };
      const failures = result.status === 'ok' ? 0 : state.failures + 1;
      const alerting = result.status === 'ok' ? false : (state.alerting || failures >= options.threshold);
      if (alerting !== state.alerting) {
        await options.queue.enqueue({ received_at: new Date().toISOString(), source: 'synthetic',
          status: alerting ? 'firing' : 'recovery', alert_names: [result.name],
          firing: alerting ? 1 : 0, resolved: alerting ? 0 : 1 });
      }
      // Update only after the outbox accepts the notification. A failed HTTP
      // send does not affect this state: the outbox retains and retries it.
      states[result.name] = { failures, alerting };
    }
    await mkdir(path.dirname(options.statePath), { recursive: true, mode: 0o700 });
    const temporary = `${options.statePath}.${randomUUID()}.tmp`;
    const handle = await open(temporary, 'wx', 0o600);
    try { await handle.writeFile(JSON.stringify(states)); await handle.sync(); }
    finally { await handle.close(); }
    await rename(temporary, options.statePath);
    const directory = await open(path.dirname(options.statePath), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  };
}
