import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import {
  AlertQueueFullError,
  DurableAlertQueue,
  type AlertReceipt,
} from './alertDeliveryQueue.js';

const receipt: AlertReceipt = {
  received_at: '2026-09-23T10:00:00.000Z',
  source: 'synthetic',
  status: 'firing',
  alert_names: ['DependencyDown'],
  firing: 1,
  resolved: 0,
};

async function withQueueDir(run: (directory: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(path.join(process.cwd(), '.test-alert-queue-'));
  try {
    await run(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

function queueOptions(directory: string, send: (alert: AlertReceipt) => Promise<void>, extra: {
  now?: () => number;
  maxAttempts?: number;
} = {}) {
  return {
    directory,
    maxItems: 5,
    maxAttempts: extra.maxAttempts ?? 3,
    backoffBaseMs: 1_000,
    backoffCapMs: 4_000,
    now: extra.now,
    send,
  };
}

test('pending alert survives restart and records durable last-success state', async () => {
  await withQueueDir(async (directory) => {
    const first = new DurableAlertQueue(queueOptions(directory, async () => {
      throw new Error('must not send before the queue worker starts');
    }));
    await first.initialize();
    await first.enqueue(receipt);
    assert.equal(first.metrics().pendingCount, 1);

    const forwarded: AlertReceipt[] = [];
    const restarted = new DurableAlertQueue(queueOptions(directory, async (alert) => {
      forwarded.push(alert);
    }));
    await restarted.initialize();
    assert.equal(restarted.metrics().pendingCount, 1);
    assert.equal(await restarted.processDue(), 1);
    assert.deepEqual(forwarded, [receipt]);
    assert.equal(restarted.metrics().pendingCount, 0);
    assert.ok(restarted.metrics().lastSuccessAt);

    const recoveredState = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await recoveredState.initialize();
    assert.equal(recoveredState.metrics().pendingCount, 0);
    assert.equal(recoveredState.metrics().lastSuccessAt, restarted.metrics().lastSuccessAt);
  });
});

test('bounded retries persist backoff and move exhausted alerts to dead letter', async () => {
  await withQueueDir(async (directory) => {
    let now = Date.parse('2026-09-23T10:00:00.000Z');
    let attempts = 0;
    const queue = new DurableAlertQueue(queueOptions(directory, async () => {
      attempts += 1;
      throw new Error('forward endpoint unavailable');
    }, { now: () => now, maxAttempts: 2 }));
    await queue.initialize();
    await queue.enqueue(receipt);
    now += 70_000;
    assert.equal(queue.metrics(now).oldestQueueAgeSeconds, 70);

    assert.equal(await queue.processDue(), 1);
    assert.equal(await queue.processDue(), 0, 'backoff prevents an immediate retry');
    now += 1_000;
    assert.equal(await queue.processDue(), 1);
    assert.equal(attempts, 2);
    assert.equal(queue.metrics(now).pendingCount, 0);
    assert.equal(queue.metrics(now).deadLetterCount, 1);
    assert.match(queue.metrics(now).lastError ?? '', /unavailable/);

    const restarted = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await restarted.initialize();
    assert.equal(restarted.metrics(now).deadLetterCount, 1);
  });
});

test('rejects new alerts once the durable queue reaches its configured bound', async () => {
  await withQueueDir(async (directory) => {
    const queue = new DurableAlertQueue({
      ...queueOptions(directory, async () => {}),
      maxItems: 1,
    });
    await queue.initialize();
    await queue.enqueue(receipt);
    await assert.rejects(queue.enqueue(receipt), AlertQueueFullError);
    assert.equal(queue.metrics().pendingCount, 1);
  });
});
