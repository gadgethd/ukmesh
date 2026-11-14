import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import {
  AlertQueueFullError,
  AlertRedriveError,
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

async function deadLetter(directory: string) {
  const initial = new DurableAlertQueue(queueOptions(directory, async () => { throw new Error('owned unavailable endpoint'); }, { maxAttempts: 1 }));
  await initial.initialize();
  await initial.enqueue(receipt);
  await initial.processDue();
  const name = (await readdir(directory)).find((file) => file.endsWith('.dead.json'))!;
  return { id: name.slice(0, -'.dead.json'.length), file: path.join(directory, name), bytes: await readFile(path.join(directory, name)) };
}

// These tests exercise persisted boundaries on real files. Only the specific
// failing filesystem operation is substituted; recovery always uses a new queue.
test('redrive keeps original history unresolved until confirmed durable delivery', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const requestId = randomUUID();
    const delivered: AlertReceipt[] = [];
    const queue = new DurableAlertQueue(queueOptions(directory, async (item) => { delivered.push(item); }));
    await queue.initialize();
    assert.deepEqual(await queue.redrive(original.id, requestId, 'endpoint recovered'), { state: 'queued' });
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    assert.equal(queue.metrics().resolvedDeadLetterCount, 0);
    const restarted = new DurableAlertQueue(queueOptions(directory, async (item) => { delivered.push(item); }));
    await restarted.initialize();
    await restarted.processDue();
    assert.deepEqual(delivered, [receipt]);
    assert.equal(restarted.metrics().deadLetterCount, 1);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 0);
    assert.equal(restarted.metrics().resolvedDeadLetterCount, 1);
    assert.deepEqual(await readFile(original.file), original.bytes);
    const resolution = JSON.parse(await readFile(path.join(directory, `${original.id}.resolved.json`), 'utf8'));
    assert.equal(resolution.originalSha256, createHash('sha256').update(original.bytes).digest('hex'));
    assert.equal(resolution.requestId, requestId);
    assert.equal(resolution.reason, 'endpoint recovered');
    assert.equal(resolution.attempts, 1);
    assert.deepEqual(await restarted.redrive(original.id, requestId, 'endpoint recovered'), { state: 'resolved' });
    await assert.rejects(restarted.redrive(original.id, randomUUID(), 'duplicate recovery'), { statusCode: 409 });
  });
});

test('concurrent redrive is idempotent and shares capacity with ordinary enqueue while sending', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sent!: () => void;
    let release!: () => void;
    const started = new Promise<void>((resolve) => { sent = resolve; });
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const queue = new DurableAlertQueue({ ...queueOptions(directory, async () => { sent(); await hold; }), maxItems: 1 });
    await queue.initialize();
    const requestId = randomUUID();
    const results = await Promise.all(Array.from({ length: 8 }, () => queue.redrive(original.id, requestId, 'recovered')));
    assert.ok(results.every((item) => item.state === 'queued'));
    assert.equal((await readdir(directory)).filter((file) => file.endsWith('.redrive.pending.json')).length, 1);
    const processing = queue.processDue();
    await started;
    try {
      assert.deepEqual(await queue.redrive(original.id, requestId, 'recovered'), { state: 'queued' });
      await assert.rejects(queue.redrive(original.id, randomUUID(), 'competing request'), { statusCode: 409 });
      await assert.rejects(queue.enqueue(receipt), AlertQueueFullError);
      assert.equal(queue.metrics().pendingCount, 1);
      assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
      assert.equal(await queue.processDue(), 0);
    } finally { release(); await processing; }
    await queue.close();
    await assert.rejects(queue.enqueue(receipt), { statusCode: 503 });
    await assert.rejects(queue.redrive(original.id, requestId, 'after close'), { statusCode: 503 });
  });
});

test('full queue rejects a redrive without persisting another intent', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const queue = new DurableAlertQueue({ ...queueOptions(directory, async () => {}), maxItems: 1 });
    await queue.initialize();
    await queue.enqueue(receipt);
    await assert.rejects(queue.redrive(original.id, randomUUID(), 'recovered'), AlertQueueFullError);
    assert.equal((await readdir(directory)).some((file) => file.endsWith('.redrive.pending.json')), false);
  });
});

test('exhausted redrive remains unresolved and the same request cannot reset its budget after restart', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let now = Date.now();
    let sends = 0;
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; throw new Error('still unavailable'); }, { now: () => now, maxAttempts: 2 }));
    await queue.initialize();
    const requestId = randomUUID();
    await queue.redrive(original.id, requestId, 'explicit recovery attempt');
    await queue.processDue();
    assert.equal(await queue.processDue(), 0);
    now += 1_000;
    await queue.processDue();
    assert.equal(sends, 2);
    assert.equal(queue.metrics().deadLetterCount, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    assert.equal(queue.metrics().pendingCount, 0);
    const failedFile = path.join(directory, `${original.id}.redrive.${requestId}.failed.json`);
    const failedBytes = await readFile(failedFile);
    assert.equal(JSON.parse(failedBytes.toString()).attempts, 2);
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }));
    await restarted.initialize();
    await assert.rejects(restarted.redrive(original.id, requestId, 'reset attempt'), { statusCode: 409 });
    await restarted.redrive(original.id, randomUUID(), 'second explicit attempt');
    await restarted.processDue();
    assert.equal(sends, 3);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 0);
    assert.deepEqual(await readFile(failedFile), failedBytes);
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('restart never exceeds a redrive attempt budget reserved before an ambiguous crash', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const requestId = randomUUID();
    const queue = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await queue.initialize();
    await queue.redrive(original.id, requestId, 'recovered');
    const file = path.join(directory, `${original.id}.redrive.pending.json`);
    const reserved = JSON.parse(await readFile(file, 'utf8'));
    reserved.attempts = 3;
    await writeFile(file, JSON.stringify(reserved));
    let sends = 0;
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }));
    await restarted.initialize();
    await restarted.processDue();
    assert.equal(sends, 0);
    assert.equal(restarted.metrics().pendingCount, 0);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 1);
    await assert.rejects(restarted.redrive(original.id, requestId, 'reset'), { statusCode: 409 });
    assert.equal(JSON.parse(await readFile(path.join(directory, `${original.id}.redrive.${requestId}.failed.json`), 'utf8')).attempts, 3);
  });
});

type QueueFiles = {
  removeFile(file: string): Promise<void>;
  writeImmutable(file: string, value: unknown): Promise<void>;
  syncDirectory(): Promise<void>;
};

test('restart reconciles durable resolution before pending unlink without resending', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sends = 0;
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }));
    await queue.initialize();
    const requestId = randomUUID();
    await queue.redrive(original.id, requestId, 'recovered');
    const files = queue as unknown as QueueFiles;
    const remove = files.removeFile.bind(files);
    t.mock.method(files, 'removeFile', async (file: string) => {
      if (file.endsWith('.redrive.pending.json')) throw new Error('owned unlink failure');
      await remove(file);
    });
    await queue.processDue();
    assert.equal(sends, 1);
    assert.equal(queue.metrics().resolvedDeadLetterCount, 1);
    assert.equal(queue.metrics().pendingCount, 1);
    assert.match(queue.metrics().lastError!, /unlink failure/);
    const resolutionBytes = await readFile(path.join(directory, `${original.id}.resolved.json`));
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }));
    await restarted.initialize();
    assert.equal(await restarted.processDue(), 0);
    assert.equal(sends, 1);
    assert.equal(restarted.metrics().pendingCount, 0);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 0);
    assert.ok(restarted.metrics().lastSuccessAt);
    assert.deepEqual(await readFile(path.join(directory, `${original.id}.resolved.json`)), resolutionBytes);
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('failed resolution persistence stays unresolved and restart retries at least once within budget', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sends = 0;
    let now = Date.now();
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }, { now: () => now }));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    const files = queue as unknown as QueueFiles;
    const write = files.writeImmutable.bind(files);
    t.mock.method(files, 'writeImmutable', async (file: string, value: unknown) => {
      if (file.endsWith('.resolved.json')) throw new Error('owned resolution persistence failure');
      await write(file, value);
    });
    await queue.processDue();
    assert.equal(sends, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    assert.equal(queue.metrics().resolvedDeadLetterCount, 0);
    await assert.rejects(readFile(path.join(directory, `${original.id}.resolved.json`)), { code: 'ENOENT' });
    now += 1_000;
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }, { now: () => now }));
    await restarted.initialize();
    await restarted.processDue();
    assert.equal(sends, 2);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 0);
  });
});

test('visible resolution with failed directory sync is reconciled before any further send', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sends = 0;
    let now = Date.now();
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }, { now: () => now }));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    const files = queue as unknown as QueueFiles;
    const sync = files.syncDirectory.bind(files);
    let injected = false;
    t.mock.method(files, 'syncDirectory', async () => {
      if (!injected && (await readdir(directory)).some((file) => file.endsWith('.resolved.json'))) {
        injected = true;
        throw new Error('owned resolution directory sync failure');
      }
      await sync();
    });
    await queue.processDue();
    assert.equal(injected, true);
    assert.equal(sends, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    now += 1_000;
    await queue.processDue();
    assert.equal(sends, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 0);
    assert.equal(queue.metrics().pendingCount, 0);
  });
});

test('restart cleans an exhausted replay after failure-audit persistence but before pending unlink', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sends = 0;
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; throw new Error('still down'); }, { maxAttempts: 1 }));
    await queue.initialize();
    const requestId = randomUUID();
    await queue.redrive(original.id, requestId, 'recovered');
    const files = queue as unknown as QueueFiles;
    t.mock.method(files, 'removeFile', async () => { throw new Error('owned unlink failure'); });
    await assert.rejects(queue.processDue(), /unlink failure/);
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }));
    await restarted.initialize();
    assert.equal(await restarted.processDue(), 0);
    assert.equal(sends, 1);
    assert.equal(restarted.metrics().deadLetterCount, 1);
    assert.equal(restarted.metrics().unresolvedDeadLetterCount, 1);
    await assert.rejects(restarted.redrive(original.id, requestId, 'reset'), { statusCode: 409 });
  });
});

test('invalid IDs, missing records and symlinked originals cannot escape the queue', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const queue = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await queue.initialize();
    const before = await readdir(directory);
    for (const id of ['../outside', original.id.toUpperCase(), `${original.id}/../../outside`]) {
      await assert.rejects(queue.redrive(id, randomUUID(), 'recovered'), { statusCode: 400 });
    }
    await assert.rejects(queue.redrive(randomUUID(), randomUUID(), 'missing'), { statusCode: 404 });
    await assert.rejects(queue.redrive(original.id, '../request', 'recovered'), AlertRedriveError);
    await assert.rejects(queue.redrive(original.id, randomUUID(), '\n'), { statusCode: 400 });
    assert.deepEqual(await readdir(directory), before);
    const alias = randomUUID();
    await symlink(original.file, path.join(directory, `${alias}.dead.json`));
    await assert.rejects(queue.redrive(alias, randomUUID(), 'recovered'), { code: 'ELOOP' });
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('malformed, hash-mismatched or symlinked resolution fails closed on restart', async () => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const queue = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    await queue.processDue();
    const file = path.join(directory, `${original.id}.resolved.json`);
    const valid = await readFile(file);
    for (const content of ['{}', JSON.stringify({ ...JSON.parse(valid.toString()), originalSha256: '0'.repeat(64) })]) {
      await writeFile(file, content);
      const restarted = new DurableAlertQueue(queueOptions(directory, async () => { assert.fail('must not send'); }));
      await assert.rejects(restarted.initialize());
    }
    await rm(file);
    await symlink(original.file, file);
    const restarted = new DurableAlertQueue(queueOptions(directory, async () => { assert.fail('must not send'); }));
    await assert.rejects(restarted.initialize(), { code: 'ELOOP' });
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('uncertain intent fsync retains the ID and capacity and an idempotent retry confirms durability', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const queue = new DurableAlertQueue({ ...queueOptions(directory, async () => {}), maxItems: 1 });
    await queue.initialize();
    const files = queue as unknown as QueueFiles;
    const sync = files.syncDirectory.bind(files);
    let fail = true;
    t.mock.method(files, 'syncDirectory', async () => {
      if (fail) { fail = false; throw new Error('owned admission fsync failure'); }
      await sync();
    });
    const requestId = randomUUID();
    await assert.rejects(queue.redrive(original.id, requestId, 'recovered'), /admission fsync failure/);
    assert.equal(queue.metrics().pendingCount, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    await assert.rejects(queue.enqueue(receipt), AlertQueueFullError);
    assert.deepEqual(await queue.redrive(original.id, requestId, 'recovered'), { state: 'queued' });
    await queue.processDue();
    assert.equal(queue.metrics().resolvedDeadLetterCount, 1);
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('queue-state failure after resolution cannot cause a second send on retry', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let sends = 0;
    let now = Date.now();
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }, { now: () => now }));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    const files = queue as unknown as { writeAtomic(file: string, value: unknown, exclusive?: boolean): Promise<void> };
    const write = files.writeAtomic.bind(files);
    let injected = false;
    t.mock.method(files, 'writeAtomic', async (file: string, value: unknown, exclusive?: boolean) => {
      if (file.endsWith('queue-state.json') && !injected) { injected = true; throw new Error('owned metadata failure'); }
      await write(file, value, exclusive);
    });
    await queue.processDue();
    assert.equal(sends, 1);
    assert.equal(queue.metrics().resolvedDeadLetterCount, 1);
    assert.equal(queue.metrics().pendingCount, 1);
    now += 1_000;
    await queue.processDue();
    assert.equal(sends, 1);
    assert.equal(queue.metrics().pendingCount, 0);
    assert.equal(queue.metrics().lastError, null);
  });
});

test('resolution must match the complete pending request audit after restart', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    const queue = new DurableAlertQueue(queueOptions(directory, async () => {}));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    t.mock.method(queue as unknown as QueueFiles, 'removeFile', async () => { throw new Error('owned unlink failure'); });
    await queue.processDue();
    const file = path.join(directory, `${original.id}.resolved.json`);
    const valid = JSON.parse(await readFile(file, 'utf8'));
    for (const delta of [{ reason: 'different reason' }, { requestedAt: '2025-01-01T00:00:00.000Z' }, { attempts: 2 }]) {
      await writeFile(file, JSON.stringify({ ...valid, ...delta }));
      const restarted = new DurableAlertQueue(queueOptions(directory, async () => { assert.fail('must not resend'); }));
      await assert.rejects(restarted.initialize(), /resolution does not match pending redrive/);
    }
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});

test('exclusive resolution publication failure retains pending work and never overwrites the obstruction', async (t) => {
  await withQueueDir(async (directory) => {
    const original = await deadLetter(directory);
    let now = Date.now();
    let sends = 0;
    const queue = new DurableAlertQueue(queueOptions(directory, async () => { sends += 1; }, { now: () => now }));
    await queue.initialize();
    await queue.redrive(original.id, randomUUID(), 'recovered');
    const files = queue as unknown as { writeAtomic(file: string, value: unknown, exclusive?: boolean): Promise<void> };
    const write = files.writeAtomic.bind(files);
    let injected = false;
    t.mock.method(files, 'writeAtomic', async (file: string, value: unknown, exclusive?: boolean) => {
      if (exclusive && file.endsWith('.resolved.json') && !injected) {
        injected = true;
        await mkdir(file); // Actual link(2) must fail without replacing this path.
      }
      await write(file, value, exclusive);
    });
    await queue.processDue();
    assert.equal(injected, true);
    assert.equal(sends, 1);
    assert.equal(queue.metrics().pendingCount, 1);
    assert.equal(queue.metrics().unresolvedDeadLetterCount, 1);
    const file = path.join(directory, `${original.id}.resolved.json`);
    assert.deepEqual(await readdir(file), []);
    assert.equal((await readdir(directory)).some((name) => name.endsWith('.tmp')), false);
    now += 1_000;
    await assert.rejects(queue.processDue(), /regular file/);
    assert.equal(sends, 1, 'an invalid resolution path fails closed');
    await rm(file, { recursive: true });
    await queue.processDue();
    assert.equal(sends, 2, 'ambiguous send is retried within the reserved budget after storage repair');
    assert.equal(queue.metrics().resolvedDeadLetterCount, 1);
    assert.deepEqual(await readFile(original.file), original.bytes);
  });
});
