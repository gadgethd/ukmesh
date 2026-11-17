import assert from 'node:assert/strict';
import test from 'node:test';
import { createReadBatcher } from './readBatch.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}

test('one pending cohort reads once, copies every caller and never caches completed data', async t => {
  let calls = 0, copies = 0;
  const batch = createReadBatcher<{ nested: { count: number }; at: Date }>(value => {
    copies++;
    return structuredClone(value);
  });
  t.after(() => batch.close());
  const load = async () => ({ nested: { count: ++calls }, at: new Date('2026-01-01T00:00:00Z') });
  const requests = Array.from({ length: 8 }, () => batch.run('same', load));
  assert.equal(calls, 0, 'no body read starts before dispatch');
  const results = await Promise.all(requests);
  assert.equal(calls, 1);
  assert.equal(copies, 8, 'the first caller also receives a copy');
  results[0]!.nested.count = 99;
  results[0]!.at.setUTCFullYear(2030);
  assert.equal(results[1]!.nested.count, 1);
  assert.equal(results[1]!.at.getUTCFullYear(), 2026);
  assert.equal((await batch.run('same', load)).nested.count, 2);
});

test('a later arrival after dispatch starts a fresh read while the first SQL is unresolved', { timeout: 1_000 }, async t => {
  const held = deferred<number>();
  const loaderStarted = deferred<void>();
  const batch = createReadBatcher<number>(value => value);
  t.after(() => { held.resolve(1); batch.close(); });
  let calls = 0;
  const first = batch.run('same', () => { calls++; loaderStarted.resolve(); return held.promise; });
  await loaderStarted.promise;
  assert.equal(calls, 1);
  const second = await batch.run('same', async () => { calls++; return 2; });
  assert.equal(second, 2);
  assert.equal(calls, 2);
  held.resolve(1);
  assert.equal(await first, 1);
});

test('dispatch removes the old entry before a reentrant loader and never deletes its successor', { timeout: 1_000 }, async t => {
  const batch = createReadBatcher<number>(value => value);
  t.after(() => batch.close());
  let nested: Promise<number> | undefined;
  const first = batch.run('same', async () => {
    nested = batch.run('same', async () => 2);
    return 1;
  });
  assert.equal(await first, 1);
  assert.ok(nested);
  assert.equal(await nested, 2);
});

test('64pending keys retain joins and overflow executes normally without admission failure', async t => {
  const batch = createReadBatcher<{ value: number }>(structuredClone);
  t.after(() => batch.close());
  let scheduled = 0, ordinary = 0;
  const requests = Array.from({ length: 64 }, (_, value) => batch.run(String(value), async () => {
    scheduled++;
    return { value };
  }));
  const joined = batch.run('0', async () => { throw new Error('existing pending load must win'); });
  const overflow = batch.run('overflow', async () => { ordinary++; return { value: 64 }; });
  assert.equal(ordinary, 1);
  assert.equal(scheduled, 0);
  assert.deepEqual(await overflow, { value: 64 });
  const error = new Error('ordinary overflow failure');
  await assert.rejects(batch.run('overflow failure', () => { throw error; }), actual => actual === error);
  const results = await Promise.all(requests);
  assert.equal(scheduled, 64);
  assert.deepEqual(results.map(row => row.value), Array.from({ length: 64 }, (_, i) => i));
  assert.deepEqual(await joined, { value: 0 });
  assert.notEqual(await joined, results[0]);
});

test('synchronous and asynchronous load failures settle a cohort and the next request can read again', async t => {
  for (const asynchronous of [false, true]) {
    const batch = createReadBatcher<number>(value => value);
    t.after(() => batch.close());
    const error = new Error(asynchronous ? 'async failure' : 'sync failure');
    let calls = 0;
    const load = () => { calls++; if (asynchronous) return Promise.reject(error); throw error; };
    const results = await Promise.allSettled([batch.run('same', load), batch.run('same', load)]);
    assert.equal(calls, 1);
    assert.ok(results.every(result => result.status === 'rejected' && result.reason === error));
    assert.equal(await batch.run('same', async () => 42), 42);
  }
});

test('copy errors are caller-specific and batch state never crosses an instance', async t => {
  let copies = 0;
  const error = new Error('first copy failed');
  const first = createReadBatcher<number>(value => { if (++copies === 1) throw error; return value; });
  const second = createReadBatcher<number>(value => value);
  t.after(() => { first.close(); second.close(); });
  const failed = first.run('same', async () => 1);
  const joined = first.run('same', async () => 99);
  const independent = second.run('same', async () => 2);
  const results = await Promise.allSettled([failed, joined, independent]);
  assert.deepEqual(results, [{ status: 'rejected', reason: error },
    { status: 'fulfilled', value: 1 }, { status: 'fulfilled', value: 2 }]);
  assert.equal(copies, 2);
});

test('close settles queued and future requests and cancels their dispatch handles', async t => {
  const batch = createReadBatcher<number>(value => value);
  const cleared = t.mock.method(globalThis, 'clearTimeout');
  let calls = 0;
  const load = async () => ++calls;
  const results = Promise.allSettled([batch.run('a', load), batch.run('a', load), batch.run('b', load)]);
  batch.close();
  batch.close();
  assert.equal(cleared.mock.callCount(), 2, 'each distinct pending timer is cancelled exactly once');
  assert.notEqual(cleared.mock.calls[0]!.arguments[0], cleared.mock.calls[1]!.arguments[0]);
  assert.ok((await results).every(result => result.status === 'rejected' && result.reason.message === 'READ_BATCH_CLOSED'));
  await assert.rejects(batch.run('a', load), /READ_BATCH_CLOSED/);
  assert.equal(calls, 0);
});

test('close does not cancel already-dispatched work owned by its callers', async t => {
  const held = deferred<number>();
  const loaderStarted = deferred<void>();
  const batch = createReadBatcher<number>(value => value);
  t.after(() => { held.resolve(7); batch.close(); });
  const first = batch.run('same', () => { loaderStarted.resolve(); return held.promise; });
  await loaderStarted.promise;
  batch.close();
  held.resolve(7);
  assert.equal(await first, 7);
  await assert.rejects(batch.run('same', async () => 9), /READ_BATCH_CLOSED/);
});
