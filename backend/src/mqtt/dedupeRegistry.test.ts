import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForInFlightPacket } from './dedupeRegistry.js';

interface ClaimHandle {
  claim: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
}

function claimHandle(): ClaimHandle {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const claim = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  claim.catch(() => { /* observed by waiters or the test */ });
  return { claim, resolve, reject };
}

test('claims (and registers) when no attempt is in flight', async () => {
  const registry = new Map<string, Promise<void>>();
  const mine = claimHandle();
  assert.equal(await waitForInFlightPacket(registry, 'k', mine.claim), 'claimed');
  assert.equal(registry.get('k'), mine.claim);
});

test('resolves as duplicate when the in-flight attempt succeeds', async () => {
  const registry = new Map<string, Promise<void>>();
  const holder = claimHandle();
  holder.resolve();
  registry.set('k', holder.claim);
  const mine = claimHandle();
  assert.equal(await waitForInFlightPacket(registry, 'k', mine.claim), 'duplicate');
  assert.equal(registry.get('k'), holder.claim); // untouched
});

test('re-arbitrates after a failed attempt: exactly one successor claims', async () => {
  const registry = new Map<string, Promise<void>>();
  const first = claimHandle();
  registry.set('k', first.claim);

  const b = claimHandle();
  const c = claimHandle();
  const bResult = waitForInFlightPacket(registry, 'k', b.claim);
  const cResult = waitForInFlightPacket(registry, 'k', c.claim);

  // The failed attempt rejects; its cleanup (handler finally) removes it
  // before waiters re-check, exactly as in production.
  first.reject(new Error('db down'));
  registry.delete('k');

  assert.equal(await bResult, 'claimed'); // the successor claims...
  assert.equal(registry.get('k'), b.claim); // ...and registers its own attempt.
  b.resolve(); // successor's persist succeeds
  assert.equal(await cResult, 'duplicate'); // the other copy is a duplicate, not a second write.
});

test('successive failures chain without double-claiming or losing the packet', async () => {
  const registry = new Map<string, Promise<void>>();
  const first = claimHandle();
  registry.set('k', first.claim);

  const b = claimHandle();
  const bResult = waitForInFlightPacket(registry, 'k', b.claim);

  first.reject(new Error('first failure'));
  registry.delete('k');
  assert.equal(await bResult, 'claimed');

  // c starts waiting behind b's claim; b's attempt also fails.
  const c = claimHandle();
  const cResult = waitForInFlightPacket(registry, 'k', c.claim);
  b.reject(new Error('second failure'));
  registry.delete('k');
  assert.equal(await cResult, 'claimed');

  // The third attempt succeeds; exactly one persist wins overall.
  c.resolve();
  await c.claim;
});
