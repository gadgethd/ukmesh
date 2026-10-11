#!/usr/bin/env python3
"""Review fixes v2 — the claim is registered INSIDE waitForInFlightPacket, in the
same microtask as the emptiness check, so exactly one successor can ever claim.

Run from the worktree root AFTER `git checkout -- backend/src/mqtt/client.ts
backend/src/metrics.ts` (v1 edits reverted)."""
import os

W = os.getcwd()
MQ = os.path.join(W, "backend/src/mqtt")
METRICS = os.path.join(W, "backend/src/metrics.ts")

# ---------------------------------------------------------------- A. helper module
helper = '''/**
 * Claim arbitration for the in-flight packet dedupe registry.
 *
 * Multiple copies of the same packet can arrive concurrently (broker
 * redeliveries, relay copies fanned across observers, several ingest
 * connections). Only one copy may be mid-persist at a time; everyone else
 * waits for its outcome:
 *
 *  - the attempt succeeds -> every waiter resolves as a duplicate (no write);
 *  - the attempt fails    -> waiters re-arbitrate. The re-check and the claim
 *    registration happen in the SAME microtask inside this function, so
 *    exactly one successor claims; every later waiter queues behind that
 *    successor rather than racing it. This closes the race where two waiters
 *    both fall through after a failure and persist the packet concurrently
 *    (double insert: packets carry no unique constraint).
 *
 * Contract: the caller passes the promise that will settle when its own
 * attempt settles (`claim`). On 'claimed', the caller MUST settle that
 * promise and remove it from the registry when done.
 */
export async function waitForInFlightPacket(
  registry: Map<string, Promise<void>>,
  dedupeKey: string,
  claim: Promise<void>,
): Promise<'claimed' | 'duplicate'> {
  for (;;) {
    const holder = registry.get(dedupeKey);
    if (!holder) {
      registry.set(dedupeKey, claim);
      return 'claimed';
    }
    const persisted = await holder.then(() => true, () => false);
    if (persisted) return 'duplicate';
  }
}
'''
open(os.path.join(MQ, "dedupeRegistry.ts"), "w").write(helper)
print("A. dedupeRegistry.ts written (v2)")

# ---------------------------------------------------------------- B. client.ts
p = os.path.join(MQ, "client.ts")
s = open(p).read()

# B0. import
a0 = "import { createDurableMqttHandleMessage } from './durableHandler.js';"
assert s.count(a0) == 1, "B0 import anchor"
s = s.replace(a0, a0 + "\nimport { waitForInFlightPacket } from './dedupeRegistry.js';", 1)

# B1. claim creation + arbitration replaces the single-shot if
old1 = """  const inFlight = inFlightPackets.get(dedupeKey);
  if (inFlight) {
    // The same packet is already being persisted by a concurrent ingest.
    // Wait for its outcome: success makes this copy a confirmed duplicate,
    // failure means nothing was committed and this copy persists below.
    const persisted = await inFlight.then(() => true, () => false);
    if (persisted) {
      markMqttPersistenceHealthy();
      mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
      return;
    }
  }"""
assert s.count(old1) == 1, "B1 in-flight block anchor"
new1 = """  // Arbitration against concurrent copies of this packet. The claim below is
  // registered inside waitForInFlightPacket in the same microtask as its
  // re-check, so after a failed predecessor exactly one waiter becomes the
  // successor and the rest queue behind it (never two concurrent retries).
  let claimResolve!: () => void;
  let claimReject!: (error: unknown) => void;
  const claim = new Promise<void>((resolve, reject) => {
    claimResolve = resolve;
    claimReject = reject;
  });
  claim.catch(() => { /* waiters observe failure via their own handlers */ });

  const claimState = await waitForInFlightPacket(inFlightPackets, dedupeKey, claim);
  if (claimState === 'duplicate') {
    claimResolve(); // never registered; settle for tidiness
    markMqttPersistenceHealthy();
    mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
    return;
  }"""
s = s.replace(old1, new1, 1)

# B2. remove the pre-claim redelivered probe
old2 = """  // After a restart there is no in-memory dedup state to match against: a
  // QoS 1 redelivery whose original commit survived the crash must not be
  // persisted or counted a second time. Confirm against the retained window.
  if (redelivered && await packetAlreadyPersisted(finalHash, observerKey, topic, network)) {
    markMqttPersistenceHealthy();
    mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
    return;
  }

"""
assert s.count(old2) == 1, "B2 probe block anchor"
s = s.replace(old2, "", 1)

# B3. probe moves inside the claimed IIFE
old3 = """  const ingest = (async () => {
    try {
      await persistOnce();"""
assert s.count(old3) == 1, "B3 ingest head anchor"
new3 = """  const ingest = (async () => {
    // Claimed above. After a restart there is no in-memory dedup state to
    // match against: a QoS 1 redelivery whose original commit survived the
    // crash must not be persisted or counted a second time. Confirm against
    // the retained window before writing (inside the claim so the registered
    // claim promise covers this path too).
    if (redelivered && await packetAlreadyPersisted(finalHash, observerKey, topic, network)) {
      markMqttPersistenceHealthy();
      mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
      return;
    }
    try {
      await persistOnce();"""
s = s.replace(old3, new3, 1)

# B4. module state for the follow-up bound
old4 = "const inFlightPackets = new Map<string, Promise<void>>();"
assert s.count(old4) == 1, "B4 registry decl anchor"
new4 = old4 + """
// Best-effort post-persist follow-ups are bounded: a stalled database must
// not grow the in-flight set without limit. Over the bound they are dropped
// (safe: everything here is explicitly best-effort) and counted.
const POST_PERSIST_MAX_INFLIGHT = 64;
let postPersistInFlight = 0;"""
s = s.replace(old4, new4, 1)

# B5. wire ingest settlement to the claim + cleanup keyed on the claim
old5 = """  inFlightPackets.set(dedupeKey, ingest);
  try {
    await ingest;
  } finally {
    if (inFlightPackets.get(dedupeKey) === ingest) inFlightPackets.delete(dedupeKey);
  }
}"""
assert s.count(old5) == 1, "B5 registration block anchor"
new5 = """  void ingest.then(
    () => claimResolve(),
    (err: unknown) => claimReject(err),
  );
  try {
    await ingest;
  } finally {
    if (inFlightPackets.get(dedupeKey) === claim) inFlightPackets.delete(dedupeKey);
  }
}"""
s = s.replace(old5, new5, 1)

# B6. bounded follow-up dispatch
old6 = """    // Best-effort live-map follow-ups run once the durable write is done and
    // outside the acknowledgement path: PUBACK is released as soon as the
    // write commits and these resolve asynchronously.
    void runPostPersistFollowUps({
      finalHash,
      decodedPathHashSizeBytes,
      path,
      decodedRouteType,
      network,
      resolvedPacketType,
    }).catch((err: unknown) => {
      console.error(
        '[mqtt] post-persist follow-up failed:',
        err instanceof Error ? err.message : String(err),
      );
    });"""
assert s.count(old6) == 1, "B6 follow-up block anchor"
new6 = """    // Best-effort live-map follow-ups run once the durable write is done and
    // outside the acknowledgement path: PUBACK is released as soon as the
    // write commits and these resolve asynchronously. The in-flight count is
    // bounded so a database stall cannot grow them without limit; over the
    // bound they are dropped (best-effort) and counted.
    if (postPersistInFlight >= POST_PERSIST_MAX_INFLIGHT) {
      postPersistDroppedTotal.inc();
    } else {
      postPersistInFlight++;
      postPersistInFlightGauge.set(postPersistInFlight);
      void runPostPersistFollowUps({
        finalHash,
        decodedPathHashSizeBytes,
        path,
        decodedRouteType,
        network,
        resolvedPacketType,
      }).catch((err: unknown) => {
        console.error(
          '[mqtt] post-persist follow-up failed:',
          err instanceof Error ? err.message : String(err),
        );
      }).finally(() => {
        postPersistInFlight--;
        postPersistInFlightGauge.set(postPersistInFlight);
      });
    }"""
s = s.replace(old6, new6, 1)

# B7. metrics import additions
old7 = """import {
  boundedNetworkMetricLabel,
  mqttIngestAckDuration,
  mqttIngestActive,
  mqttIngestOutcomesTotal,
  mqttMessagesTotal,
} from '../metrics.js';"""
assert s.count(old7) == 1, "B7 metrics import anchor"
new7 = """import {
  boundedNetworkMetricLabel,
  mqttIngestAckDuration,
  mqttIngestActive,
  mqttIngestOutcomesTotal,
  mqttMessagesTotal,
  postPersistDroppedTotal,
  postPersistInFlightGauge,
} from '../metrics.js';"""
s = s.replace(old7, new7, 1)

open(p, "w").write(s)
print("B. client.ts patched (v2)")

# ---------------------------------------------------------------- C. metrics.ts
m = open(METRICS).read()
addition = """
export const postPersistInFlightGauge = new Gauge({
  name: 'meshcore_mqtt_post_persist_inflight',
  help: 'Best-effort post-persist follow-ups currently in flight (bounded).',
});

export const postPersistDroppedTotal = new Counter({
  name: 'meshcore_mqtt_post_persist_dropped_total',
  help: 'Best-effort post-persist follow-ups dropped because the in-flight bound was reached.',
});
"""
assert "postPersistInFlightGauge" not in m, "C: metrics already patched?"
open(METRICS, "a").write(addition)
print("C. metrics.ts appended (v2)")

# ---------------------------------------------------------------- D. tests (v2 contract)
tests = '''import assert from 'node:assert/strict';
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

  const outcomes = (await Promise.all([bResult, cResult])).sort();
  assert.deepEqual(outcomes, ['claimed', 'duplicate']);
  // The successor's claim is registered; the packet is not lost.
  assert.equal(registry.get('k'), b.claim);
  b.resolve();
  await b.claim;
});

test('successive failures chain without double-claiming or losing the packet', async () => {
  const registry = new Map<string, Promise<void>>();
  const first = claimHandle();
  registry.set('k', first.claim);

  const b = claimHandle();
  const c = claimHandle();
  const bResult = waitForInFlightPacket(registry, 'k', b.claim);

  first.reject(new Error('first failure'));
  registry.delete('k');
  assert.equal(await bResult, 'claimed');

  // c starts waiting behind b; b's attempt also fails.
  const cResult = waitForInFlightPacket(registry, 'k', c.claim);
  b.reject(new Error('second failure'));
  registry.delete('k');
  assert.equal(await cResult, 'claimed');

  // The third attempt succeeds; exactly one persist wins overall.
  c.resolve();
  await c.claim;
});
'''
open(os.path.join(MQ, "dedupeRegistry.test.ts"), "w").write(tests)
print("D. dedupeRegistry.test.ts written (v2)")
print("ALL EDITS APPLIED (v2)")
