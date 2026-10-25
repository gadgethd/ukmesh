#!/usr/bin/env python3
"""Apply review fixes (ChatGPT points 2+3) to the perf worktree on the VPS.

Run from the worktree root: python3 apply-review-fixes.py
All edits are asserted before writing; a failed assert aborts with nothing written
for that file.
"""
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
 *  - the attempt fails    -> waiters re-arbitrate: the first waiter whose
 *    re-check finds no claimant returns 'claimed' and its caller registers
 *    its own attempt synchronously (before any other await), so the remaining
 *    waiters queue behind that successor. This closes the race where two
 *    waiters both fall through after a failure and persist the packet
 *    concurrently (double insert, since packets carry no unique constraint).
 */
export async function waitForInFlightPacket(
  registry: Map<string, Promise<void>>,
  dedupeKey: string,
): Promise<'claimed' | 'duplicate'> {
  for (;;) {
    const holder = registry.get(dedupeKey);
    if (!holder) return 'claimed';
    const persisted = await holder.then(() => true, () => false);
    if (persisted) return 'duplicate';
  }
}
'''
open(os.path.join(MQ, "dedupeRegistry.ts"), "w").write(helper)
print("A. dedupeRegistry.ts written")

# ---------------------------------------------------------------- B. client.ts
p = os.path.join(MQ, "client.ts")
s = open(p).read()

# B0. import
a0 = "import { createDurableMqttHandleMessage } from './durableHandler.js';"
assert s.count(a0) == 1, "B0 import anchor"
s = s.replace(a0, a0 + "\nimport { waitForInFlightPacket } from './dedupeRegistry.js';", 1)

# B1. claim loop replaces the single-shot if
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
new1 = """  // Wait for any concurrent copy of this packet already mid-persist. On its
  // success this copy is a duplicate; on its failure we re-arbitrate so that
  // exactly one waiter takes over the claim instead of racing it. The
  // successor claim below is registered synchronously (no await between this
  // return and inFlightPackets.set).
  if ((await waitForInFlightPacket(inFlightPackets, dedupeKey)) === 'duplicate') {
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

# B3. probe (and its comment) moves inside the claimed IIFE
old3 = """  const ingest = (async () => {
    try {
      await persistOnce();"""
assert s.count(old3) == 1, "B3 ingest head anchor"
new3 = """  const ingest = (async () => {
    // Claimed above. After a restart there is no in-memory dedup state to
    // match against: a QoS 1 redelivery whose original commit survived the
    // crash must not be persisted or counted a second time. Confirm against
    // the retained window before writing (inside the claim so the synchronous
    // registration right after this IIFE covers this path too).
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

# B5. bounded follow-up dispatch
old5 = """    // Best-effort live-map follow-ups run once the durable write is done and
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
assert s.count(old5) == 1, "B5 follow-up block anchor"
new5 = """    // Best-effort live-map follow-ups run once the durable write is done and
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
s = s.replace(old5, new5, 1)

# B6. metrics import additions
old6 = """import {
  boundedNetworkMetricLabel,
  mqttIngestAckDuration,
  mqttIngestActive,
  mqttIngestOutcomesTotal,
  mqttMessagesTotal,
} from '../metrics.js';"""
assert s.count(old6) == 1, "B6 metrics import anchor"
new6 = """import {
  boundedNetworkMetricLabel,
  mqttIngestAckDuration,
  mqttIngestActive,
  mqttIngestOutcomesTotal,
  mqttMessagesTotal,
  postPersistDroppedTotal,
  postPersistInFlightGauge,
} from '../metrics.js';"""
s = s.replace(old6, new6, 1)

open(p, "w").write(s)
print("B. client.ts patched")

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
print("C. metrics.ts appended")

# ---------------------------------------------------------------- D. tests
tests = '''import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForInFlightPacket } from './dedupeRegistry.js';

test('claims when no attempt is in flight', async () => {
  const registry = new Map<string, Promise<void>>();
  assert.equal(await waitForInFlightPacket(registry, 'k'), 'claimed');
});

test('resolves as duplicate when the in-flight attempt succeeds', async () => {
  const registry = new Map<string, Promise<void>>();
  registry.set('k', Promise.resolve());
  assert.equal(await waitForInFlightPacket(registry, 'k'), 'duplicate');
});

test('re-arbitrates after a failed attempt: exactly one successor claims', async () => {
  const registry = new Map<string, Promise<void>>();
  let failFirst!: (error: unknown) => void;
  const first = new Promise<void>((_resolve, reject) => { failFirst = reject; });
  registry.set('k', first);

  // Mirror the handler contract: after 'claimed' the caller registers its own
  // attempt synchronously, then resolves it.
  const contender = async (): Promise<'claimed' | 'duplicate'> => {
    const state = await waitForInFlightPacket(registry, 'k');
    if (state === 'duplicate') return 'duplicate';
    let resolveMine!: () => void;
    const mine = new Promise<void>((resolve) => { resolveMine = resolve; });
    registry.set('k', mine);
    queueMicrotask(resolveMine);
    await mine;
    return 'claimed';
  };

  const b = contender();
  const c = contender();
  // The failed attempt's cleanup (handler finally) removes it before waiters
  // re-check, exactly as in production.
  failFirst(new Error('db down'));
  registry.delete('k');

  const outcomes = (await Promise.all([b, c])).sort();
  assert.deepEqual(outcomes, ['claimed', 'duplicate']);
});

test('successive failures chain without double-claiming or losing the packet', async () => {
  const registry = new Map<string, Promise<void>>();

  interface Claim { resolve: () => void; reject: (e: unknown) => void; done: Promise<string>; }
  const contender = async (): Promise<'duplicate' | Claim> => {
    const state = await waitForInFlightPacket(registry, 'k');
    if (state === 'duplicate') return 'duplicate';
    let resolve!: () => void;
    let reject!: (e: unknown) => void;
    const mine = new Promise<void>((res, rej) => { resolve = res; reject = rej; });
    registry.set('k', mine);
    return {
      resolve,
      reject,
      done: mine.then(() => 'claimed-persisted', () => 'claimed-failed'),
    };
  };

  let failFirst!: (e: unknown) => void;
  registry.set('k', new Promise<void>((_res, rej) => { failFirst = rej; }));

  const b = contender();
  const c = contender();

  failFirst(new Error('first failure'));
  registry.delete('k');

  const bRes = await b;
  assert.notEqual(bRes, 'duplicate');
  const claimB = bRes as Claim;

  // c is now queued behind b's claim; b's claim also fails.
  claimB.reject(new Error('second failure'));
  registry.delete('k');

  const cRes = await c;
  assert.notEqual(cRes, 'duplicate');
  const claimC = cRes as Claim;

  // The third attempt succeeds; exactly one persist wins overall.
  claimC.resolve();
  assert.equal(await claimB.done, 'claimed-failed');
  assert.equal(await claimC.done, 'claimed-persisted');
});
'''
open(os.path.join(MQ, "dedupeRegistry.test.ts"), "w").write(tests)
print("D. dedupeRegistry.test.ts written")
print("ALL EDITS APPLIED")
