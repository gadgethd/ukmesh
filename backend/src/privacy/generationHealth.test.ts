import assert from 'node:assert/strict';
import test from 'node:test';
import { privacyGenerationHealthy } from '../metrics.js';
import {
  createPrivacyGenerationReconciler,
  evaluatePrivacyGenerations,
  PRIVACY_GENERATION_TIMEOUT_MS,
  type PrivacyGenerationHealth,
  type PrivacyGenerationSnapshot,
} from './generationHealth.js';

const settled: PrivacyGenerationSnapshot = {
  desiredGeneration: '42', materializedGeneration: '42', appliedGeneration: '42', queueActive: false,
};

test('no generation diff is healthy; ahead, behind and missing artefacts fail closed', () => {
  assert.equal(evaluatePrivacyGenerations(settled).healthy, true);
  const cases: [Partial<PrivacyGenerationSnapshot>, PrivacyGenerationHealth['reason']][] = [
    [{ desiredGeneration: '43', appliedGeneration: null }, 'desired_ahead'],
    [{ materializedGeneration: '43', appliedGeneration: null }, 'materialized_ahead'],
    [{ appliedGeneration: '43' }, 'applied_ahead'],
    [{ appliedGeneration: '41' }, 'applied_behind'],
    [{ desiredGeneration: null }, 'desired_missing'],
    [{ materializedGeneration: null }, 'materialized_missing'],
    [{ appliedGeneration: null }, 'applied_missing'],
    [{ queueActive: true, appliedGeneration: null }, 'queue_active'],
    [{ queueActive: null }, 'queue_active'],
  ];
  for (const [override, reason] of cases) {
    const result = evaluatePrivacyGenerations({ ...settled, ...override });
    assert.equal(result.healthy, false, reason);
    assert.equal(result.reason, reason);
  }
});

test('generation comparisons preserve large integer precision and reject unsupported values', () => {
  assert.equal(evaluatePrivacyGenerations({
    desiredGeneration: '9007199254740991', materializedGeneration: '9007199254740990',
    appliedGeneration: null, queueActive: false,
  }).reason, 'desired_ahead');
  for (const value of ['0', '-1', '1e2', 'NaN', '', '9007199254740992']) {
    for (const field of ['desiredGeneration', 'materializedGeneration', 'appliedGeneration'] as const) {
      assert.equal(evaluatePrivacyGenerations({ ...settled, [field]: value }).reason, 'invalid_generation');
    }
  }
});

test('healthy, desired-ahead, applied-ahead and missing artefact observations publish one matching metric and log', async (t) => {
  const values: number[] = [];
  const logs: string[] = [];
  t.mock.method(privacyGenerationHealthy, 'set', (value: number) => values.push(value));
  t.mock.method(console, 'log', (line: string) => logs.push(line));
  t.mock.method(console, 'warn', (line: string) => logs.push(line));
  for (const state of [
    settled,
    { ...settled, desiredGeneration: '43', appliedGeneration: null },
    // Defensive check for an inconsistent admission result. The current SQL
    // never admits an ahead certificate; its real DB case is covered too.
    { ...settled, appliedGeneration: '43' },
    { ...settled, materializedGeneration: null, appliedGeneration: null },
  ]) {
    const reconcile = createPrivacyGenerationReconciler(async () => ({ rows: [state] }));
    await reconcile();
  }
  assert.deepEqual(values, [1, 0, 0, 0]);
  assert.deepEqual(logs.map((line) => JSON.parse(line).reason),
    ['healthy', 'desired_ahead', 'applied_ahead', 'materialized_missing']);
  assert.deepEqual(logs.map((line) => Number(JSON.parse(line).healthy)), values);
});

test('concurrent health/scrape observations share one read and one structured publication', async () => {
  let reads = 0;
  let complete!: (value: { rows: PrivacyGenerationSnapshot[] }) => void;
  const reports: PrivacyGenerationHealth[] = [];
  const reconcile = createPrivacyGenerationReconciler(async () => {
    reads += 1;
    return new Promise((resolve) => { complete = resolve; });
  }, (state) => reports.push(state));
  const first = reconcile();
  assert.equal(reconcile(), first);
  await Promise.resolve();
  complete({ rows: [settled] });
  assert.equal((await first).healthy, true);
  assert.equal(reads, 1);
  assert.equal(reports.length, 1);
  await new Promise((resolve) => setImmediate(resolve));
  const next = reconcile();
  await Promise.resolve();
  complete({ rows: [{ ...settled, desiredGeneration: '43', appliedGeneration: null }] });
  assert.equal((await next).reason, 'desired_ahead');
  assert.equal(reads, 2);
});

test('read errors and missing query results replace a healthy observation without exposing errors', async (t) => {
  let failure = false;
  const logs: string[] = [];
  t.mock.method(console, 'log', (line: string) => logs.push(line));
  t.mock.method(console, 'warn', (line: string) => logs.push(line));
  const reconcile = createPrivacyGenerationReconciler(async () => {
    if (failure) throw new Error('postgresql://secret:password@private-host packet contents');
    return { rows: [settled] };
  });
  assert.equal((await reconcile()).healthy, true);
  await new Promise((resolve) => setImmediate(resolve));
  failure = true;
  assert.equal((await reconcile()).reason, 'read_failed');
  assert.deepEqual(logs.map((line) => JSON.parse(line).healthy), [true, false]);
  assert.ok(logs.every((line) => JSON.parse(line).event === 'privacy_generation_health'));
  assert.doesNotMatch(logs.join(''), /secret|password|private-host|packet contents/);
  const empty = createPrivacyGenerationReconciler(async () => ({ rows: [] }), () => {});
  assert.equal((await empty()).reason, 'read_failed');
});

test('a stalled acquisition times out, aborts and cannot accumulate overlapping reads; recovery is observed', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let reads = 0;
  let signal!: AbortSignal;
  let complete!: (value: { rows: PrivacyGenerationSnapshot[] }) => void;
  const reports: PrivacyGenerationHealth[] = [];
  const reconcile = createPrivacyGenerationReconciler(async (currentSignal) => {
    reads += 1;
    signal = currentSignal;
    return new Promise((resolve) => { complete = resolve; });
  }, (state) => reports.push(state));
  const first = reconcile();
  await Promise.resolve();
  t.mock.timers.tick(PRIVACY_GENERATION_TIMEOUT_MS);
  assert.equal((await first).reason, 'read_failed');
  assert.equal(signal.aborted, true);
  assert.equal(reconcile(), first, 'do not enqueue another query while the old acquisition drains');
  assert.equal(reads, 1);
  complete({ rows: [settled] });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(reports.length, 1, 'late success must not overwrite the failed health signal');
  const next = reconcile();
  await Promise.resolve();
  complete({ rows: [settled] });
  assert.equal((await next).healthy, true);
  assert.equal(reads, 2);
});
