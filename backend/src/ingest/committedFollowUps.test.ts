import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { createCommittedFollowUps, type PostPersistFollowUpArgs } from './committedFollowUps.js';
import { postPersistDroppedTotal, postPersistInFlightGauge } from '../metrics/ingest.js';

const input: PostPersistFollowUpArgs = { finalHash: 'owned-followup', decodedPathHashSizeBytes: 2,
  path: ['AABB', 'BBCC', 'CCDD', 'DDEE', 'EEFF', 'FFAA'], decodedRouteType: 0,
  network: 'ukmesh', resolvedPacketType: 4 };

test('durable and RAM commit callers share the same bounded followup capacity and drain', async t => {
  const previous = process.env['PATH_BETA_PRERESOLVE_ENABLED'];
  process.env['PATH_BETA_PRERESOLVE_ENABLED'] = '0';
  t.after(() => { if (previous === undefined) delete process.env['PATH_BETA_PRERESOLVE_ENABLED']; else process.env['PATH_BETA_PRERESOLVE_ENABLED'] = previous; });
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let queries = 0;
  const service = createCommittedFollowUps({
    query: async () => { queries++; await gate; return { rows: [] }; },
    resolvePool: { runBackground: async () => null },
    scheduleSlowResolution: () => undefined, emitNodeUpsert: () => undefined,
  });
  t.after(async () => { release(); await service.close(); });
  const before = (await postPersistDroppedTotal.get()).values[0]!.value;
  for (let index = 0; index < 65; index++) service.schedule({ ...input, finalHash: 'owned-' + index });
  assert.equal(queries, 64);
  assert.equal((await postPersistDroppedTotal.get()).values[0]!.value - before, 1);
  let drained = false;
  const closing = service.close().then(() => { drained = true; });
  await setImmediate(); assert.equal(drained, false);
  release(); await closing;
  service.schedule(input);
  assert.equal(queries, 64);
  assert.equal((await postPersistInFlightGauge.get()).values[0]!.value, 0);
});

test('pre-resolution coalesces observers and closes before a late result can publish', async t => {
  const previous = process.env['PATH_BETA_PRERESOLVE_ENABLED'];
  process.env['PATH_BETA_PRERESOLVE_ENABLED'] = '1';
  t.after(() => { if (previous === undefined) delete process.env['PATH_BETA_PRERESOLVE_ENABLED']; else process.env['PATH_BETA_PRERESOLVE_ENABLED'] = previous; });
  t.mock.timers.enable({ apis: ['Date'], now: 100_000 });
  let finish!: () => void, calls = 0;
  const gate = new Promise<void>(resolve => { finish = resolve; });
  const service = createCommittedFollowUps({ query: async () => ({ rows: [] }),
    resolvePool: { runBackground: async () => { calls++; await gate; return null; } },
    scheduleSlowResolution: () => undefined, emitNodeUpsert: () => undefined });
  t.after(async () => { finish(); await service.close(); });
  service.schedule({ ...input, path: undefined, decodedPathHashSizeBytes: undefined });
  service.schedule({ ...input, path: undefined, decodedPathHashSizeBytes: undefined });
  assert.equal(calls, 1);
  let closed = false;
  const closing = service.close().then(() => { closed = true; });
  await setImmediate(); assert.equal(closed, false);
  finish(); await closing;
  service.schedule({ ...input, finalHash: 'another' });
  assert.equal(calls, 1);
});
