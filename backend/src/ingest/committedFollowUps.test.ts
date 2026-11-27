import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { createCommittedFollowUps, type PostPersistFollowUpArgs } from './committedFollowUps.js';
import { postPersistDroppedTotal, postPersistInFlightGauge } from '../metrics/ingest.js';

const input: PostPersistFollowUpArgs = { finalHash: 'owned-followup', decodedPathHashSizeBytes: 2,
  path: ['AABB', 'BBCC', 'CCDD', 'DDEE', 'EEFF', 'FFAA'], decodedRouteType: 0,
  network: 'ukmesh', resolvedPacketType: 4 };

test('durable and RAM commit callers share the same bounded followup capacity and drain', async t => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let queries = 0;
  const notices: string[] = [];
  const service = createCommittedFollowUps({
    query: async () => { queries++; await gate; return { rows: [] }; },
    observePath: ({ packetHash }) => notices.push(packetHash),
    emitNodeUpsert: () => undefined,
  });
  t.after(async () => { release(); await service.close(); });
  const before = (await postPersistDroppedTotal.get()).values[0]!.value;
  for (let index = 0; index < 65; index++) service.schedule({ ...input, finalHash: 'owned-' + index });
  assert.equal(queries, 64);
  assert.equal(notices.length, 65, 'cache invalidation is independent of the lookup bound');
  assert.equal((await postPersistDroppedTotal.get()).values[0]!.value - before, 1);
  let drained = false;
  const closing = service.close().then(() => { drained = true; });
  await setImmediate(); assert.equal(drained, false);
  release(); await closing;
  service.schedule(input);
  assert.equal(queries, 64);
  assert.equal((await postPersistInFlightGauge.get()).values[0]!.value, 0);
});
