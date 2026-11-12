import assert from 'node:assert/strict';
import test from 'node:test';
import { createPublicPolicyReader } from './policyReader.js';

test('public authority reads independently of write-batcher configuration and refuses unfinished generations', async () => {
  let state = { generation: '7', materialized_generation: '7', remat_active: false, prefixes: [] };
  let reads = 0;
  const read = createPublicPolicyReader(async (_sql, _params, signal) => {
    reads++;
    assert.ok(signal instanceof AbortSignal);
    return { rows: [state] } as never;
  });
  assert.equal((await read())?.currentGeneration, 7);
  state = { ...state, generation: '8' };
  assert.equal(await read(), null);
  state = { ...state, materialized_generation: '8', remat_active: true };
  assert.equal(await read(), null);
  state = { ...state, remat_active: false };
  assert.equal((await read())?.currentGeneration, 8);
  assert.equal(reads, 4);
});
