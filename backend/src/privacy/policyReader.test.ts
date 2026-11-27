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

test('a warm prefix index still requires fresh authority and is rebuilt after refusal', async () => {
  let active = false;
  const requests: unknown[] = [];
  const node = 'AB'.repeat(32);
  const read = createPublicPolicyReader(async (_sql, params) => {
    requests.push(params?.[0]);
    return { rows: [{
      generation: '7', materialized_generation: '7', remat_active: active,
      prefixes: active || params?.[0] === 7 ? null
        : [{ node_id: node, network: 'ukmesh', prefix_size_bytes: 1, prefix: 'AB' }],
    }] } as never;
  });

  const index = await read();
  const packet = { network: 'ukmesh', rxNodeId: node, srcNodeId: null, pathHashes: null, pathHashSizeBytes: null };
  assert.equal(index?.classify(packet).visibilityOk, false);
  assert.equal(await read(), index);
  active = true;
  assert.equal(await read(), null);
  active = false;
  const rebuilt = await read();
  assert.notEqual(rebuilt, index);
  assert.equal(rebuilt?.classify(packet).visibilityOk, false);
  assert.deepEqual(requests, [null, 7, 7, null]);
});

test('a late concurrent read cannot replace a newer certified prefix index', async () => {
  let release!: (rows: never) => void;
  const held = new Promise<never>(resolve => { release = resolve; });
  let calls = 0;
  const row = (generation: string, prefixes: [] | null) => ({
    rows: [{ generation, materialized_generation: generation, remat_active: false, prefixes }],
  }) as never;
  const read = createPublicPolicyReader(async (_sql, params) => {
    switch (++calls) {
      case 1: return row('7', []);
      case 2: return held;
      case 3: return row('8', []);
      default:
        assert.deepEqual(params, [8]);
        return row('8', null);
    }
  });
  await read();
  const inFlight = read();
  const current = await read();
  release(row('7', null));
  assert.equal(await inFlight, null);
  assert.equal(await read(), current);
  assert.equal(current?.currentGeneration, 8);
});

test('a delayed read cannot reuse an index after a concurrent fence refuses authority', async () => {
  let release!: (rows: never) => void;
  const held = new Promise<never>(resolve => { release = resolve; });
  const row = (active: boolean, prefixes: [] | null) => ({
    rows: [{ generation: '7', materialized_generation: '7', remat_active: active, prefixes }],
  }) as never;
  let calls = 0;
  const read = createPublicPolicyReader(async () => {
    if (++calls === 1) return row(false, []);
    if (calls === 2) return held;
    return row(true, null);
  });
  assert.equal((await read())?.currentGeneration, 7);
  const inFlight = read();
  assert.equal(await read(), null);
  release(row(false, null));
  assert.equal(await inFlight, null);
});
