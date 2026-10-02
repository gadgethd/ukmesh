import assert from 'node:assert/strict';
import test from 'node:test';
import { createOwnerService } from './ownerService.js';
import { createOwnerRepository } from './ownerRepository.js';

function lastHopService(fetch: ReturnType<typeof createOwnerRepository>['fetchLastHopStrength']) {
  const repository = createOwnerRepository({ query: async () => ({ rows: [] }) });
  repository.fetchLastHopStrength = fetch;
  return createOwnerService({
    repository,
    ownerLiveCache: new Map(),
    ownerLiveCacheTtlMs: 15_000,
    ownerDashboardCacheTtlMs: 20_000,
    ownerLastHopCacheTtlMs: 60_000,
    verifyMqttCredentials: async () => true,
    resolveOwnerNodeIds: async () => [],
    autoLinkOwnerNodeIds: async () => [],
    buildOwnerDashboard: async () => ({ nodes: [] }),
    invalidateOwnerNodeIdCache: () => {},
  });
}

test('last-hop prewarm and foreground reads share one in-flight refresh', async () => {
  let calls = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const service = lastHopService(async () => { calls++; await gate; return { rows: [] }; });
  const prewarm = service.getOwnerLastHopStrength(['a'], 'a', true);
  const foreground = service.getOwnerLastHopStrength(['a'], 'a');
  assert.equal(calls, 1);
  release();
  assert.deepEqual(await prewarm, { points: [] });
  assert.deepEqual(await foreground, { points: [] });
  await service.getOwnerLastHopStrength(['a'], 'a');
  assert.equal(calls, 1);
  await service.getOwnerLastHopStrength(['a'], 'a', true);
  assert.equal(calls, 2);
});

test('last-hop cache respects the complete ownership exclusion scope', async () => {
  const scopes: string[][] = [];
  const service = lastHopService(async (_nodeIds, ownedNodeIds) => { scopes.push(ownedNodeIds); return { rows: [] }; });
  await service.getOwnerLastHopStrength(['a', 'b'], 'a');
  await service.getOwnerLastHopStrength(['a', 'c'], 'a');
  await service.getOwnerLastHopStrength(['b', 'a'], 'a');
  assert.deepEqual(scopes, [['a', 'b'], ['a', 'c']]);
  await assert.rejects(service.getOwnerLastHopStrength(['b'], 'a', true), /NODE_NOT_OWNED/);
});

test('a failed last-hop refresh releases single-flight state for retry', async () => {
  let calls = 0;
  const service = lastHopService(async () => {
    if (++calls === 1) throw new Error('query failed');
    return { rows: [] };
  });
  await assert.rejects(service.getOwnerLastHopStrength(['a'], 'a', true), /query failed/);
  assert.deepEqual(await service.getOwnerLastHopStrength(['a'], 'a', true), { points: [] });
  assert.equal(calls, 2);
});
