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

function lastHopRow(bucket: string, sampleCount = 1) {
  return { bucket, last_hop_node_id: 'peer', last_hop_name: 'Peer', resolution: 'resolved' as const,
    avg_snr: 4, avg_rssi: -90, sample_count: sampleCount };
}

test('a warm refresh updates the current bucket, retains history and trims the rolling seven-day window', async (t) => {
  const now = Date.parse('2026-10-02T12:00:00Z');
  t.mock.timers.enable({ apis: ['Date'], now });
  const expiring = new Date(now - 7 * 24 * 60 * 60_000 + 1_000).toISOString();
  const previous = '2026-10-02T10:00:00.000Z';
  const latest = '2026-10-02T11:00:00.000Z';
  const since: Array<string | undefined> = [];
  const service = lastHopService(async (_nodes, _scope, cursor) => {
    since.push(cursor);
    return { rows: cursor === undefined
      ? [lastHopRow(expiring), lastHopRow(previous), lastHopRow(latest)]
      : [lastHopRow(latest, 9), lastHopRow('2026-10-02T12:00:00.000Z', 2)] };
  });
  assert.equal((await service.getOwnerLastHopStrength(['a'], 'a')).points.length, 3);
  t.mock.timers.tick(2_000);
  const refreshed = await service.getOwnerLastHopStrength(['a'], 'a', true);
  assert.deepEqual(since, [undefined, latest]);
  assert.deepEqual(refreshed.points.map(point => [point.bucket, point.sampleCount]), [
    [previous, 1], [latest, 9], ['2026-10-02T12:00:00.000Z', 2],
  ]);
  assert.deepEqual(await service.getOwnerLastHopStrength(['a'], 'a'), refreshed);
  assert.equal(since.length, 2, 'the completed warm refresh serves the next foreground read');
});

test('a failed warm refresh preserves the prior foreground cache and allows a later retry', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-02T12:00:00Z') });
  let calls = 0;
  const service = lastHopService(async () => {
    if (++calls === 2) throw new Error('warm query failed');
    return { rows: [lastHopRow('2026-10-02T11:00:00.000Z', calls)] };
  });
  const cached = await service.getOwnerLastHopStrength(['a'], 'a');
  await assert.rejects(service.getOwnerLastHopStrength(['a'], 'a', true), /warm query failed/);
  assert.deepEqual(await service.getOwnerLastHopStrength(['a'], 'a'), cached);
  assert.equal(calls, 2, 'failed refreshes must not evict useful cached data');
  assert.equal((await service.getOwnerLastHopStrength(['a'], 'a', true)).points[0]?.sampleCount, 3);
});

test('expired last-hop caches start a fresh bounded-window query instead of reusing an old cursor', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-02T12:00:00Z') });
  const since: Array<string | undefined> = [];
  const service = lastHopService(async (_nodes, _scope, cursor) => {
    since.push(cursor);
    return { rows: [lastHopRow('2026-10-02T11:00:00.000Z')] };
  });
  await service.getOwnerLastHopStrength(['a'], 'a');
  t.mock.timers.tick(60_000);
  await service.getOwnerLastHopStrength(['a'], 'a', true);
  assert.deepEqual(since, [undefined, undefined]);
});

test('fresh foreground last-hop data stays responsive while a background warm refresh is pending', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-02T12:00:00Z') });
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let calls = 0;
  const service = lastHopService(async () => {
    const count = ++calls;
    if (count === 2) await gate;
    return { rows: [lastHopRow('2026-10-02T11:00:00.000Z', count)] };
  });
  const cached = await service.getOwnerLastHopStrength(['a'], 'a');
  const warming = service.getOwnerLastHopStrength(['a'], 'a', true);
  t.after(async () => { release(); await warming; });
  const joinedWarm = service.getOwnerLastHopStrength(['a'], 'a', true);
  let foregroundSettled = false;
  const foreground = service.getOwnerLastHopStrength(['a'], 'a').then(result => {
    foregroundSettled = true;
    return result;
  });
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(foregroundSettled, true, 'a fresh cache hit must not wait for a slow background query');
  assert.deepEqual(await foreground, cached);
  assert.equal(calls, 2, 'overlapping warm requests still share the one refresh');
  release();
  assert.deepEqual(await joinedWarm, await warming);
  assert.equal((await service.getOwnerLastHopStrength(['a'], 'a')).points[0]?.sampleCount, 2);
  assert.equal(calls, 2);
});
