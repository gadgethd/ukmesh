import assert from 'node:assert/strict';
import test from 'node:test';
import { createOwnerService } from './ownerService.js';
import type { OwnerRepository } from './ownerRepository.js';

const NODE_A = 'A'.repeat(64);
const NODE_B = 'B'.repeat(64);

function makeService(resolve: () => Promise<string[]>, built: string[][]) {
  return createOwnerService({
    ownerLiveCacheTtlMs: 1_000,
    ownerLiveCache: new Map(),
    ownerDashboardCacheTtlMs: 60_000,
    ownerLastHopCacheTtlMs: 60_000,
    verifyMqttCredentials: async () => true,
    resolveOwnerNodeIds: resolve,
    autoLinkOwnerNodeIds: resolve,
    buildOwnerDashboard: async (nodeIds) => {
      built.push([...nodeIds]);
      return { totals: { ownedNodes: nodeIds.length } };
    },
    repository: {} as OwnerRepository,
  });
}

test('a warm dashboard cache cannot survive server-side revocation', async () => {
  let current = [NODE_A];
  const built: string[][] = [];
  const service = makeService(async () => current, built);
  const session = { v: 2 as const, mqttUsername: 'owner-a', exp: Date.now() + 60_000 };

  await service.getSessionDashboard(session);
  current = [];
  await assert.rejects(() => service.getSessionDashboard(session), /NO_ACTIVE_OWNER_NODE/);
  assert.deepEqual(built, [[NODE_A]]);
});

test('authorization changes build a dashboard only for the current node set', async () => {
  let current = [NODE_A];
  const built: string[][] = [];
  const service = makeService(async () => current, built);
  const session = { v: 2 as const, mqttUsername: 'owner-a', exp: Date.now() + 60_000 };

  await service.getSessionDashboard(session);
  current = [NODE_B];
  const result = await service.getSessionDashboard(session);
  assert.deepEqual(result.nodeIds, [NODE_B]);
  assert.deepEqual(built, [[NODE_A], [NODE_B]]);
});
