import assert from 'node:assert/strict';
import test from 'node:test';
import { renderOwnerAcl, OWNER_ACL_RENDERER_VERSION } from '../mqtt/aclManager.js';
import { buildOwnerInventoryBaseline } from './ownerInventoryBaseline.js';
import { createOwnerAuthorizationReadiness, OWNER_READINESS_FORMAT, publicReadinessPayload } from './ownerReadiness.js';
import { decideOwnerBaselineAction } from './autoRebaseline.js';

test('public and authenticated readiness share the same baseline validation evidence', async () => {
  const rendered = renderOwnerAcl('', [{ mqttUsername: 'owner', nodeIds: ['A'.repeat(64)] }], []);
  const state = {
    desiredGeneration: rendered.generation, renderedGeneration: rendered.generation,
    appliedGeneration: rendered.generation, lastError: null, lastVerifiedAt: null,
  };
  const baseline = buildOwnerInventoryBaseline({ accounts: [], configuredGrants: [], aclContent: rendered.content, aclState: state });
  const read = createOwnerAuthorizationReadiness({
    config: () => ({ mode: 'enforce', aclMode: 'apply', baselinePath: 'fixture', configuredGrants: [] }),
    readState: async () => state,
    readInventory: async () => ({ accounts: [] }),
    readAcl: () => rendered.content,
    readBaseline: () => baseline,
    loadAppliedArtifact: async () => ({ ...rendered, rendererVersion: OWNER_ACL_RENDERER_VERSION }),
  });
  const diagnostics = await read();
  assert.equal(diagnostics.format, OWNER_READINESS_FORMAT);
  assert.equal(diagnostics.ownerAuthorization.inventoryBaseline.ok, true);
  const payload = publicReadinessPayload(false, true, 'disconnected', 0);
  assert.deepEqual(Object.keys(payload.checks), ['database', 'mqtt']);
  assert.equal(decideOwnerBaselineAction(503, payload, diagnostics).reason, 'owner-baseline-current');
});

test('disabled baseline avoids inventory and file reads; unreadable configured baseline rejects', async () => {
  let baselinePath = '';
  const read = createOwnerAuthorizationReadiness({
    config: () => ({ mode: 'enforce', aclMode: 'apply', baselinePath, configuredGrants: [] }),
    readState: async () => ({ desiredGeneration: null, renderedGeneration: null, appliedGeneration: null, lastError: null, lastVerifiedAt: null }),
    readInventory: async () => { throw new Error('inventory-unavailable'); },
    readAcl: () => { throw new Error('must not read ACL'); },
    readBaseline: () => { throw new Error('must not read baseline'); },
    loadAppliedArtifact: async () => null,
  });
  assert.equal((await read()).ownerAuthorization.inventoryBaseline.required, false);
  baselinePath = 'fixture';
  await assert.rejects(read(), /inventory-unavailable/);
});
