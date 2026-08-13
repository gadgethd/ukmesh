import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { renderOwnerAcl } from '../mqtt/aclManager.js';
import {
  buildOwnerInventoryBaseline,
  loadOwnerInventoryBaseline,
  validateOwnerInventoryBaseline,
  writeOwnerInventoryBaseline,
} from './ownerInventoryBaseline.js';

const NODE_A = 'A'.repeat(64);
const NODE_B = 'B'.repeat(64);
const NODE_C = 'C'.repeat(64);

function fixture() {
  const aclContent = renderOwnerAcl('', [
    { mqttUsername: 'alice', nodeIds: [NODE_A] },
    { mqttUsername: 'bob', nodeIds: [NODE_B] },
  ], []).content;
  return buildOwnerInventoryBaseline({
    generatedAt: '2026-08-13T00:00:00.000Z',
    accounts: [
      { mqttUsername: 'alice', isActive: true, nodeId: NODE_A, verificationMethod: 'operator-config', verifiedAt: '2026-01-01', revokedAt: null },
      { mqttUsername: 'bob', isActive: true, nodeId: NODE_B, verificationMethod: 'operator-database', verifiedAt: '2026-01-01', revokedAt: null },
      { mqttUsername: 'legacy', isActive: true, nodeId: NODE_C, verificationMethod: null, verifiedAt: null, revokedAt: null },
    ],
    configuredGrants: [{ mqttUsername: 'alice', nodeId: NODE_A }],
    aclContent,
    aclState: {
      desiredGeneration: 'desired',
      renderedGeneration: 'desired',
      appliedGeneration: 'desired',
      lastError: null,
    },
  });
}

test('owner baseline preserves every active verification method and ACL readback', () => {
  const baseline = fixture();
  assert.deepEqual(baseline.counts, {
    activeAccounts: 3,
    activeGrants: 3,
    operatorConfig: 1,
    operatorDatabase: 1,
    legacyOrNullMethod: 1,
    configuredGrants: 1,
    aclGrants: 2,
  });
  assert.equal(baseline.activeGrants[2]?.verificationMethod, null);
  assert.equal(validateOwnerInventoryBaseline(baseline, baseline).ok, true);
  const missing = { ...baseline, activeGrants: baseline.activeGrants.slice(0, 2) };
  assert.deepEqual(validateOwnerInventoryBaseline(baseline, missing).mismatches, ['activeGrants']);
});

test('owner baseline is mode-0600, exclusive, checksummed, and tamper-evident', () => {
  const directory = mkdtempSync(path.join(tmpdir(), 'meshcore-owner-inventory-'));
  const filePath = path.join(directory, 'baseline.json');
  try {
    const baseline = fixture();
    writeOwnerInventoryBaseline(filePath, baseline);
    assert.deepEqual(loadOwnerInventoryBaseline(filePath), baseline);
    assert.throws(() => writeOwnerInventoryBaseline(filePath, baseline), /EEXIST/);
    const bytes = readFileSync(filePath, 'utf8').replace('operator-config', 'operator-database');
    writeFileSync(filePath, bytes, { mode: 0o600 });
    assert.throws(() => loadOwnerInventoryBaseline(filePath), /CHECKSUM_INVALID/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
