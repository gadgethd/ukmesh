import assert from 'node:assert/strict';
import test from 'node:test';
import { ownerGrantConfigGeneration, parseOwnerGrantConfig } from './ownerGrantConfig.js';

const NODE_A = 'A1'.repeat(32);
const NODE_B = 'B2'.repeat(32);
const HERMES_TEST_NODE_ID = 'A2FF345C45EDDCCB453C9F6B1E9973296932CD4C32EC106541D972BFBF4A4FC5';

test('operator grant config is normalized, deduplicated, and deterministic', () => {
  const grants = parseOwnerGrantConfig(`owner=${NODE_B.toLowerCase()}|${NODE_A},owner=${NODE_A}`);
  assert.deepEqual(grants, [
    { mqttUsername: 'owner', nodeId: NODE_A },
    { mqttUsername: 'owner', nodeId: NODE_B },
  ]);
  assert.equal(ownerGrantConfigGeneration(grants), ownerGrantConfigGeneration([...grants]));
});

test('operator grant config rejects empty, malformed, and ambiguous entries', () => {
  assert.throws(() => parseOwnerGrantConfig('owner='), /INVALID_OWNER_GRANT_NODE/);
  assert.throws(() => parseOwnerGrantConfig('bad user=' + NODE_A), /INVALID_OWNER_GRANT_USERNAME/);
  assert.throws(() => parseOwnerGrantConfig('missing-separator'), /INVALID_OWNER_GRANT_CONFIG_ENTRY/);
});

test('includes the canonical hermes-test owner grant', () => {
  assert.deepEqual(
    parseOwnerGrantConfig(`hermes-test=${HERMES_TEST_NODE_ID.toLowerCase()}`),
    [{ mqttUsername: 'hermes-test', nodeId: HERMES_TEST_NODE_ID }],
  );
});
