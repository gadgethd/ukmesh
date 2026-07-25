import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { observeBrokerLogLine } from './connectionMonitor.js';

test('client ids and denied publishes remain non-authoritative observations', () => {
  const nodeId = 'A'.repeat(64);
  const connected = observeBrokerLogLine(
    "1710000000: New client connected from 203.0.113.9:4000 as meshcore_A1A1A1A1A1A1_2 (p4, c1, k60, u'owner-a').",
    1_000,
  );
  assert.deepEqual(connected, {
    kind: 'connection',
    mqttUsername: 'owner-a',
    clientId: 'meshcore_A1A1A1A1A1A1_2',
    claimedNodePrefix: 'A1A1A1A1A1A1',
  });

  const denied = observeBrokerLogLine(
    `1710000001: Denied PUBLISH from meshcore_A1A1A1A1A1A1_2 (d0, q0, r0, m0, 'meshcore/UK/${nodeId}/packets', ... (2 bytes))`,
    1_001,
  );
  assert.deepEqual(denied, {
    kind: 'denied-publish',
    mqttUsername: 'owner-a',
    clientId: 'meshcore_A1A1A1A1A1A1_2',
    claimedNodeId: nodeId,
  });

  const source = fs.readFileSync(new URL('./connectionMonitor.ts', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /ownerAuth|addOwnerNodeForUsername|updateUserAclBlock/);
});
