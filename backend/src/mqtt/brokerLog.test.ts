import assert from 'node:assert/strict';
import test from 'node:test';
import {
  parseDeniedOwnerPublish,
  parseMeshcoreClientNodePrefix,
  parseMosquittoConnection,
  parseOwnerNodeTopic,
} from './brokerLog.js';

const NODE_ID = 'A1'.repeat(32);

test('parses the authenticated username and mctomqtt key prefix from a connection log', () => {
  const line = "1784060066: New client connected from 192.0.2.10:35816 as meshcore_A1A1A1A1A1A1_2 (p4, c1, k60, u'test.user').";
  assert.deepEqual(parseMosquittoConnection(line), {
    clientId: 'meshcore_A1A1A1A1A1A1_2',
    mqttUsername: 'test.user',
  });
  assert.equal(parseMeshcoreClientNodePrefix('meshcore_A1A1A1A1A1A1_2'), 'A1A1A1A1A1A1');
  assert.equal(parseMeshcoreClientNodePrefix('meshcore_client_a1a1a1a1_12'), 'A1A1A1A1');
  assert.equal(parseMeshcoreClientNodePrefix('owner-auth-ab12cd'), null);
});

test('extracts an exact public key only from supported owner MQTT topics', () => {
  assert.equal(parseOwnerNodeTopic(`meshcore/NCL/${NODE_ID}/packets`), NODE_ID);
  assert.equal(parseOwnerNodeTopic(`ukmesh/ncl/${NODE_ID.toLowerCase()}/status`), NODE_ID);
  assert.equal(parseOwnerNodeTopic(`meshcore/NCL/${NODE_ID}/neighbors`), NODE_ID);
  assert.equal(parseOwnerNodeTopic(`meshcore/NCL/${NODE_ID}/neighbours`), NODE_ID);
  assert.equal(parseOwnerNodeTopic(`meshcore-test/NCL/${NODE_ID}/packets`), null);
  assert.equal(parseOwnerNodeTopic(`meshcore/NCL/${NODE_ID}/internal`), null);
  assert.equal(parseOwnerNodeTopic('meshcore/NCL/not-a-key/packets'), null);
});

test('associates a denied publish with its client and exact topic public key', () => {
  const line = `Denied PUBLISH from meshcore_A1A1A1A1A1A1_2 (d0, q0, r0, m0, 'meshcore/NCL/${NODE_ID}/status', ... (42 bytes))`;
  assert.deepEqual(parseDeniedOwnerPublish(line), {
    clientId: 'meshcore_A1A1A1A1A1A1_2',
    nodeId: NODE_ID,
  });
  assert.equal(
    parseDeniedOwnerPublish(`Denied PUBLISH from client (d0, q0, r0, m0, 'other/NCL/${NODE_ID}/status')`),
    null,
  );
});
