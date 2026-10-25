import assert from 'node:assert/strict';
import test from 'node:test';
import {
  parseDeniedOwnerPublish,
  parseDeniedPublish,
  parseMeshcoreClientNodePrefix,
  parseMosquittoConnection,
  parseOwnerNodeTopic,
} from './brokerLog.js';

const NODE_ID = 'A1'.repeat(32);

test('parses a bare default-root denial even though it contains no node key', () => {
  const line = "1792756800: Denied PUBLISH from ESP32_123456 (d0, q0, r1, m0, 'meshcore', ... (42 bytes))";
  assert.deepEqual(parseDeniedPublish(line), { clientId: 'ESP32_123456', topic: 'meshcore' });
  assert.equal(parseDeniedOwnerPublish(line), null);
  assert.equal(parseDeniedPublish('New connection from 192.0.2.1'), null);
});

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

test('observes raw and malformed topic shapes without claiming an owner node', () => {
  for (const topic of [`meshcore/LBA/${NODE_ID}/raw`, 'meshcore', 'meshcore/neighbors', 'unknown/topic']) {
    const line = `1792538423: Denied PUBLISH from ESP32_a806F8 (d0, q0, r0, m0, '${topic}', ... (437 bytes))`;
    assert.deepEqual(parseDeniedPublish(line), { clientId: 'ESP32_a806F8', topic });
    assert.equal(parseDeniedOwnerPublish(line), null);
  }
});

test('retains exact EXT status and packet topics from the historical meshmonitor family', () => {
  const key = '67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5';
  for (const suffix of ['status', 'packets']) {
    const topic = `meshcore/EXT/${key}/${suffix}`;
    const line = `1788720614: Denied PUBLISH from meshmonitor-observer-qy92lten (d0, q0, r1, m0, '${topic}', ... (378 bytes))`;
    assert.deepEqual(parseDeniedPublish(line), { clientId: 'meshmonitor-observer-qy92lten', topic });
    assert.deepEqual(parseDeniedOwnerPublish(line), { clientId: 'meshmonitor-observer-qy92lten', nodeId: key });
  }
});

test('handles spaces in client IDs and quotes in topics; ignores other broker messages', () => {
  const topic = "meshcore/a'b/raw";
  const line = `Denied PUBLISH from client with spaces (d1, q2, r0, m123, '${topic}', ... (1 bytes))`;
  assert.deepEqual(parseDeniedPublish(line), { clientId: 'client with spaces', topic });
  assert.equal(parseMosquittoConnection("New client connected from 192.0.2.1:1 as client with spaces (p4, c1, k60, u'operator').")?.clientId, 'client with spaces');
  assert.equal(parseDeniedPublish('Denied PUBLISH from incomplete'), null);
  assert.equal(parseDeniedPublish(line.replace('Denied', 'Received')), null);
  assert.equal(parseDeniedPublish(`Received PUBLISH from client (d0, q0, r0, m0, '${line}', ... (42 bytes))`), null);
});
