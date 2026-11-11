import assert from 'node:assert/strict';
import test from 'node:test';
import {
  observationIdentityKey,
  fallbackPacketIdentity,
  ramMqttDeliveryKey,
  packetAlreadyPersisted,
  type ObservationIdentity,
} from './observationIdentity.js';

test('hashless invalid envelopes retain identity across replay without collapsing different bytes', () => {
  const raw = Buffer.from('{"type":5,"raw":"invalid","timestamp":1234}');
  assert.equal(fallbackPacketIdentity(raw), fallbackPacketIdentity(Buffer.from(raw)));
  assert.notEqual(fallbackPacketIdentity(raw), fallbackPacketIdentity(Buffer.from('{"type":5,"raw":"invalid","timestamp":1235}')));
  assert.match(fallbackPacketIdentity(raw), /^[A-F0-9]{64}$/);
});

test('RAM admits QoS 0 without a message ID and preserves stable QoS 1 redelivery claims', () => {
  const input = { clientId: 'owned-fixture', topic: 'test/packets', payload: Buffer.from('data'), observationKey: 'sighting' };
  const first = ramMqttDeliveryKey({ ...input, qos: 0 });
  assert.notEqual(first, ramMqttDeliveryKey({ ...input, qos: 0 }));
  assert.match(first, /^q0:/);
  assert.equal(ramMqttDeliveryKey({ ...input, qos: 1, messageId: 7 }), ramMqttDeliveryKey({ ...input, qos: 1, messageId: 7 }));
  assert.notEqual(ramMqttDeliveryKey({ ...input, qos: 1, messageId: 7 }), ramMqttDeliveryKey({ ...input, qos: 1, messageId: 8 }));
  for (const messageId of [undefined, 0, -1, 65_536, 1.5]) {
    assert.throws(() => ramMqttDeliveryKey({ ...input, qos: 1, messageId }), /DELIVERY_ID_MISSING/);
  }
});

const base: ObservationIdentity = {
  packetHash: 'ABCDEF',
  rxNodeId: 'A'.repeat(64),
  topic: `meshcore/EMA/${'A'.repeat(64)}/packets`,
  network: 'ukmesh',
  hopCount: 2,
  routeType: 0,
  pathHashes: ['ABCD', 'EF01'],
  pathHashSizeBytes: 2,
  direction: 'rx',
};

test('F03 canonical observation key distinguishes hop, path, direction, observer and scope', () => {
  const key = observationIdentityKey(base);
  assert.equal(key, observationIdentityKey({ ...base, pathHashes: ['abcd', 'ef01'] }));
  for (const changed of [
    { hopCount: 3 },
    { pathHashes: ['ABCD', 'EF02'] },
    { pathHashSizeBytes: 3 },
    { routeType: 1 },
    { direction: 'tx' as const },
    { rxNodeId: 'B'.repeat(64) },
    { topic: `meshcore/MME/${'A'.repeat(64)}/packets` },
    { network: 'teesside' },
  ]) {
    assert.notEqual(key, observationIdentityKey({ ...base, ...changed }), JSON.stringify(changed));
  }
});

test('F03 restart probe checks the same identity, including a prior same-hash different-hop sighting', async () => {
  let statement = '';
  let values: unknown[] = [];
  const query = async (_sql: string, params: unknown[]) => {
    statement = _sql;
    values = params;
    return { rows: [{ present: false }] };
  };
  assert.equal(await packetAlreadyPersisted(query, { ...base, hopCount: 3 }), false);
  assert.match(statement, /hop_count IS NOT DISTINCT FROM/);
  assert.match(statement, /route_type IS NOT DISTINCT FROM/);
  assert.match(statement, /path_hashes IS NOT DISTINCT FROM/);
  assert.match(statement, /path_hash_size_bytes IS NOT DISTINCT FROM/);
  assert.match(statement, /payload->>'direction'/);
  assert.equal(values[4], 3);
  assert.deepEqual(values[6], ['ABCD', 'EF01']);
  assert.equal(values[8], 'rx');
});
