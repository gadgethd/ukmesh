import assert from 'node:assert/strict';
import test from 'node:test';
import {
  configurePacketBatch,
  enqueuePacket,
  flush,
  type PacketBatchInput,
} from './packetBatch.js';

test('packet batches retry transient database failures without losing the pending row', async () => {
  let attempts = 0;
  const statements: string[] = [];
  configurePacketBatch(async (text) => {
    if (text.includes('jsonb_agg(jsonb_build_object')) {
      return { rows: [{ generation: '7', prefixes: [] }] };
    }
    statements.push(text);
    attempts += 1;
    if (attempts < 3) {
      throw new Error('the database system is not yet accepting connections');
    }
    return {
      rows: [{
        row_id: 0,
        is_private: false,
        visibility_ok: true,
        prefix_cache_fresh: true,
      }],
    };
  });

  const packet: PacketBatchInput = {
    time: new Date('2026-08-06T01:00:00.000Z'),
    packetHash: 'retry-test-packet',
    rxNodeId: 'A'.repeat(64),
    srcNodeId: 'B'.repeat(64),
    topic: `meshcore/EMA/${'A'.repeat(64)}/packets`,
    topicPrefix: 'meshcore',
    iata: 'EMA',
    packetType: 5,
    routeType: 0,
    hopCount: 1,
    rssi: -80,
    snr: 5,
    payloadJson: JSON.stringify({ test: true }),
    companionSender: null,
    rawHex: '00',
    advertCount: null,
    pathHashes: null,
    pathHashSizeBytes: null,
    network: 'ukmesh',
    transportCodes: null,
    regionScope: null,
  };

  const resultPromise = enqueuePacket(packet);
  await flush();

  assert.deepEqual(await resultPromise, {
    isPrivate: false,
    visibilityOk: true,
  });
  assert.equal(attempts, 3);
  assert.doesNotMatch(statements[0]!, /new_rows AS MATERIALIZED/);
  assert.match(statements[1]!, /new_rows AS MATERIALIZED/);
  assert.match(statements[1]!, /existing\.packet_hash = c\.packet_hash/);
  assert.match(statements[1]!, /INSERT INTO packets \(\s*observation_id,/);
  assert.match(statements[1]!, /SELECT gen_random_uuid\(\), time, packet_hash/);
  assert.match(statements[1]!, /FOR KEY SHARE/);
  assert.doesNotMatch(statements[1]!, /FROM private_node_prefixes pp/);
});
