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
        prefix_cache_fresh: true, lock_ok: true,
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
  assert.match(statements[1]!, /existing\.packet_hash = d\.packet_hash/);
  assert.match(statements[1]!, /ROW_NUMBER\(\) OVER/);
  assert.match(statements[1]!, /pg_try_advisory_xact_lock\(hashtext\(.meshcore:packet-batch-writer.\)\)/);
  assert.match(statements[1]!, /locked AS MATERIALIZED/);
  assert.match(statements[1]!, /AS lock_ok/);
  assert.match(statements[1]!, /INSERT INTO packets \(\s*observation_id,/);
  assert.match(statements[1]!, /COALESCE\(i\.observation_id_in, gen_random_uuid\(\)\) AS observation_id/);
  assert.match(statements[1]!, /SELECT observation_id, time, packet_hash/);
  assert.match(statements[1]!, /network, observation_id\s+FROM new_rows/);
  assert.match(statements[1]!, /FOR KEY SHARE/);
  assert.doesNotMatch(statements[1]!, /FROM private_node_prefixes pp/);
});

test('F03 batch arbitration retains distinct same-hash paths at one hop count', async () => {
  let batchSql = '';
  let batchParams: unknown[] = [];
  configurePacketBatch(async (sql, params) => {
    if (sql.includes('jsonb_agg(jsonb_build_object')) {
      return { rows: [{ generation: '7', prefixes: [] }] };
    }
    batchSql = sql;
    batchParams = params ?? [];
    return { rows: [0, 1].map((row_id) => ({
      row_id, is_private: false, visibility_ok: true, prefix_cache_fresh: true,
      lock_ok: true,
    })) };
  });
  const base: PacketBatchInput = {
    time: new Date('2026-08-06T01:00:00.000Z'), packetHash: 'same-hash',
    rxNodeId: 'A'.repeat(64), srcNodeId: 'B'.repeat(64),
    topic: `meshcore/EMA/${'A'.repeat(64)}/packets`, topicPrefix: 'meshcore',
    iata: 'EMA', packetType: 5, routeType: 0, hopCount: 2,
    rssi: -80, snr: 5, payloadJson: JSON.stringify({ direction: 'rx' }),
    companionSender: null, rawHex: '00', advertCount: null,
    pathHashes: ['ABCD'], pathHashSizeBytes: 2, network: 'ukmesh',
    transportCodes: null, regionScope: null,
  };
  const first = enqueuePacket(base);
  const second = enqueuePacket({ ...base, pathHashes: ['EF01'] });
  await flush();
  await Promise.all([first, second]);
  assert.match(batchSql, /incoming/);
  assert.equal(batchParams.length, 50);
});
