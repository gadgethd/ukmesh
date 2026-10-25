import assert from 'node:assert/strict';
import test from 'node:test';
import { buildPacketBatchInput } from './packetInput.js';
import {
  configurePacketBatch,
  enqueuePacket,
  flush,
  type PacketBatchInput,
} from './packetBatch.js';

test('a poison row cannot reject the valid packet sharing its transaction', async () => {
  const query = async (sql: string, params: unknown[] = []) => {
    if (sql.includes('jsonb_agg(jsonb_build_object')) return { rows: [{ generation: '7', prefixes: [] }] };
    if (sql.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: true }] };
    const count = params.length / 27;
    for (let index = 0; index < count; index++) {
      if (params[index * 27 + 9] === 5.5) throw Object.assign(new Error('invalid integer'), { code: '22P02' });
    }
    return { rows: Array.from({ length: count }, (_, row_id) => ({ row_id, is_private: false, visibility_ok: true, prefix_cache_fresh: true })) };
  };
  configurePacketBatch(query, async (work) => work(query));
  const make = (packetHash: string, packetType: number) => buildPacketBatchInput({ packetHash, packetType,
    topic: 'meshcore/LHR/fixture/packets', rawHex: '01' });
  const valid = enqueuePacket(make('valid-neighbour', 5));
  const invalid = enqueuePacket(make('poison', 5.5));
  const rejected = assert.rejects(invalid, /invalid integer/);
  await flush();
  assert.equal((await valid).visibilityOk, true);
  await rejected;
});

test('packet batches retry transient database failures without losing the pending row', async () => {
  let attempts = 0;
  const statements: string[] = [];
  const query = async (text: string) => {
    if (text.includes('jsonb_agg(jsonb_build_object')) {
      return { rows: [{ generation: '7', prefixes: [] }] };
    }
    statements.push(text);
    if (text.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: true }] };
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
  };
  configurePacketBatch(query, async (work) => work(query));

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
  assert.match(statements[3]!, /new_rows AS MATERIALIZED/);
  assert.match(statements[3]!, /INSERT INTO ingest_observation_ledger/);
  assert.match(statements[3]!, /JOIN claimed/);
  assert.match(statements[3]!, /existing\.packet_hash = d\.packet_hash/);
  assert.match(statements[3]!, /ROW_NUMBER\(\) OVER/);
  assert.match(statements[2]!, /pg_try_advisory_xact_lock\(hashtext\(.meshcore:packet-batch-writer.\)\)/);
  assert.match(statements[3]!, /INSERT INTO packets \(\s*observation_id,/);
  assert.match(statements[3]!, /COALESCE\(i\.observation_id_in, gen_random_uuid\(\)\) AS observation_id/);
  assert.match(statements[3]!, /SELECT observation_id, time, packet_hash/);
  assert.match(statements[3]!, /network, observation_id\s+FROM new_rows/);
  assert.match(statements[3]!, /FOR KEY SHARE/);
  assert.doesNotMatch(statements[3]!, /FROM private_node_prefixes pp/);
});

test('F03 batch arbitration retains distinct same-hash paths at one hop count', async () => {
  let batchSql = '';
  let batchParams: unknown[] = [];
  const query = async (sql: string, params?: unknown[]) => {
    if (sql.includes('jsonb_agg(jsonb_build_object')) {
      return { rows: [{ generation: '7', prefixes: [] }] };
    }
    if (sql.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: true }] };
    batchSql = sql;
    batchParams = params ?? [];
    return { rows: [0, 1].map((row_id) => ({
      row_id, is_private: false, visibility_ok: true, prefix_cache_fresh: true,
      lock_ok: true,
    })) };
  };
  configurePacketBatch(query, async (work) => work(query));
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
  assert.equal(batchParams.length, 54);
});

test('F02 refuses a contended writer before any mutation and gates the hourly predicate', async () => {
  const statements: string[] = [];
  let transactionStarted = false;
  const query = async (sql: string) => {
    if (sql.includes('jsonb_agg(jsonb_build_object')) {
      return { rows: [{ generation: '7', prefixes: [] }] };
    }
    assert.equal(transactionStarted, true);
    statements.push(sql);
    if (sql.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: false }] };
    return { rows: [{ row_id: 0, is_private: false, visibility_ok: true, prefix_cache_fresh: true }] };
  };
  configurePacketBatch(query, async (work) => {
    transactionStarted = true;
    try { return await work(query); } finally { transactionStarted = false; }
  });
  const packet: PacketBatchInput = {
    time: new Date('2026-08-06T01:00:00.000Z'), packetHash: 'f02-refused',
    rxNodeId: 'A'.repeat(64), srcNodeId: 'B'.repeat(64),
    topic: `meshcore/EMA/${'A'.repeat(64)}/packets`, topicPrefix: 'meshcore',
    iata: 'EMA', packetType: 5, routeType: 0, hopCount: 1,
    rssi: -80, snr: 5, payloadJson: '{}', companionSender: null,
    rawHex: '00', advertCount: null, pathHashes: null,
    pathHashSizeBytes: null, network: 'ukmesh', transportCodes: null, regionScope: null,
  };
  const result = enqueuePacket(packet);
  await assert.rejects(flush(), /PACKET_BATCH_LOCK_BUSY/);
  await assert.rejects(result, /PACKET_BATCH_LOCK_BUSY/);
  assert.equal(statements.length, 7, 'each retry acquires only the lock');
  assert.ok(statements.every((sql) => sql.includes('pg_try_advisory_xact_lock')),
    'lock refusal never reaches a mutation statement');

  statements.length = 0;
  configurePacketBatch(async (sql) => {
    if (sql.includes('jsonb_agg(jsonb_build_object')) return { rows: [{ generation: '7', prefixes: [] }] };
    statements.push(sql);
    if (sql.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: true }] };
    return { rows: [{ row_id: 0, is_private: false, visibility_ok: true, prefix_cache_fresh: true }] };
  }, async (work) => work(async (sql) => {
    statements.push(sql);
    if (sql.includes('pg_try_advisory_xact_lock')) return { rows: [{ ok: true }] };
    return { rows: [{ row_id: 0, is_private: false, visibility_ok: true, prefix_cache_fresh: true }] };
  }));
  const accepted = enqueuePacket(packet);
  await flush();
  await accepted;
  assert.equal(statements.length, 2);
  assert.match(statements[0]!, /pg_try_advisory_xact_lock/);
  assert.doesNotMatch(statements[1]!, /locked AS MATERIALIZED/);
  assert.match(statements[1]!, /WHERE \(network = 'test'\s+OR topic_prefix <> 'meshcore-test'\)/);
});
