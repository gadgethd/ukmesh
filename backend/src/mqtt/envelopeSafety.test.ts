import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ingestFailureAction,
  isPermanentPersistenceError,
  packetEnvelopeViolations,
  statusEnvelopeViolations,
} from './envelopeSafety.js';
import { createDurableMqttHandleMessage, type DurableMqttPacket } from './durableHandler.js';

test('an integer-valued packet envelope has no violations', () => {
  assert.deepEqual(packetEnvelopeViolations({
    packetType: 4,
    hopCount: 3,
    pathHashSizeBytes: 2,
    snr: -7.25,
    rssi: -101,
  }), []);
  // Integer-valued floats and numeric strings (as mctomqtt sends them) are fine:
  // only a fractional or non-finite value can fail the integer cast.
  assert.deepEqual(packetEnvelopeViolations({ packetType: Number('5'), hopCount: Number('0') }), []);
});

test('a fractional packet_type is rejected as not_integer', () => {
  assert.deepEqual(packetEnvelopeViolations({ packetType: Number('4.5') }), [
    { field: 'packet_type', value: 4.5, reason: 'not_integer' },
  ]);
});

test('values outside the destination integer column domain are rejected as out_of_range', () => {
  assert.deepEqual(packetEnvelopeViolations({ packetType: 3_000_000_000 }), [
    { field: 'packet_type', value: 3_000_000_000, reason: 'out_of_range' },
  ]);
  assert.deepEqual(packetEnvelopeViolations({ hopCount: -2_147_483_649 }), [
    { field: 'hop_count', value: -2_147_483_649, reason: 'out_of_range' },
  ]);
  assert.deepEqual(packetEnvelopeViolations({ pathHashSizeBytes: 2 ** 31 }), [
    { field: 'path_hash_size_bytes', value: 2 ** 31, reason: 'out_of_range' },
  ]);
});

test('non-finite radio values are rejected before reaching a float column', () => {
  assert.deepEqual(packetEnvelopeViolations({ snr: Number.POSITIVE_INFINITY }), [
    { field: 'snr', value: Number.POSITIVE_INFINITY, reason: 'not_finite' },
  ]);
});

test('status stats are validated against the INTEGER/BIGINT columns they feed', () => {
  assert.deepEqual(statusEnvelopeViolations({
    stats: { battery_mv: '4200', uptime_secs: '7200', tx_air_secs: '1', rx_air_secs: '2' },
  }), []);
  assert.deepEqual(statusEnvelopeViolations({ stats: { battery_mv: '4200.5' } }), [
    { field: 'battery_mv', value: 4200.5, reason: 'not_integer' },
  ]);
  assert.deepEqual(statusEnvelopeViolations({ stats: { batteryMv: '5e9' } }), [
    { field: 'battery_mv', value: 5e9, reason: 'out_of_range' },
  ]);
  assert.deepEqual(statusEnvelopeViolations({ stats: { uptime_secs: Number.MAX_SAFE_INTEGER + 1 } }), [
    { field: 'uptime_secs', value: Number.MAX_SAFE_INTEGER + 1, reason: 'out_of_range' },
  ]);
  // Mirrors statusTelemetry.readNum: a present-but-unparseable alias falls
  // through to the next alias rather than being treated as invalid.
  assert.deepEqual(statusEnvelopeViolations({ stats: { battery_mv: 'n/a', batteryMv: '4100' } }), []);
  assert.deepEqual(statusEnvelopeViolations({ stats: 'not-an-object' }), []);
});

test('deterministic SQLSTATE classes are permanent, transient ones are retried', () => {
  for (const code of ['22P02', '22003', '22001', '23514', '23503', '23502']) {
    assert.equal(isPermanentPersistenceError(Object.assign(new Error('db'), { code })), true, code);
    assert.equal(ingestFailureAction(Object.assign(new Error('db'), { code })), 'ack_quarantine', code);
  }
  for (const code of ['40001', '40P01', '08006', '57P01', '53300']) {
    assert.equal(isPermanentPersistenceError(Object.assign(new Error('db'), { code })), false, code);
    assert.equal(ingestFailureAction(Object.assign(new Error('db'), { code })), 'retry', code);
  }
  assert.equal(ingestFailureAction(new Error('Connection terminated unexpectedly')), 'retry');
  assert.equal(ingestFailureAction(new Error('PACKET_BATCH_QUEUE_FULL')), 'retry');
  // A wrapped driver error keeps its deterministic meaning via the message.
  assert.equal(
    ingestFailureAction(new Error('invalid input syntax for type integer: "4.5"')),
    'ack_quarantine',
  );
});

test('a fractional packet_type is quarantined and acked, and the next message is processed', async () => {
  const quarantined: string[] = [];
  const persisted: number[] = [];

  // Mirrors the durable ingest decision: an out-of-domain envelope is rejected
  // before the insert; a deterministic persistence failure is counted and
  // acknowledged; a transient one is rethrown so the broker redelivers.
  const ingest = async (envelope: Record<string, unknown>): Promise<void> => {
    const declared = envelope['packet_type'];
    const packetType = declared === undefined ? undefined : Number(declared);
    if (packetEnvelopeViolations({ packetType }).length > 0) {
      quarantined.push(String(declared));
      return;
    }
    try {
      // Safety net: a deterministic error the pre-insert validation did not
      // catch (e.g. a column narrower than the domain table assumes).
      if (packetType === 5) {
        throw Object.assign(new Error('invalid input syntax for type integer: "5.5"'), { code: '22P02' });
      }
      persisted.push(packetType ?? -1);
    } catch (error) {
      if (ingestFailureAction(error) === 'ack_quarantine') {
        quarantined.push(String(declared));
        return;
      }
      throw error;
    }
  };

  const handle = createDurableMqttHandleMessage(
    (packet: DurableMqttPacket) => ingest(JSON.parse(String(packet.payload)) as Record<string, unknown>),
    () => { /* durable failure handler */ },
  );

  const acks: Array<Error | undefined> = [];
  const topic = 'meshcore/MME/OBS/packets';
  handle({ topic, payload: JSON.stringify({ packet_type: '4.5' }) }, (error) => acks.push(error));
  handle({ topic, payload: JSON.stringify({ packet_type: 5 }) }, (error) => acks.push(error));
  handle({ topic, payload: JSON.stringify({ packet_type: 4 }) }, (error) => acks.push(error));
  for (let i = 0; i < 50 && acks.length < 3; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 2));
  }

  assert.deepEqual(quarantined, ['4.5', '5']);
  assert.deepEqual(persisted, [4], 'the consumer must keep processing after a quarantine');
  assert.deepEqual(acks, [undefined, undefined, undefined], 'quarantined messages are acknowledged, not retried');
});
