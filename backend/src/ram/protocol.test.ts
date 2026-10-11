import assert from 'node:assert/strict';
import test from 'node:test';
import {
  decodeRamStreamValue,
  encodeCommitMarker,
  encodePendingEntry,
  getRamBatchSize,
  getRamIngestMode,
  getRamMaxAgeMs,
  getRamQueueMaxBytes,
  getRamQueueMaxPackets,
  getRamStreamKey,
  type RamPendingEntry,
} from './protocol.js';
import type { PacketBatchInput } from '../db/packetBatch.js';

function sampleInput(overrides: Partial<PacketBatchInput> = {}): PacketBatchInput {
  return {
    time: new Date('2026-10-11T12:00:00.000Z'),
    packetHash: 'AB'.repeat(32),
    rxNodeId: 'CD'.repeat(32),
    srcNodeId: null,
    topic: 'meshcore/EGNX/OBSERVER/packets',
    topicPrefix: 'meshcore',
    iata: 'EGNX',
    packetType: 4,
    routeType: 1,
    hopCount: 2,
    rssi: -80,
    snr: 6.5,
    payloadJson: '{"_summary":"advert"}',
    companionSender: null,
    rawHex: 'DEADBEEF',
    advertCount: 3,
    pathHashes: ['ABCD'],
    pathHashSizeBytes: 2,
    network: 'ukmesh',
    transportCodes: null,
    regionScope: null,
    ...overrides,
  };
}

function sampleEntry(): RamPendingEntry {
  return {
    observationId: 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
    admittedAtMs: 1_700_000_000_000,
    isPrivate: false,
    visibilityOk: true,
    input: sampleInput(),
  };
}

function withEnv(name: string, value: string | undefined, fn: () => void): void {
  const previous = process.env[name];
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
  try {
    fn();
  } finally {
    if (previous === undefined) delete process.env[name];
    else process.env[name] = previous;
  }
}

test('pending entry round-trips through encode/decode', () => {
  const entry = sampleEntry();
  const decoded = decodeRamStreamValue(encodePendingEntry(entry));
  assert.ok(decoded && decoded.kind === 'entry');
  if (decoded.kind !== 'entry') return;
  assert.equal(decoded.entry.observationId, entry.observationId);
  assert.equal(decoded.entry.admittedAtMs, entry.admittedAtMs);
  assert.equal(decoded.entry.isPrivate, false);
  assert.equal(decoded.entry.visibilityOk, true);
  assert.equal(decoded.entry.input.time.toISOString(), '2026-10-11T12:00:00.000Z');
  assert.deepEqual(decoded.entry.input.pathHashes, ['ABCD']);
  assert.equal(decoded.entry.input.packetHash, entry.input.packetHash);
  assert.equal(decoded.entry.input.packetType, 4);
});

test('commit marker round-trips through encode/decode', () => {
  const marker = encodeCommitMarker({ kind: 'commit', through: '1700000000000-0', atMs: 1_700_000_001_000, packets: 42 });
  const decoded = decodeRamStreamValue(marker);
  assert.ok(decoded && decoded.kind === 'commit');
  if (decoded.kind !== 'commit') return;
  assert.equal(decoded.marker.through, '1700000000000-0');
  assert.equal(decoded.marker.packets, 42);
});

test('garbage values decode to null', () => {
  assert.equal(decodeRamStreamValue(undefined), null);
  assert.equal(decodeRamStreamValue(''), null);
  assert.equal(decodeRamStreamValue('not json'), null);
  assert.equal(decodeRamStreamValue('{"v":2,"k":"p"}'), null);
  assert.equal(decodeRamStreamValue('{"v":1,"k":"x"}'), null);
  assert.equal(decodeRamStreamValue('{"v":1,"k":"p"}'), null);
  assert.equal(decodeRamStreamValue('{"v":1,"k":"p","o":"","a":1,"d":{}}'), null);
  assert.equal(decodeRamStreamValue('{"v":1,"k":"p","o":"id","a":1,"d":{}}'), null);
});

test('private entries encode their classification flags', () => {
  const entry = sampleEntry();
  entry.isPrivate = true;
  entry.visibilityOk = false;
  const decoded = decodeRamStreamValue(encodePendingEntry(entry));
  assert.ok(decoded && decoded.kind === 'entry');
  if (decoded.kind !== 'entry') return;
  assert.equal(decoded.entry.isPrivate, true);
  assert.equal(decoded.entry.visibilityOk, false);
});

test('ingest mode parsing defaults to durable and accepts ram', () => {
  withEnv('MQTT_INGEST_MODE', undefined, () => assert.equal(getRamIngestMode(), 'durable'));
  withEnv('MQTT_INGEST_MODE', 'durable', () => assert.equal(getRamIngestMode(), 'durable'));
  withEnv('MQTT_INGEST_MODE', 'ram', () => assert.equal(getRamIngestMode(), 'ram'));
  withEnv('MQTT_INGEST_MODE', 'RAM', () => assert.equal(getRamIngestMode(), 'ram'));
  withEnv('MQTT_INGEST_MODE', 'nonsense', () => assert.equal(getRamIngestMode(), 'durable'));
});

test('env knobs are bounded and defaulted', () => {
  withEnv('MQTT_RAM_BATCH_SIZE', undefined, () => assert.equal(getRamBatchSize(), 100));
  withEnv('MQTT_RAM_BATCH_SIZE', '250', () => assert.equal(getRamBatchSize(), 250));
  withEnv('MQTT_RAM_BATCH_SIZE', '999999', () => assert.equal(getRamBatchSize(), 2_000));
  withEnv('MQTT_RAM_BATCH_SIZE', '0', () => assert.equal(getRamBatchSize(), 1));
  withEnv('MQTT_RAM_MAX_AGE_MS', undefined, () => assert.equal(getRamMaxAgeMs(), 120_000));
  withEnv('MQTT_RAM_MAX_AGE_MS', '50', () => assert.equal(getRamMaxAgeMs(), 1_000));
  withEnv('MQTT_RAM_QUEUE_MAX_PACKETS', undefined, () => assert.equal(getRamQueueMaxPackets(), 5_000));
  withEnv('MQTT_RAM_QUEUE_MAX_BYTES', undefined, () => assert.equal(getRamQueueMaxBytes(), 33_554_432));
  withEnv('MQTT_RAM_STREAM_KEY', 'custom:key', () => assert.equal(getRamStreamKey(), 'custom:key'));
  withEnv('MQTT_RAM_STREAM_KEY', '  ', () => assert.equal(getRamStreamKey(), 'meshcore:ingest:pending'));
});
