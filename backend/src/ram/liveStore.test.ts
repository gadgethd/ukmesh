// RAM mode is enabled for this whole file before any test runs; the store
// gates its behaviour on getRamIngestMode() per call.
process.env['MQTT_INGEST_MODE'] = 'ram';

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  admitPacketToRam,
  configureRamStore,
  RamAdmissionRefusedError,
  readPendingEntries,
  type RamStoreRedis,
} from './liveStore.js';
import { mergePendingPacketsIntoRecentRows } from './liveRead.js';
import {
  encodeCommitMarker,
  encodePendingEntry,
  type RamPendingEntry,
} from './protocol.js';
import type { InsertPacketInput } from '../db/packetInput.js';

class FakeStore implements RamStoreRedis {
  len = 0;
  bytes = 0;
  added: string[] = [];
  rev: Array<[string, string[]]> = [];
  xlenError: Error | null = null;

  async xlen(): Promise<number> {
    if (this.xlenError) throw this.xlenError;
    return this.len;
  }

  async xadd(_key: string, _id: string, _field: string, value: string): Promise<string> {
    this.added.push(value);
    return `${1_700_000_000_000 + this.added.length}-0`;
  }

  async xrevrange(): Promise<Array<[string, string[]]>> {
    return this.rev;
  }

  async memory(): Promise<number> {
    return this.bytes;
  }
}

function sampleInput(hash = 'ab'.repeat(32)): InsertPacketInput {
  return {
    packetHash: hash,
    rxNodeId: 'cd'.repeat(32),
    topic: 'meshcore/EGNX/OBSERVER/packets',
    packetType: 4,
    routeType: 1,
    hopCount: 2,
    rawHex: 'DEADBEEF',
    pathHashes: ['abcd', 'ef12'],
    pathHashSizeBytes: 2,
    network: 'ukmesh',
  };
}

function entryFor(hash: string, opts: { visibilityOk?: boolean; network?: string; time?: string } = {}): RamPendingEntry {
  return {
    observationId: `obs-${hash.slice(0, 6)}`,
    admittedAtMs: 1_700_000_000_000,
    isPrivate: opts.visibilityOk === false,
    visibilityOk: opts.visibilityOk !== false,
    input: {
      time: new Date(opts.time ?? '2026-10-11T12:00:00.000Z'),
      packetHash: hash,
      rxNodeId: 'cd'.repeat(32),
      srcNodeId: null,
      topic: 'meshcore/EGNX/OBSERVER/packets',
      topicPrefix: 'meshcore',
      iata: 'EGNX',
      packetType: 4,
      routeType: 1,
      hopCount: 2,
      rssi: -80,
      snr: 6,
      payloadJson: '{"_summary":"advert","direction":"rx"}',
      companionSender: null,
      rawHex: 'DEADBEEF',
      advertCount: null,
      pathHashes: null,
      pathHashSizeBytes: null,
      network: opts.network ?? 'ukmesh',
      transportCodes: null,
      regionScope: null,
    },
  };
}

test('admission fails closed when the privacy cache is unavailable', async () => {
  const fake = new FakeStore();
  configureRamStore(fake);
  const result = await admitPacketToRam(sampleInput());
  assert.ok(result.observationId.length > 0);
  assert.equal(result.isPrivate, true); // no cache loaded in tests → fail closed
  assert.equal(result.visibilityOk, false);
  assert.equal(fake.added.length, 1);
});

test('admission encodes the transformed batch input into the stream', async () => {
  const fake = new FakeStore();
  configureRamStore(fake);
  await admitPacketToRam(sampleInput());
  const decoded = JSON.parse(fake.added[0] ?? '{}') as Record<string, unknown>;
  assert.equal(decoded['k'], 'p');
  const input = decoded['d'] as Record<string, unknown>;
  assert.equal(input['packetHash'], 'ab'.repeat(32));
  assert.deepEqual(input['pathHashes'], ['ABCD', 'EF12']); // uppercased by buildPacketBatchInput
  assert.equal(input['iata'], 'EGNX');
});

test('admission refuses when the packet-count bound is reached', async () => {
  const fake = new FakeStore();
  fake.len = 5_000; // default MQTT_RAM_QUEUE_MAX_PACKETS
  configureRamStore(fake);
  await assert.rejects(
    admitPacketToRam(sampleInput()),
    (error: unknown) => error instanceof RamAdmissionRefusedError && error.reason === 'queue_full',
  );
  assert.equal(fake.added.length, 0);
});

test('admission refuses when the byte bound is reached', async () => {
  const fake = new FakeStore();
  fake.len = 3_000; // ≥ half the packet bound → forces a stats refresh
  fake.bytes = 33_554_432; // default MQTT_RAM_QUEUE_MAX_BYTES
  configureRamStore(fake);
  await assert.rejects(
    admitPacketToRam(sampleInput()),
    (error: unknown) => error instanceof RamAdmissionRefusedError && error.reason === 'queue_bytes',
  );
  assert.equal(fake.added.length, 0);
});

test('admission reports redis unavailability distinctly', async () => {
  const fake = new FakeStore();
  fake.xlenError = new Error('connect ECONNREFUSED');
  configureRamStore(fake);
  await assert.rejects(
    admitPacketToRam(sampleInput()),
    (error: unknown) => error instanceof RamAdmissionRefusedError && error.reason === 'redis_unavailable',
  );
});

test('readPendingEntries skips markers and returns oldest-last', async () => {
  const fake = new FakeStore();
  fake.rev = [
    ['100-0', ['e', encodeCommitMarker({ kind: 'commit', through: '99-0', atMs: 1, packets: 1 })]],
    ['99-0', ['e', encodePendingEntry(entryFor('99'.repeat(32)))]],
    ['98-0', ['e', encodePendingEntry(entryFor('98'.repeat(32)))]],
  ];
  configureRamStore(fake);
  const pending = await readPendingEntries(10);
  assert.equal(pending.length, 2);
  assert.equal(pending[0]?.id, '98-0');
  assert.equal(pending[1]?.id, '99-0');
});

test('merged reads append only public, scoped, non-duplicate pending rows', async () => {
  const fake = new FakeStore();
  fake.rev = [
    ['103-0', ['e', encodePendingEntry(entryFor('ab'.repeat(32), { time: '2026-10-11T12:05:00.000Z' }))]], // dup of DB row
    ['102-0', ['e', encodePendingEntry(entryFor('ef'.repeat(32), { visibilityOk: false }))]], // private → skip
    ['101-0', ['e', encodePendingEntry(entryFor('12'.repeat(32), { network: 'othernet' }))]], // out of scope → skip
    ['100-0', ['e', encodePendingEntry(entryFor('34'.repeat(32), { time: '2026-10-11T12:10:00.000Z' }))]], // public, new (newer than the DB row)
  ];
  configureRamStore(fake);
  const rows = [{ packet_hash: 'ab'.repeat(32), time: new Date('2026-10-11T12:04:00.000Z') }];
  const merged = await mergePendingPacketsIntoRecentRows(rows, { limit: 10, network: 'ukmesh' });
  assert.equal(merged.length, 2);
  assert.equal(merged[0]?.['packet_hash'], '34'.repeat(32)); // newer pending first
  assert.equal(merged[1]?.['packet_hash'], 'ab'.repeat(32));
  assert.equal(merged[0]?.['rx_count'], 1);
  assert.equal(merged[0]?.['tx_count'], 0);
  assert.equal(merged[0]?.['observer_iatas'] ? (merged[0]?.['observer_iatas'] as string[])[0] : null, 'EGNX');

  const withPayload = await mergePendingPacketsIntoRecentRows(rows, {
    limit: 10,
    network: 'ukmesh',
    includePayload: true,
  });
  assert.equal((withPayload[0]?.['payload'] as Record<string, unknown>)?.['direction'], 'rx');
});
