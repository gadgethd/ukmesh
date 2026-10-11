// RAM mode is enabled for this whole file before any test runs; the store
// gates its behaviour on getRamIngestMode() per call.
process.env['MQTT_INGEST_MODE'] = 'ram';

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  admitPacketToRam,
  configureRamStore,
  oldestPendingAgeSeconds,
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
import { classifyForAdmission, configurePacketBatch } from '../db/packetBatch.js';

class FakeStore implements RamStoreRedis {
  len = 0;
  bytes = 0;
  added: string[] = [];
  rev: Array<[string, string[]]> = [];
  rangeOldestFirst: Array<[string, string[]]> = [];
  xlenError: Error | null = null;
  maxPacketsSeen: unknown = null;
  maxBytesSeen: unknown = null;

  async xlen(): Promise<number> {
    if (this.xlenError) throw this.xlenError;
    return this.len;
  }

  /** Mirrors ADMIT_SCRIPT semantics against the fake's state. */
  async eval(_script: string, _numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    if (this.xlenError) throw this.xlenError;
    const maxPackets = Number(args[2]);
    const maxBytes = Number(args[3]);
    const add = Number(args[4]);
    const value = String(args[6]);
    this.maxPacketsSeen = maxPackets;
    this.maxBytesSeen = maxBytes;
    if (this.len >= maxPackets) return -1;
    if (this.bytes + add > maxBytes) return -2;
    this.len += 1;
    this.bytes += add;
    this.added.push(value);
    return `${1_700_000_000_000 + this.added.length}-0`;
  }

  async xrevrange(): Promise<Array<[string, string[]]>> {
    return this.rev;
  }

  async xrange(): Promise<Array<[string, string[]]>> {
    return this.rangeOldestFirst;
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
  fake.len = 3_000;
  fake.bytes = 33_554_432; // default MQTT_RAM_QUEUE_MAX_BYTES
  configureRamStore(fake);
  await assert.rejects(
    admitPacketToRam(sampleInput()),
    (error: unknown) => error instanceof RamAdmissionRefusedError && error.reason === 'queue_bytes',
  );
  assert.equal(fake.added.length, 0);
});

test('admission enforces both bounds atomically and accounts conservative entry bytes', async () => {
  const fake = new FakeStore();
  configureRamStore(fake);
  await admitPacketToRam(sampleInput());
  assert.equal(fake.maxPacketsSeen, 5_000);
  assert.equal(fake.maxBytesSeen, 33_554_432);
  const raw = fake.added[0] ?? '';
  assert.equal(fake.bytes, Buffer.byteLength(raw) + 256, 'byte counter = value + entry overhead');
  assert.equal(fake.len, 1, 'count increments inside the atomic step');
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

test('readPendingEntries stops at the latest commit marker (retained committed entries excluded)', async () => {
  const fake = new FakeStore();
  // Newest first: one pending entry above the newest marker; everything at or
  // below the marker is committed (retained for the overlap window).
  fake.rev = [
    ['103-0', ['e', encodePendingEntry(entryFor('03'.repeat(32)))]],
    ['102-0', ['e', encodeCommitMarker({ kind: 'commit', through: '101-0', atMs: 2, packets: 2 })]],
    ['101-0', ['e', encodePendingEntry(entryFor('01'.repeat(32)))]],
    ['100-0', ['e', encodePendingEntry(entryFor('00'.repeat(32)))]],
  ];
  configureRamStore(fake);
  const pending = await readPendingEntries(10);
  assert.equal(pending.length, 1, 'only entries newer than the newest marker are pending');
  assert.equal(pending[0]?.id, '103-0');
});

test('oldest pending age is computed from the oldest entry (markers skipped)', () => {
  const entries: Array<[string, string[]]> = [
    ['900-0', ['e', encodeCommitMarker({ kind: 'commit', through: '800-0', atMs: 1, packets: 1 })]],
    ['1000-0', ['e', encodePendingEntry(entryFor('aa'.repeat(32)))]],
    ['5000-0', ['e', encodePendingEntry(entryFor('bb'.repeat(32)))]],
  ];
  assert.equal(oldestPendingAgeSeconds(entries, 6_000), 5, 'oldest pending is the 1000-0 entry');
  assert.equal(oldestPendingAgeSeconds([], 6_000), null);
  assert.equal(
    oldestPendingAgeSeconds([['200-0', ['e', 'not-json']]], 6_000),
    null,
    'undecodable entries are skipped',
  );
});

async function loadPublicPrivacyCache(): Promise<void> {
  configurePacketBatch(async () => ({
    rows: [{ generation: '7', prefixes: [] }],
  }));
  await classifyForAdmission({
    network: 'ukmesh',
    rxNodeId: null,
    srcNodeId: null,
    pathHashes: null,
    pathHashSizeBytes: null,
  });
}

test('merged reads append only public, scoped, non-duplicate pending rows', async () => {
  await loadPublicPrivacyCache();
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

test('merged reads drop pending entries the current privacy cache now classifies private', async () => {
  // Load a cache where the entry's observer node is private.
  configurePacketBatch(async () => ({
    rows: [{
      generation: '8',
      prefixes: [{
        node_id: 'cd'.repeat(32).toUpperCase(),
        network: 'ukmesh',
        prefix_size_bytes: 1,
        prefix: 'CD',
      }],
    }],
  }));
  await classifyForAdmission({
    network: 'ukmesh',
    rxNodeId: null,
    srcNodeId: null,
    pathHashes: null,
    pathHashSizeBytes: null,
  });

  const fake = new FakeStore();
  fake.rev = [
    ['200-0', ['e', encodePendingEntry(entryFor('55'.repeat(32), { time: '2026-10-11T12:30:00.000Z' }))]],
  ];
  configureRamStore(fake);
  const merged = await mergePendingPacketsIntoRecentRows([], { limit: 10, network: 'ukmesh' });
  assert.equal(merged.length, 0, 'now-private pending entry must not appear in public merges');
});
