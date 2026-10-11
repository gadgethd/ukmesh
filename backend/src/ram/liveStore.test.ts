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
import { mergePendingPacketsIntoRecentRows as mergeRecentRows, type MergeOptions } from './liveRead.js';
import {
  encodeCommitMarker,
  encodePendingEntry,
  type RamPendingEntry,
} from './protocol.js';
import type { InsertPacketInput } from '../db/packetInput.js';
import { classifyForAdmission, configurePacketBatch } from '../db/packetBatch.js';

const mergePendingPacketsIntoRecentRows = <T>(rows: T[], opts: MergeOptions): Promise<T[]> =>
  mergeRecentRows(rows, {
    windowMs: Number.MAX_SAFE_INTEGER,
    resolveAliases: async () => new Map(),
    ...opts,
  });

class FakeStore implements RamStoreRedis {
  len = 0;
  bytes = 0;
  added: string[] = [];
  rev: Array<[string, string[]]> = [];
  rangeOldestFirst: Array<[string, string[]]> = [];
  xlenError: Error | null = null;
  maxPacketsSeen: unknown = null;
  maxBytesSeen: unknown = null;
  ledger = new Map<string, string>();
  committed = new Set<string>();
  loseReplyOnce = false;

  async xlen(): Promise<number> {
    if (this.xlenError) throw this.xlenError;
    return this.len;
  }

  /** Mirrors ADMIT_SCRIPT semantics against the fake's state. */
  async eval(_script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    if (this.xlenError) throw this.xlenError;
    const ledgerKey = numKeys === 3 ? String(args[2]) : '';
    const offset = numKeys;
    const maxPackets = Number(args[offset]);
    const maxBytes = Number(args[offset + 1]);
    const add = Number(args[offset + 2]);
    const value = String(args[offset + 4]);
    if (numKeys === 3 && Number(args[offset + 5]) === 1 && this.ledger.has(ledgerKey)) {
      return [1, this.ledger.get(ledgerKey)];
    }
    this.maxPacketsSeen = maxPackets;
    this.maxBytesSeen = maxBytes;
    if (this.len >= maxPackets) return -1;
    if (this.bytes + add > maxBytes) return -2;
    this.len += 1;
    this.bytes += add;
    this.added.push(value);
    if (numKeys === 3) this.ledger.set(ledgerKey, value);
    if (this.loseReplyOnce) {
      this.loseReplyOnce = false;
      throw new Error('socket closed after Redis committed the admission');
    }
    return numKeys === 3 ? [0, value] : `${1_700_000_000_000 + this.added.length}-0`;
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

  async get(): Promise<string> {
    return String(this.bytes);
  }

  async hmget(_key: string, ...fields: string[]): Promise<Array<string | null>> {
    return fields.map((field) => this.committed.has(field) ? '1700000000000' : null);
  }

  async xpending(): Promise<unknown> {
    return [0, null, null, []];
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

function entryFor(
  hash: string,
  opts: {
    visibilityOk?: boolean;
    network?: string;
    time?: string;
    rx?: string | null;
    src?: string | null;
    type?: number;
  } = {},
): RamPendingEntry {
  const seed = hash.slice(0, 32).padEnd(32, '0');
  return {
    observationId: `${seed.slice(0, 8)}-${seed.slice(8, 12)}-${seed.slice(12, 16)}-${seed.slice(16, 20)}-${seed.slice(20, 32)}`,
    admittedAtMs: 1_700_000_000_000,
    isPrivate: opts.visibilityOk === false,
    visibilityOk: opts.visibilityOk !== false,
    input: {
      time: new Date(opts.time ?? '2026-10-11T12:00:00.000Z'),
      packetHash: hash,
      rxNodeId: opts.rx === undefined ? 'cd'.repeat(32) : opts.rx,
      srcNodeId: opts.src ?? null,
      topic: 'meshcore/EGNX/OBSERVER/packets',
      topicPrefix: 'meshcore',
      iata: 'EGNX',
      packetType: opts.type ?? 4,
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
  assert.equal(fake.bytes, Buffer.byteLength(raw) * 2 + 512,
    'byte counter reserves stream, delivery ledger and bookkeeping');
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

test('F05 lost Redis reply recovers the original observation ID without a second XADD', async () => {
  const fake = new FakeStore();
  fake.loseReplyOnce = true;
  configureRamStore(fake);
  await assert.rejects(
    admitPacketToRam(sampleInput(), { key: 'client-1:42:payload-a', redelivered: false }),
    /RAM_INGEST_REFUSED:redis_unavailable/,
  );
  const originalId = JSON.parse(fake.added[0]!)['o'] as string;
  const recovered = await admitPacketToRam(sampleInput(), {
    key: 'client-1:42:payload-a', redelivered: true,
  });
  assert.equal(recovered.observationId, originalId);
  assert.equal(fake.added.length, 1);
  assert.equal(fake.len, 1);
});

test('F06 retained overlap and newer admission remain readable across a commit marker', async () => {
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
  assert.deepEqual(pending.map(({ id }) => id), ['100-0', '101-0', '103-0']);
});

test('F06 an old pending hole remains discoverable behind newer retained entries', async () => {
  const fake = new FakeStore();
  fake.rev = [
    ['103-0', ['e', encodePendingEntry(entryFor('aa'.repeat(32)))]],
    ['102-0', ['e', encodePendingEntry(entryFor('bb'.repeat(32)))]],
    ['101-0', ['e', encodePendingEntry(entryFor('cc'.repeat(32)))]],
  ];
  configureRamStore(fake);
  const seen = await readPendingEntries(1);
  assert.equal(seen.some(({ id }) => id === '101-0'), true);
});

test('F06 a captured overlap survives retention expiry after the SQL snapshot', async () => {
  await loadPublicPrivacyCache();
  const fake = new FakeStore();
  const hash = '60'.repeat(32);
  fake.rev = [['101-0', ['e', encodePendingEntry(entryFor(hash))]]];
  configureRamStore(fake);
  const pendingSnapshot = await readPendingEntries(10);
  // The SQL snapshot preceded commit. Redis cleanup then removes the copy
  // before authorization/merge. Only the captured overlap closes this gap.
  const rows: Record<string, unknown>[] = [];
  fake.rev = [];
  const opts = { limit: 10, network: 'ukmesh', pendingSnapshot };
  const merged = await mergePendingPacketsIntoRecentRows(rows, opts);
  assert.deepEqual(merged.map((row) => row['packet_hash']), [hash]);
});

test('oldest pending age is computed from the oldest entry (markers skipped)', () => {
  const entries: Array<[string, string[]]> = [
    ['900-0', ['e', encodeCommitMarker({ kind: 'commit', through: '800-0', atMs: 1, packets: 1 })]],
    ['1000-0', ['e', encodePendingEntry({ ...entryFor('aa'.repeat(32)), admittedAtMs: 1_000 })]],
    ['5000-0', ['e', encodePendingEntry({ ...entryFor('bb'.repeat(32)), admittedAtMs: 5_000 })]],
  ];
  assert.equal(oldestPendingAgeSeconds(entries, 6_000), 5, 'oldest pending is the 1000-0 entry');
  assert.equal(oldestPendingAgeSeconds([], 6_000), null);
  assert.equal(
    oldestPendingAgeSeconds([['200-0', ['e', 'not-json']]], 6_000),
    null,
    'undecodable entries are skipped',
  );
});

test('F14 pending age uses admitted time and skips committed overlap', () => {
  const young = { ...entryFor('aa'.repeat(32)), admittedAtMs: 5_000 };
  const old = { ...entryFor('bb'.repeat(32)), admittedAtMs: 1_000 };
  const entries: Array<[string, string[]]> = [
    ['100-0', ['e', encodePendingEntry(young)]],
    ['200-0', ['e', encodePendingEntry(old)]],
  ];
  assert.equal(oldestPendingAgeSeconds(entries, 6_000), 5);
  assert.equal(oldestPendingAgeSeconds(entries, 6_000, new Set(['200-0'])), 1);
  assert.equal(oldestPendingAgeSeconds(entries, 6_000, new Set(['100-0', '200-0'])), null);
});

async function loadPublicPrivacyCache(): Promise<void> {
  configurePacketBatch(async () => ({
    rows: [{ generation: '7', materialized_generation: '7', remat_active: false, prefixes: [] }],
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
  assert.equal(merged.length, 3, 'formerly private admission follows current public policy');
  assert.equal(merged[0]?.['packet_hash'], '34'.repeat(32)); // newer pending first
  assert.equal(merged[1]?.['packet_hash'], 'ab'.repeat(32));
  assert.equal(merged[2]?.['packet_hash'], 'ef'.repeat(32));
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

test('pending observations of one hash aggregate into a single row with combined observers', async () => {
  await loadPublicPrivacyCache();
  const fake = new FakeStore();
  const h = '77'.repeat(32);
  fake.rev = [
    ['300-2', ['e', encodePendingEntry({ ...entryFor(h, { rx: 'aa'.repeat(32), time: '2026-10-11T13:00:02.000Z' }), observationId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' })]],
    ['300-1', ['e', encodePendingEntry({ ...entryFor(h, { rx: 'bb'.repeat(32), time: '2026-10-11T13:00:02.000Z' }), observationId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb' })]],
    ['300-0', ['e', encodePendingEntry({ ...entryFor(h, { rx: 'cc'.repeat(32), time: '2026-10-11T13:00:02.000Z' }), observationId: 'cccccccc-cccc-cccc-cccc-cccccccccccc' })]],
  ];
  configureRamStore(fake);
  const merged = await mergePendingPacketsIntoRecentRows([], { limit: 10, network: 'ukmesh' });
  assert.equal(merged.length, 1, 'one row for one RF packet observed by three receivers');
  const row = merged[0]!;
  const observers = (row['observer_node_ids'] as string[]).map((v) => v.toUpperCase()).sort();
  assert.deepEqual(observers, ['AA'.repeat(32), 'BB'.repeat(32), 'CC'.repeat(32)].sort());
  assert.equal(row['rx_count'], 3);
  assert.equal(row['tx_count'], 0);
});

test('F12 merge deduplicates committed overlap by ID and counts a new same-observer sighting', async () => {
  await loadPublicPrivacyCache();
  const fake = new FakeStore();
  const h = '88'.repeat(32);
  fake.rev = [
    ['301-2', ['e', encodePendingEntry({
      ...entryFor(h, { rx: 'cd'.repeat(32), time: '2026-10-11T13:01:00.000Z' }),
      observationId: '11111111-1111-1111-1111-111111111111',
    })]],
    ['301-1', ['e', encodePendingEntry(entryFor(h, { rx: 'cd'.repeat(32), time: '2026-10-11T13:01:00.000Z' }))]],
    ['301-0', ['e', encodePendingEntry({
      ...entryFor(h, { rx: 'ee'.repeat(32), time: '2026-10-11T13:01:00.000Z' }),
      observationId: '22222222-2222-2222-2222-222222222222',
    })]],
  ];
  configureRamStore(fake);
  const rows = [
    {
      packet_hash: h,
      time: new Date('2026-10-11T13:00:59.000Z'),
      observer_node_ids: ['CD'.repeat(32)],
      observer_iatas: ['EGNX'],
      rx_count: 1,
      tx_count: 0,
      _observation_ids: [entryFor(h).observationId],
    },
  ];
  const merged = await mergePendingPacketsIntoRecentRows(rows, { limit: 10, network: 'ukmesh' });
  assert.equal(merged.length, 1);
  const row = merged[0]!;
  assert.equal(row['rx_count'], 3, 'new sightings count even when an observer was already present');
  assert.equal('_observation_ids' in row, false, 'internal identities are not sent to public clients');
  assert.equal((row['observer_node_ids'] as string[]).includes('EE'.repeat(32)), true);
  assert.equal((row['observer_node_ids'] as string[]).length, 2);
});

test('packetTypes restricts merged contributions (message history stays group-text only)', async () => {
  await loadPublicPrivacyCache();
  const fake = new FakeStore();
  fake.rev = [
    ['302-1', ['e', encodePendingEntry(entryFor('44'.repeat(32), { type: 4, time: '2026-10-11T13:02:00.000Z' }))]],
    ['302-0', ['e', encodePendingEntry(entryFor('55'.repeat(32), { type: 5, time: '2026-10-11T13:02:01.000Z' }))]],
  ];
  configureRamStore(fake);
  const filtered = await mergePendingPacketsIntoRecentRows([], {
    limit: 10,
    network: 'ukmesh',
    packetTypes: [5],
  });
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0]?.['packet_type'], 5);

  const all = await mergePendingPacketsIntoRecentRows([], { limit: 10, network: 'ukmesh' });
  assert.equal(all.length, 2, 'without a type filter both types merge');
});

test('merged reads drop pending entries the current privacy cache now classifies private', async () => {
  // Load a cache where the entry's observer node is private.
  configurePacketBatch(async () => ({
    rows: [{
      generation: '8', materialized_generation: '8', remat_active: false,
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

test('F01 a privacy flip overrides the loaded public cache for RAM and PG rows', async () => {
  let prefixes: Array<{ node_id: string; network: string; prefix_size_bytes: number; prefix: string }> = [];
  configurePacketBatch(async () => ({ rows: [{
    generation: '9', materialized_generation: '9', remat_active: false,
    prefixes,
  }] }));
  await classifyForAdmission({
    network: 'ukmesh', rxNodeId: 'cd'.repeat(32), srcNodeId: null,
    pathHashes: null, pathHashSizeBytes: null,
  });
  const fake = new FakeStore();
  const entry = entryFor('fe'.repeat(32), { rx: 'cd'.repeat(32) });
  fake.rev = [['100-0', ['e', encodePendingEntry(entry)]]];
  configureRamStore(fake);
  prefixes = [{ node_id: 'cd'.repeat(32).toUpperCase(), network: 'ukmesh',
    prefix_size_bytes: 1, prefix: 'CD' }];
  const oldPgRow = [{ packet_hash: entry.input.packetHash, time: entry.input.time,
    rx_node_id: entry.input.rxNodeId, src_node_id: null, network: 'ukmesh',
    path_hashes: null, path_hash_size_bytes: null }];
  assert.deepEqual(await mergePendingPacketsIntoRecentRows(oldPgRow, { limit: 10, network: 'ukmesh' }), []);
});

test('F13 RAM recent rows honor family scope, aliases, window, and slim DTO', async () => {
  await loadPublicPrivacyCache();
  const fake = new FakeStore();
  const alias = 'aa'.repeat(32).toUpperCase();
  const canonical = 'bb'.repeat(32).toUpperCase();
  fake.rev = [
    ['305-3', ['e', encodePendingEntry(entryFor('90'.repeat(32), {
      network: 'northeast', rx: alias, time: '2026-10-11T17:00:00.000Z',
    }))]],
    ['305-2', ['e', encodePendingEntry(entryFor('91'.repeat(32), {
      network: 'teesside', rx: alias, time: '2026-10-11T16:54:59.000Z',
    }))]],
    ['305-1', ['e', encodePendingEntry(entryFor('92'.repeat(32), {
      network: 'othernet', rx: alias, time: '2026-10-11T17:00:00.000Z',
    }))]],
  ];
  configureRamStore(fake);
  const rows = await mergeRecentRows([], {
    limit: 1, network: 'ukmesh', observer: canonical, fields: 'slim',
    nowMs: Date.parse('2026-10-11T17:00:00.000Z'),
    resolveAliases: async () => new Map([[alias, canonical]]),
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0]?.['packet_hash'], '90'.repeat(32));
  assert.equal(rows[0]?.['rx_node_id'], canonical);
  assert.equal('topic' in rows[0]!, false);
  assert.equal('network' in rows[0]!, false);
});
