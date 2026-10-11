import assert from 'node:assert/strict';
import test from 'node:test';
import { compareStreamIds, createRamWriter, flushDecision, type RamWriterRedis } from './writer.js';
import { decodeRamStreamValue, encodeCommitMarker, encodePendingEntry, type RamPendingEntry } from './protocol.js';
import type { PacketBatchInput } from '../db/packetBatch.js';

const STREAM = 'meshcore:ingest:pending';

function sampleInput(hash: string): PacketBatchInput {
  return {
    time: new Date('2026-10-11T12:00:00.000Z'),
    packetHash: hash,
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
    payloadJson: null,
    companionSender: null,
    rawHex: 'DEADBEEF',
    advertCount: null,
    pathHashes: null,
    pathHashSizeBytes: null,
    network: 'ukmesh',
    transportCodes: null,
    regionScope: null,
  };
}

function sampleEntry(admittedAtMs: number, hash = 'ab'.repeat(32)): RamPendingEntry {
  return {
    observationId: `obs-${hash.slice(0, 8)}-${admittedAtMs}`,
    admittedAtMs,
    isPrivate: false,
    visibilityOk: true,
    input: sampleInput(hash),
  };
}

class FakeRedis {
  entries: Array<{ id: string; fields: string[] }> = [];
  delivered = new Set<string>();
  pending = new Map<string, { consumer: string; fields: string[] }>();
  acked: string[] = [];
  deleted: string[] = [];
  groupCreated = false;
  claimCalls = 0;
  private clock = 1_700_000_000_000;

  async xgroup(command: string): Promise<unknown> {
    if (command !== 'CREATE') throw new Error('unsupported xgroup');
    if (this.groupCreated) throw new Error('BUSYGROUP Consumer Group name already exists');
    this.groupCreated = true;
    return 'OK';
  }

  async xadd(_key: string, _id: string, field: string, value: string): Promise<string> {
    const id = `${this.clock++}-0`;
    this.entries.push({ id, fields: [field, value] });
    return id;
  }

  async xreadgroup(...args: (string | number)[]): Promise<Array<[string, Array<[string, string[]]>]> | null> {
    const streamsAt = args.indexOf('STREAMS');
    const after = String(args[streamsAt + 2]);
    const countAt = args.indexOf('COUNT');
    const count = countAt >= 0 ? Number(args[countAt + 1]) : 100;
    if (after === '>') {
      const items = this.entries.filter((entry) => !this.delivered.has(entry.id)).slice(0, count);
      for (const item of items) {
        this.delivered.add(item.id);
        this.pending.set(item.id, { consumer: 'backend', fields: item.fields });
      }
      return items.length ? [[STREAM, items.map((item) => [item.id, item.fields])]] : null;
    }
    if (after === '0') {
      const items = [...this.pending.entries()]
        .filter(([id]) => this.entries.some((entry) => entry.id === id))
        .slice(0, count);
      return items.length ? [[STREAM, items.map(([id, item]) => [id, item.fields])]] : null;
    }
    return null;
  }

  async xack(_key: string, _group: string, ...ids: string[]): Promise<number> {
    for (const id of ids) this.pending.delete(id);
    this.acked.push(...ids);
    return ids.length;
  }

  async xdel(_key: string, ...ids: string[]): Promise<number> {
    this.entries = this.entries.filter((entry) => !ids.includes(entry.id));
    this.deleted.push(...ids);
    return ids.length;
  }

  async xautoclaim(): Promise<[string, Array<[string, string[]]>, string[]]> {
    this.claimCalls += 1;
    return ['0-0', [], []];
  }

  async xrange(_key: string, _min: string, _max: string, ...args: (string | number)[]): Promise<Array<[string, string[]]>> {
    const countAt = args.indexOf('COUNT');
    const count = countAt >= 0 ? Number(args[countAt + 1]) : 100;
    return [...this.entries]
      .sort((a, b) => compareStreamIds(a.id, b.id))
      .slice(0, count)
      .map((entry) => [entry.id, entry.fields]);
  }

  async xrevrange(_key: string, _max: string, _min: string, ...args: (string | number)[]): Promise<Array<[string, string[]]>> {
    const countAt = args.indexOf('COUNT');
    const count = countAt >= 0 ? Number(args[countAt + 1]) : 100;
    return [...this.entries]
      .sort((a, b) => compareStreamIds(b.id, a.id))
      .slice(0, count)
      .map((entry) => [entry.id, entry.fields]);
  }

  async xpending(_key: string, _group: string, _start: string, _end: string, count: number): Promise<Array<[string, string, number, number]>> {
    return [...this.pending.keys()].slice(0, count).map((id) => [id, 'backend', 0, 1]);
  }

  bytesReleased = 0;
  async decrby(_key: string, amount: number): Promise<number> {
    this.bytesReleased += amount;
    return -this.bytesReleased;
  }

  seedEntry(entry: RamPendingEntry): string {
    const id = `${this.clock++}-0`;
    this.entries.push({ id, fields: ['e', encodePendingEntry(entry)] });
    return id;
  }

  seedMarker(through: string): string {
    const id = `${this.clock++}-0`;
    this.entries.push({ id, fields: ['e', encodeCommitMarker({ kind: 'commit', through, atMs: this.clock, packets: 1 })] });
    return id;
  }
}

type PersistCall = { count: number; idempotent: boolean };

function makeWriter(
  fake: FakeRedis,
  now: () => number,
  persistImpl: (entries: unknown[], idempotent: boolean) => Promise<void>,
  opts: { batchSize?: number; maxAgeMs?: number } = {},
) {
  const calls: PersistCall[] = [];
  let healthy = 0;
  const writer = createRamWriter({
    redis: fake as unknown as RamWriterRedis,
    persist: async (entries, idempotent) => {
      calls.push({ count: entries.length, idempotent });
      await persistImpl(entries, idempotent);
    },
    now,
    sleep: async () => undefined,
    batchSize: () => opts.batchSize ?? 100,
    maxAgeMs: () => opts.maxAgeMs ?? 120_000,
    streamKey: () => STREAM,
    group: () => 'ingest-writers',
    consumer: () => 'backend',
    statsTick: null,
    markHealthy: () => {
      healthy += 1;
    },
    log: () => undefined,
  });
  return { writer, calls, healthyCount: () => healthy };
}

test('flushDecision: none / size / age', () => {
  const young = [{ entry: sampleEntry(1_000) }];
  assert.equal(flushDecision([], 1_000, 100, 120_000), 'none');
  assert.equal(flushDecision(young, 2_000, 100, 120_000), 'none');
  assert.equal(flushDecision(young, 2_000, 1, 120_000), 'size');
  assert.equal(flushDecision(young, 121_000, 100, 120_000), 'age');
});

test('flushes when the batch reaches the size trigger and bookkeeps the stream', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_010_000;
  fake.seedEntry(sampleEntry(now() - 500));
  fake.seedEntry(sampleEntry(now() - 400, 'cd'.repeat(32)));
  fake.seedEntry(sampleEntry(now() - 300, 'ef'.repeat(32)));

  const { writer, calls, healthyCount } = makeWriter(fake, now, async () => undefined, { batchSize: 3 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.count, 3);
  assert.equal(calls[0]?.idempotent, true); // replay-safe from the first attempt
  assert.equal(fake.acked.length, 3);
  assert.equal(fake.pending.size, 0);
  assert.equal(fake.deleted.length, 0, 'committed entries are retained for the handoff overlap');
  assert.equal(writer.accumulatorSize(), 0);
  assert.equal(healthyCount(), 1);
  assert.ok(fake.bytesReleased > 0, 'byte budget released at commit');

  // Entries are still readable (retained) plus the commit marker.
  const remaining = fake.entries.map((entry) => decodeRamStreamValue(entry.fields[1]));
  assert.equal(remaining.filter((v) => v && v.kind === 'entry').length, 3, 'entries retained pre-sweep');
  assert.ok(remaining.some((v) => v && v.kind === 'commit'));

  // The retention sweep removes committed, acknowledged, old-enough entries.
  await writer.sweepRetained();
  assert.equal(fake.deleted.length, 3, 'safe sweep deletes the committed batch');
});

test('retention sweep never deletes pending or recent entries', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_000_500; // cutoff = now - 5000 = ..._995_500 < entry ids
  const now = () => nowMs;
  fake.seedEntry(sampleEntry(nowMs - 400));
  fake.seedEntry(sampleEntry(nowMs - 300, 'cd'.repeat(32)));
  const { writer } = makeWriter(fake, now, async () => undefined, { batchSize: 2 });
  await writer.start();
  await writer.tickOnce(); // commits the two entries

  await writer.sweepRetained();
  assert.equal(fake.deleted.length, 0, 'entries inside the minimum retention window are kept');

  // A delivered-but-unacked entry from a failed batch (in the PEL): never swept.
  const failedId = '1699999999998-0';
  fake.entries.push({ id: failedId, fields: ['e', encodePendingEntry(sampleEntry(nowMs - 10_000, 'ef'.repeat(32)))] });
  fake.pending.set(failedId, { consumer: 'backend', fields: fake.entries[fake.entries.length - 1]!.fields });

  nowMs += 10_000;
  await writer.sweepRetained();
  assert.equal(fake.deleted.length, 2, 'committed, acked entries past the retention guard are swept');
  assert.equal(fake.deleted.includes(failedId), false, 'PEL entries (failed batches) must survive the sweep');
});

test('flushes when the oldest entry reaches the age trigger', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_100_000;
  fake.seedEntry(sampleEntry(now() - 200_000));

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 100, maxAgeMs: 120_000 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.count, 1);
});

test('does not flush a small, young batch', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_200_000;
  fake.seedEntry(sampleEntry(now() - 1_000));

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 100, maxAgeMs: 120_000 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 0);
  assert.equal(writer.accumulatorSize(), 1);
});

test('transient commit failure retries within the flush (idempotent throughout)', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_300_000;
  fake.seedEntry(sampleEntry(now() - 1_000));
  fake.seedEntry(sampleEntry(now() - 900, '11'.repeat(32)));

  let attempts = 0;
  const { writer, calls } = makeWriter(fake, now, async () => {
    attempts += 1;
    if (attempts === 1) throw new Error('connection terminated unexpectedly');
  }, { batchSize: 2 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(attempts, 2);
  assert.equal(calls.length, 2);
  assert.equal(calls[0]?.idempotent, true);
  assert.equal(calls[1]?.idempotent, true);
  assert.equal(fake.acked.length, 2);
});

test('exhausted commit failure leaves entries un-acked for later recovery', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_400_000;
  const now = () => nowMs;
  const seeded = fake.seedEntry(sampleEntry(nowMs - 1_000));

  let persisting = false;
  let attempts = 0;
  const { writer, calls } = makeWriter(fake, now, async () => {
    attempts += 1;
    if (!persisting) throw new Error('connection terminated unexpectedly');
  }, { batchSize: 1 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(attempts, 7); // 1 initial + 6 retries, then give up
  assert.equal(calls.length, 7);
  assert.equal(fake.acked.length, 0);
  assert.equal(fake.deleted.length, 0);
  assert.equal(fake.pending.size, 1); // still recoverable in the PEL
  assert.equal(writer.accumulatorSize(), 0);

  // Recovery: the writer re-reads its own PEL after the backoff and commits.
  persisting = true;
  nowMs += 10_000;
  await writer.tickOnce();

  assert.equal(fake.acked.includes(seeded), true);
  assert.equal(fake.pending.size, 0);
  assert.equal(calls[calls.length - 1]?.count, 1);
});

test('entries sharing one observation identity collapse within a flush', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_800_000;
  // Same identity: identical time (frozen in the entry input), hash, rx, topic, network.
  fake.seedEntry(sampleEntry(now() - 2_000, 'ab'.repeat(32)));
  fake.seedEntry(sampleEntry(now() - 1_500, 'ab'.repeat(32)));

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 2 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.count, 1, 'duplicate identity must be collapsed before persistence');
  assert.equal(fake.acked.length, 2, 'both originals are acknowledged (same observation)');
  assert.equal(writer.accumulatorSize(), 0);
});

test('markers are consumed and acknowledged without persistence', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_500_000;
  const markerId = fake.seedMarker('1699999999999-0');

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 100 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 0);
  assert.equal(fake.acked.includes(markerId), true);
});

test('unparseable stream values are acknowledged and deleted', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_600_000;
  const id = `${1_700_000_600_000}-7`;
  fake.entries.push({ id, fields: ['e', 'not-json'] });

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 100 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 0);
  assert.equal(fake.acked.includes(id), true);
  assert.equal(fake.deleted.includes(id), true);
});

test('drain flushes everything that remains', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_700_000;
  fake.seedEntry(sampleEntry(now() - 100));
  fake.seedEntry(sampleEntry(now() - 50, '22'.repeat(32)));

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 100, maxAgeMs: 600_000 });
  await writer.start();
  writer.requestStop();
  await writer.drain(5_000);

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.count, 2);
  assert.equal(fake.pending.size, 0);
  assert.equal(writer.accumulatorSize(), 0);
});
