import assert from 'node:assert/strict';
import test from 'node:test';
import { compareStreamIds, createRamWriter, flushDecision, type RamWriterRedis } from './writer.js';
import { decodeRamStreamValue, encodeCommitMarker, encodePendingEntry, type RamPendingEntry } from './protocol.js';
import type { PacketBatchInput, PersistEntry } from '../db/packetBatch.js';

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
  const seed = hash.slice(0, 32).padEnd(32, '0');
  return {
    observationId: `${seed.slice(0, 8)}-${seed.slice(8, 12)}-${seed.slice(12, 16)}-${seed.slice(16, 20)}-${seed.slice(20, 32)}`,
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
  foreignClaimReady = false;
  quarantineFails = false;
  cleanupFails = false;
  commitTimes = new Map<string, string>();
  private clock = 1_700_000_000_000;

  async xgroup(command: string): Promise<unknown> {
    if (command !== 'CREATE') throw new Error('unsupported xgroup');
    if (this.groupCreated) throw new Error('BUSYGROUP Consumer Group name already exists');
    this.groupCreated = true;
    return 'OK';
  }

  quarantined: string[] = [];
  async xtrim(_key: string, _strategy: string, threshold: number): Promise<number> {
    const removed = Math.max(0,this.quarantined.length-threshold);
    this.quarantined=this.quarantined.slice(-threshold);
    return removed;
  }
  async xadd(_key: string, idOrCommand: string, ...args: (string | number)[]): Promise<string> {
    if (idOrCommand === 'MAXLEN') {
      if (this.quarantineFails) throw new Error('quarantine unavailable');
      this.quarantined.push(String(args[4]));
      return 'q-1';
    }
    const [field, value] = args.map(String);
    const id = `${this.clock++}-0`;
    this.entries.push({ id, fields: [field!, value!] });
    return id;
  }

  async xreadgroup(...args: (string | number)[]): Promise<Array<[string, Array<[string, string[]]>]> | null> {
    if (!this.groupCreated) throw new Error('NOGROUP No such key or consumer group');
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
        .filter(([, item]) => item.consumer === 'backend')
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
    const present = ids.filter((id) => this.entries.some((entry) => entry.id === id));
    this.entries = this.entries.filter((entry) => !ids.includes(entry.id));
    this.deleted.push(...present);
    return present.length;
  }

  async xautoclaim(): Promise<[string, Array<[string, string[]]>, string[]]> {
    this.claimCalls += 1;
    if (this.foreignClaimReady) {
      const old = [...this.pending.entries()]
        .filter(([, item]) => item.consumer !== 'backend')
        .map(([id, item]) => [id, item.fields] as [string, string[]]);
      for (const [id] of old) this.pending.get(id)!.consumer = 'backend';
      return ['0-0', old, []];
    }
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

  async hset(_key: string, field: string, value: string): Promise<number> {
    this.commitTimes.set(field, value);
    return 1;
  }

  async hget(_key: string, field: string): Promise<string | null> {
    return this.commitTimes.get(field) ?? null;
  }

  async eval(_script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    assert.equal(numKeys, 4);
    if (this.cleanupFails) throw new Error('cleanup unavailable');
    if (_script.includes("local acked = redis.call('XACK'")) {
      const id = String(args[5]);
      await this.xack(String(args[0]), String(args[4]), id);
      const removed = await this.xdel(String(args[0]), id);
      if (removed) this.bytesReleased += Number(args[6]);
      return removed;
    }
    const id = String(args[4]);
    const at = Number(this.commitTimes.get(id));
    if (!Number.isFinite(at) || at > Number(args[6])) return 0;
    const removed = await this.xdel(String(args[0]), id);
    this.commitTimes.delete(id);
    if (removed) this.bytesReleased += Number(args[5]);
    return removed;
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

test('F10 destroyed group is repaired before another admitted entry is stranded', async () => {
  const fake = new FakeRedis();
  const { writer, calls } = makeWriter(fake, () => 1_700_000_100_000, async () => undefined, {
    batchSize: 1,
  });
  await writer.start();
  fake.groupCreated = false;
  fake.seedEntry(sampleEntry(1_700_000_099_000));
  await writer.tickOnce();
  assert.equal(fake.groupCreated, true);
  assert.equal(calls.length, 1);
});

test('F10 young foreign PEL is reclaimed after it ages without a restart', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_100_000;
  const entry = sampleEntry(1_700_000_099_000);
  const id = fake.seedEntry(entry);
  fake.delivered.add(id);
  fake.pending.set(id, { consumer: 'old-writer', fields: ['e', encodePendingEntry(entry)] });
  const { writer, calls } = makeWriter(fake, () => nowMs, async () => undefined, {
    batchSize: 1,
  });
  await writer.start();
  assert.equal(calls.length, 0);
  fake.foreignClaimReady = true;
  nowMs += 11_000;
  await writer.tickOnce();
  assert.equal(calls.length, 1);
  assert.equal(fake.acked.includes(id), true);
});

test('F11 malformed entries survive quarantine failure and clear from own PEL after recovery', async () => {
  const fake = new FakeRedis();
  const badId = '1699999999999-0';
  fake.entries.push({ id: badId, fields: ['e', 'not-json'] });
  const goodId = fake.seedEntry(sampleEntry(1_700_000_000_000));
  fake.quarantineFails = true;
  const { writer, calls } = makeWriter(fake, () => 1_700_000_100_000, async () => undefined, {
    batchSize: 2,
    maxAgeMs: 1,
  });
  await writer.start();
  await writer.tickOnce();
  assert.equal(fake.deleted.includes(badId), false, 'source survives failed quarantine');
  assert.equal(fake.pending.has(badId), true);
  assert.equal(calls.length, 1, 'valid entry behind malformed entry still commits');
  assert.equal(fake.acked.includes(goodId), true);

  fake.quarantineFails = false;
  await writer.tickOnce();
  assert.equal(fake.quarantined.length, 1);
  assert.equal(fake.pending.has(badId), false);
  assert.equal(fake.deleted.includes(badId), true);
});

test('F11 a permanent SQL poison row is quarantined while a valid neighbor commits', async () => {
  const fake = new FakeRedis();
  const bad = sampleEntry(1_700_000_000_000, 'aa'.repeat(32));
  const good = sampleEntry(1_700_000_000_000, 'bb'.repeat(32));
  const badId = fake.seedEntry(bad);
  const goodId = fake.seedEntry(good);
  const poison = Object.assign(new Error('invalid packet row'), { code: '23514' });
  const { writer, healthyCount, failedCount } = makeWriter(fake, () => 1_700_000_100_000, async (entries) => {
    const rows = entries as PersistEntry[];
    if (rows.length > 1 || rows[0]?.observationId === bad.observationId) throw poison;
  }, { batchSize: 2 });
  await writer.start();
  await writer.tickOnce();
  assert.equal(fake.quarantined.length, 1);
  assert.equal(fake.acked.includes(badId), true);
  assert.equal(fake.acked.includes(goodId), true);
  assert.equal(fake.deleted.includes(badId), true);
  assert.equal(fake.deleted.includes(goodId), false, 'committed overlap remains readable');
  assert.equal(fake.pending.size, 0);
  assert.equal(healthyCount(), 0, 'quarantining a poison observation cannot clear writer failure');
  assert.equal(failedCount(), 1);
});

type PersistCall = { count: number; idempotent: boolean };

function makeWriter(
  fake: FakeRedis,
  now: () => number,
  persistImpl: (entries: unknown[], idempotent: boolean) => Promise<void>,
  opts: { batchSize?: number; maxAgeMs?: number; onCommitted?: ((entries: unknown[]) => void) | null } = {},
) {
  const calls: PersistCall[] = [];
  let healthy = 0;
  let failed = 0;
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
    onCommitted: opts.onCommitted ?? null,
    markHealthy: () => {
      healthy += 1;
    },
    markFailed: () => {
      failed += 1;
    },
    log: () => undefined,
  });
  return { writer, calls, healthyCount: () => healthy, failedCount: () => failed };
}

test('flushDecision: none / size / age', () => {
  const young = [{ entry: sampleEntry(1_000) }];
  assert.equal(flushDecision([], 1_000, 100, 120_000), 'none');
  assert.equal(flushDecision(young, 2_000, 100, 120_000), 'none');
  assert.equal(flushDecision(young, 2_000, 1, 120_000), 'size');
  assert.equal(flushDecision(young, 121_000, 100, 120_000), 'age');
});

test('F14 an older reclaimed entry behind a young entry triggers the age flush', () => {
  const young = sampleEntry(10_000);
  const old = sampleEntry(1_000, 'cd'.repeat(32));
  assert.equal(flushDecision([{ entry: young }, { entry: old }], 11_000, 100, 5_000), 'age');
});

test('flushes when the batch reaches the size trigger and bookkeeps the stream', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_010_000;
  const now = () => nowMs;
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
  assert.equal(fake.bytesReleased, 0, 'retained payload remains in byte budget');

  // Entries stay readable during the handoff overlap.
  const remaining = fake.entries.map((entry) => decodeRamStreamValue(entry.fields[1]));
  assert.equal(remaining.filter((v) => v && v.kind === 'entry').length, 3, 'entries retained pre-sweep');
  assert.equal(remaining.some((v) => v && v.kind === 'commit'), false);

  // The retention sweep removes committed, acknowledged, old-enough entries.
  nowMs += 121_000;
  await writer.sweepRetained();
  assert.equal(fake.deleted.length, 3, 'safe sweep deletes the committed batch');
  assert.ok(fake.bytesReleased > 0, 'budget releases when bytes are deleted');
});

test('F06 retention starts at commit time even for an old admission', async () => {
  const fake = new FakeRedis();
  const nowMs = 1_700_000_100_000;
  const oldId = fake.seedEntry(sampleEntry(nowMs - 120_000));
  const { writer } = makeWriter(fake, () => nowMs, async () => undefined, {
    batchSize: 1,
  });
  await writer.start();
  await writer.tickOnce();
  await writer.sweepRetained();
  assert.equal(fake.deleted.includes(oldId), false, 'recent commit is retained');
});

test('F09 interrupted cleanup releases bytes exactly once on retry', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_100_000;
  const id = fake.seedEntry(sampleEntry(nowMs - 1_000));
  const { writer } = makeWriter(fake, () => nowMs, async () => undefined, {
    batchSize: 1,
  });
  await writer.start();
  await writer.tickOnce();
  nowMs += 121_000;
  fake.cleanupFails = true;
  await writer.sweepRetained();
  assert.equal(fake.entries.some((entry) => entry.id === id), true);
  assert.equal(fake.bytesReleased, 0);
  fake.cleanupFails = false;
  await writer.sweepRetained();
  const released = fake.bytesReleased;
  assert.ok(released > 0);
  await writer.sweepRetained();
  assert.equal(fake.bytesReleased, released);
  assert.equal(fake.deleted.filter((deleted) => deleted === id).length, 1);
});

test('F15 drain reports an uncommitted PEL after a permanent write failure', async () => {
  const fake = new FakeRedis();
  fake.seedEntry(sampleEntry(1_700_000_099_000));
  const { writer } = makeWriter(fake, () => 1_700_000_100_000,
    async () => { throw new Error('permanent SQL rejection'); }, { batchSize: 1 });
  await writer.start();
  await writer.tickOnce();
  await assert.rejects(writer.drain(0), /RAM_DRAIN_INCOMPLETE:pel=/);
});

test('retention sweep never deletes pending or recent entries', async () => {
  const fake = new FakeRedis();
  let nowMs = 1_700_000_000_500; // recent commits remain inside the overlap guard
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

  nowMs += 121_000;
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
  const same = sampleEntry(now() - 2_000, 'ab'.repeat(32));
  fake.seedEntry(same);
  fake.seedEntry({ ...same });

  const { writer, calls } = makeWriter(fake, now, async () => undefined, { batchSize: 2 });
  await writer.start();
  await writer.tickOnce();

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.count, 1, 'duplicate identity must be collapsed before persistence');
  assert.equal(fake.acked.length, 2, 'both originals are acknowledged (same observation)');
  assert.equal(writer.accumulatorSize(), 0);
});

test('F05 two same-millisecond sightings with distinct hop/path and IDs both persist', async () => {
  const fake = new FakeRedis();
  const at = 1_700_000_800_000;
  const first = sampleEntry(at, 'cd'.repeat(32));
  const second = {
    ...sampleEntry(at, 'cd'.repeat(32)),
    observationId: '12345678-1234-1234-1234-123456789abc',
    input: { ...first.input, hopCount: 3, pathHashes: ['ABCD'] },
  };
  fake.seedEntry(first);
  fake.seedEntry(second);
  const { writer, calls } = makeWriter(fake, () => at + 1_000, async () => undefined, {
    batchSize: 2,
  });
  await writer.start();
  await writer.tickOnce();
  assert.equal(calls[0]?.count, 2);
});

test('post-commit hook fires for the committed batch only after success', async () => {
  const fake = new FakeRedis();
  const now = () => 1_700_000_700_000;
  fake.seedEntry(sampleEntry(now() - 1_000));
  fake.seedEntry(sampleEntry(now() - 900, 'cd'.repeat(32)));

  const seen: unknown[][] = [];
  const { writer } = makeWriter(fake, now, async () => undefined, {
    batchSize: 2,
    onCommitted: (entries) => seen.push(entries),
  });
  await writer.start();
  await writer.tickOnce();

  assert.equal(seen.length, 1);
  assert.equal(seen[0]?.length, 2, 'hook receives the persisted batch');
  assert.equal(fake.acked.length, 2, 'hook runs after the acknowledgement bookkeeping');

  // A failing commit must not fire the hook.
  fake.seedEntry(sampleEntry(now() - 800, 'ef'.repeat(32)));
  fake.seedEntry(sampleEntry(now() - 700, '12'.repeat(32)));
  const failing = makeWriter(
    fake,
    now,
    async () => {
      throw new Error('connection terminated unexpectedly');
    },
    { batchSize: 2, onCommitted: (entries) => seen.push(entries) },
  );
  await failing.writer.start();
  await failing.writer.tickOnce();
  assert.equal(seen.length, 1, 'no hook on failed commits');
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

test('unparseable stream values are quarantined, acknowledged and deleted', async () => {
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
  assert.equal(fake.quarantined.length, 1, 'recovery evidence is preserved');
  const record = JSON.parse(fake.quarantined[0] ?? '{}') as Record<string, unknown>;
  assert.equal(record['src'], id);
  assert.equal(record['bytes'], Buffer.byteLength('not-json'));
  assert.equal(typeof record['sha256'], 'string');
  assert.equal((record['sha256'] as string).length, 64);
  assert.equal(record['raw'], 'not-json');
});

test('F09 quarantine caps encoded evidence bytes including JSON escaping', async () => {
  const fake = new FakeRedis();
  fake.entries.push({id:'1700000600000-0',fields:['e','\u0000'.repeat(16_000)]});
  const {writer} = makeWriter(fake,()=>1_700_000_600_000,async()=>undefined,{batchSize:1});
  await writer.start();
  await writer.tickOnce();
  assert.equal(fake.quarantined.length,1);
  assert.ok(Buffer.byteLength(fake.quarantined[0]!)<=16_384,'encoded evidence exceeds the reserved per-record bound');
  const value=JSON.parse(fake.quarantined[0]!);
  assert.equal(value.bytes,16_000);
  assert.equal(value.sha256.length,64);
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
