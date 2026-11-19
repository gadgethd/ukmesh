import assert from 'node:assert/strict';
import test from 'node:test';
import type pg from 'pg';
import { readScopedPackets, type ScopedPacketRow } from './scopedPackets.js';

const settings = { windowDays: 92, maxPackets: 4_000, statementTimeoutMs: 120_000 };

function records(start: number, length: number): ScopedPacketRow[] {
  return Array.from({ length }, (_, index) => ({
    packet_hash: String(start + index), t0: new Date(0), t1: new Date(0),
    raw: null, elems: null, observers: null,
  }));
}

function reader(batches: ScopedPacketRow[][], failures: { fetch?: Error; rollback?: Error } = {},
  onQuery?: (text: string) => void) {
  const statements: string[] = [];
  const parameters: unknown[][] = [];
  const releases: boolean[] = [];
  let fetches = 0;
  const client = {
    async query(query: string | pg.QueryConfig, values?: unknown[]) {
      const text = typeof query === 'string' ? query : query.text;
      statements.push(text);
      parameters.push(values ?? (typeof query === 'string' ? undefined : query.values) ?? []);
      onQuery?.(text);
      if (text === 'ROLLBACK' && failures.rollback) throw failures.rollback;
      if (text.startsWith('FETCH ') && fetches === 1 && failures.fetch) throw failures.fetch;
      return { rows: text.startsWith('FETCH ') ? batches[fetches++] ?? [] : [] };
    },
    release(destroy: boolean) { releases.push(destroy); },
  };
  return { pool: { connect: async () => client as unknown as pg.PoolClient } as Pick<pg.Pool, 'connect'>,
    statements, parameters, releases, fetches: () => fetches };
}

test('reads whole scoped packets in cursor order and ends the read-only transaction', async () => {
  const source = reader([records(0, 2_048), records(2_048, 10)]);
  const order: string[] = [];
  assert.equal(await readScopedPackets(source.pool, settings, row => order.push(row.packet_hash)), 2_058);
  assert.deepEqual(order, Array.from({ length: 2_058 }, (_, index) => String(index)));
  assert.equal(source.statements[0], 'BEGIN READ ONLY');
  const declaration = source.statements.findIndex(text => text.startsWith('DECLARE '));
  assert.deepEqual(source.parameters[declaration], [92, 4_001]);
  assert.equal(source.fetches(), 2);
  assert.equal(source.statements.at(-1), 'ROLLBACK');
  assert.deepEqual(source.releases, [false]);
});

test('rejects the capacity sentinel and rolls back without visiting the excess batch', async () => {
  const source = reader([records(0, 2_048), records(2_048, 1)]);
  const visited: string[] = [];
  await assert.rejects(readScopedPackets(source.pool, { ...settings, maxPackets: 2_048 },
    row => visited.push(row.packet_hash)), /REGION_SCOPE_PACKET_CAPACITY/);
  assert.equal(visited.length, 2_048);
  assert.equal(source.statements.at(-1), 'ROLLBACK');
  assert.deepEqual(source.releases, [false]);
});

test('a processing failure closes the cursor, and a rollback failure discards its session', async () => {
  const failure = new Error('processing failed');
  const source = reader([records(0, 2_048), records(2_048, 1)]);
  await assert.rejects(readScopedPackets(source.pool, settings, () => { throw failure; }), error => error === failure);
  assert.equal(source.fetches(), 1);
  assert.equal(source.statements.at(-1), 'ROLLBACK');
  assert.deepEqual(source.releases, [false]);

  const broken = reader([], { rollback: new Error('rollback failed') });
  await assert.rejects(readScopedPackets(broken.pool, settings, () => assert.fail('unexpected row')), /rollback failed/);
  assert.deepEqual(broken.releases, [true]);
});

test('a database read timeout discards the connection without queuing rollback', async () => {
  const failure = new Error('Query read timeout');
  const source = reader([records(0, 2_048)], { fetch: failure });
  await assert.rejects(readScopedPackets(source.pool, settings, () => {}), error => error === failure);
  assert.equal(source.statements.some(text => text === 'ROLLBACK'), false);
  assert.deepEqual(source.releases, [true]);
});

test('database setup and earlier batches consume the same read budget', async t => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  const source = reader([records(0, 2_048)], {}, text => {
    if (text.includes('set_config')) now += 450;
    if (text.startsWith('FETCH ')) now += 400;
  });
  await assert.rejects(readScopedPackets(source.pool, { ...settings, statementTimeoutMs: 1_500 }, () => {}),
    /REGION_SCOPE_READ_DEADLINE/);
  assert.equal(source.fetches(), 1);
  assert.equal(source.statements.at(-1), 'ROLLBACK');
  assert.deepEqual(source.releases, [false]);
});
