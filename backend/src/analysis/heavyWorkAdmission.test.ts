import assert from 'node:assert/strict';
import test from 'node:test';
import { withHeavyWorkAdmission } from './heavyWorkAdmission.js';

function fakeAdmission(sequence: boolean[], unlock: boolean | Error = true) {
  const calls: string[] = [];
  const client = {
    async query(sql: string) {
      if (sql.includes('pg_try_advisory_lock')) {
        calls.push('try');
        return { rows: [{ acquired: sequence.shift() ?? true }] };
      }
      if (sql === 'SELECT 42 AS value') { calls.push('query'); return { rows: [{ value: 42 }] }; }
      calls.push('unlock');
      if (unlock instanceof Error) throw unlock;
      return { rows: [{ unlocked: unlock }] };
    },
    release(error?: Error) {
      calls.push(error ? 'destroy' : 'release');
    },
  };
  return {
    calls, client,
    pool: { async connect() { calls.push('connect'); return client; } },
  };
}

test('heavy work admission waits, runs once, and releases the session claim', async () => {
  const fake = fakeAdmission([false, true]);
  const logs: string[] = [];
  const value = await withHeavyWorkAdmission({
    pool: fake.pool as never,
    workload: 'chart-refresh:ukmesh',
    retryMs: 1,
    log: { log: (line) => logs.push(String(line)), warn: (line) => logs.push(String(line)) },
    task: async () => {
      fake.calls.push('task');
      return 42;
    },
  });
  assert.equal(value, 42);
  assert.deepEqual(fake.calls, ['connect', 'try', 'try', 'task', 'unlock', 'release']);
  assert.ok(logs.some((line) => line.includes('waiting')));
  assert.ok(logs.some((line) => line.includes('released')));
});

test('heavy work admission unlocks when the protected task fails', async () => {
  const fake = fakeAdmission([true]);
  await assert.rejects(withHeavyWorkAdmission({
    pool: fake.pool as never,
    workload: 'path-learning:all',
    task: async () => { throw new Error('publication failed'); },
  }), /publication failed/);
  // A failed publication may retain session state; the owning helper discards it.
  assert.deepEqual(fake.calls, ['connect', 'try', 'unlock', 'destroy']);
});


test('the task queries on its pinned admission session without borrowing another client', async () => {
  const fake = fakeAdmission([true]);
  const value = await withHeavyWorkAdmission({
    pool: fake.pool as never,
    workload: 'region-snapshot',
    task: async session => {
      assert.equal(session, fake.client);
      return (await session.query<{ value: number }>('SELECT 42 AS value')).rows[0]!.value;
    },
  });
  assert.equal(value, 42);
  assert.deepEqual(fake.calls, ['connect', 'try', 'query', 'unlock', 'release']);
});

test('failed shared unlock destroys the session even when the task succeeds', async () => {
  for (const failure of [false, new Error('connection closed during unlock')]) {
    const fake = fakeAdmission([true], failure);
    assert.equal(await withHeavyWorkAdmission({
      pool: fake.pool as never,
      workload: 'region-snapshot',
      log: { log() {}, warn() {} },
      task: async () => 42,
    }), 42);
    assert.deepEqual(fake.calls, ['connect', 'try', 'unlock', 'destroy']);
  }
});

test('an abort racing successful acquisition cannot dispatch the task and reclaims once', async t => {
  const fake = fakeAdmission([true]);
  const shutdown = new AbortController();
  const reason = new Error('owned acquisition cancelled');
  const query = fake.client.query.bind(fake.client);
  t.mock.method(fake.client, 'query', async sql => {
    const result = await query(sql);
    if (sql.includes('pg_try_advisory_lock')) shutdown.abort(reason);
    return result;
  });
  let entered = false;
  await assert.rejects(withHeavyWorkAdmission({
    pool: fake.pool as never,
    workload: 'region-snapshot',
    signal: shutdown.signal,
    task: async () => { entered = true; },
  }), error => error === reason);
  assert.equal(entered, false);
  assert.deepEqual(fake.calls, ['connect', 'try', 'unlock', 'destroy']);
});
