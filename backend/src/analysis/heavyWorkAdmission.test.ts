import assert from 'node:assert/strict';
import test from 'node:test';
import { withHeavyWorkAdmission } from './heavyWorkAdmission.js';

function fakeAdmission(sequence: boolean[]) {
  const calls: string[] = [];
  const client = {
    async query(sql: string) {
      if (sql.includes('pg_try_advisory_lock')) {
        calls.push('try');
        return { rows: [{ acquired: sequence.shift() ?? true }] };
      }
      calls.push('unlock');
      return { rows: [{ unlocked: true }] };
    },
    release(error?: Error) {
      calls.push(error ? 'destroy' : 'release');
    },
  };
  return {
    calls,
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
  assert.deepEqual(fake.calls, ['connect', 'try', 'unlock', 'release']);
});
