import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { auditSoakWorksetHashes,countSoakWorkset,soakWorkset } from './soakWorksetCounts.js';

test('workset retains every ACK and auxiliary canonical hash once with an exact shared digest',()=>{
  const a='A'.repeat(64),b='B'.repeat(64),c='C'.repeat(64);
  const result=soakWorkset([b,a,a.toLowerCase()],[c,b,c]);
  assert.deepEqual(result.hashes,[a,b,c]);
  assert.equal(result.countsScope.ackHashCount,2);assert.equal(result.countsScope.auxiliaryHashCount,2);
  assert.equal(result.countsScope.hashCount,3);
  assert.equal(result.countsScope.sha256,createHash('sha256').update(JSON.stringify([a,b,c])).digest('hex'));
  assert.match(result.countsScope.description,/Not global totals/);
  assert.throws(()=>soakWorkset(['not-a-packet-hash']),/invalid/);
});

test('both exact queries retain their deadline and propagate database failures', async () => {
  const queries = [
    (query: () => Promise<{ rows: unknown[] }>, now: () => number) => countSoakWorkset(query, [], { now }),
    (query: () => Promise<{ rows: unknown[] }>, now: () => number) => auditSoakWorksetHashes(query, [], [], [], now),
  ];
  for (const run of queries) {
    let clock = 0;
    const row = { packets: 0, paths: 0, facts: 0, ledger: 0, mismatch_count: 0, mismatches: [] };
    await run(async () => { clock = 30_000; return { rows: [row] }; }, () => clock);
    clock = 0;
    await assert.rejects(run(async () => { clock = 30_001; return { rows: [row] }; }, () => clock), /logical deadline/);
    clock = 0;
    await assert.rejects(run(async () => ({ rows: [{ ...row, facts: NaN }] }), () => clock), /invalid exact totals/);
    const failure = new Error('database statement timeout');
    let calls = 0;
    await assert.rejects(run(async () => { calls++; throw failure; }, () => clock), error => error === failure);
    assert.equal(calls, 1);
  }
});
