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

test('both exact query owners retain30s logical budget and never retry failures',async()=>{
  for(const counter of [countSoakWorkset,auditSoakWorksetHashes]){
    let calls=0,clock=0;const totals={packets:2,paths:1,facts:1,ledger:2};
    assert.deepEqual(await counter(async()=>{calls++;clock=30000;return{rows:[totals]};},[],()=>clock),totals);
    assert.equal(calls,1);
    clock=0;
    await assert.rejects(counter(async()=>{clock=30001;return{rows:[totals]};},[],()=>clock),/logical deadline/);
    await assert.rejects(counter(async()=>({rows:[{...totals,facts:NaN}]}),[]),/invalid exact totals/);
    const failure=new Error('database statement timeout');calls=0;
    await assert.rejects(counter(async()=>{calls++;throw failure;},[]),error=>error===failure);
    assert.equal(calls,1);
  }
});
