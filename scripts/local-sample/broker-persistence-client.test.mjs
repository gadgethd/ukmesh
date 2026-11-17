import assert from 'node:assert/strict';
import test from 'node:test';
import {bounded} from './broker-persistence-client.mjs';

function clock(t) {
  const token={},cleared=[];let expire;
  t.mock.method(globalThis,'setTimeout',(fn,ms)=>{assert.equal(ms,6000);expire=fn;return token;});
  t.mock.method(globalThis,'clearTimeout',value=>cleared.push(value));
  return {token,cleared,expire:()=>expire()};
}

test('successful bounded wait clears its own losing deadline',async t=>{
  const c=clock(t);
  assert.equal(await bounded(Promise.resolve(42),'deadline'),42);
  assert.deepEqual(c.cleared,[c.token]);
});

test('failed bounded wait preserves the error and clears its deadline',async t=>{
  const c=clock(t),failure=new Error('original');
  await assert.rejects(bounded(Promise.reject(failure),'deadline'),error=>error===failure);
  assert.deepEqual(c.cleared,[c.token]);
});

test('six-second expiry fails and also finalizes its timer',async t=>{
  const c=clock(t),waiting=bounded(new Promise(()=>{}),'owned expiry');
  const rejected=assert.rejects(waiting,/owned expiry/);
  c.expire();await rejected;
  assert.deepEqual(c.cleared,[c.token]);
});
