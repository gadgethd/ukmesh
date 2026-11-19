import assert from 'node:assert/strict';
import test from 'node:test';
import { SoakPublication, soakInvocationToken } from './soakPublication.js';

test('standalone calls retain compatibility while supplied invocation identities stay exact',()=>{
  const token='a'.repeat(32);assert.equal(soakInvocationToken(token),token);
  const generated=soakInvocationToken(undefined);assert.match(generated,/^[a-f0-9-]{36}$/);
  assert.notEqual(soakInvocationToken(undefined),generated);
  assert.throws(()=>soakInvocationToken('invalid'),/UUID/);
});

test('a known failure before either publisher retains a conservative zero-publication receipt',()=>{
  const phase=new SoakPublication();phase.stage='baseline-counts';
  const receipt=phase.failure({guardCompleted:true,invocationToken:'owned',countsScope:{sha256:'owned-workset'}},
    Object.assign(new Error('counter timed out'),{code:'57014'}));
  assert.equal(receipt.passed,false);assert.equal(receipt.phase,'pre-publication');
  assert.equal(receipt.publicationStarted,false);assert.equal(receipt.ackPublished,0);
  assert.equal(receipt.auxiliaryPublished,0);assert.equal(receipt.error.code,'57014');
  assert.equal(receipt.stage,'baseline-counts');
});

test('entering either publisher latches before any send or synchronous rejection and cannot attest zero publication',()=>{
  for(const publisher of ['ACK','auxiliary']){
    const phase=new SoakPublication();
    phase.begin();
    const failure=new Error(publisher+' publisher failed before ACK receipt');
    const receipt=phase.failure({publicationStarted:false,phase:'pre-publication'},failure);
    assert.equal(receipt.publicationStarted,true);assert.equal(receipt.phase,'publication');
    assert.equal(receipt.ackPublished,null);assert.equal(receipt.auxiliaryPublished,null);
    phase.ackPublished=0;phase.auxiliaryPublished=0;
    assert.equal(phase.failure({},failure).publicationStarted,true,'zero acknowledgements do not prove no sends');
  }
});

test('later SQL failure preserves completed publication, delivery and drain without inventing reconciliation',()=>{
  const phase = new SoakPublication();
  phase.begin();
  phase.ackPublished = 72000;
  phase.auxiliaryPublished = 240;
  const context = { invocationToken: 'a'.repeat(32), countsScope: { sha256: 'owned-workset' } };
  phase.complete('publication', { ...context, passed: true, acknowledgedMs: 2400100 });
  phase.complete('delivery', { ...context, passed: true, wsPackets: 72000, elapsedMs: 2400200 });
  phase.complete('drain', { ...context, passed: true, drainElapsedMs: 4500 });
  phase.stage = 'final-verification';
  const receipt = phase.failure(context, new Error('parent-key query timed out'));
  assert.equal(receipt.passed, false);
  assert.equal(receipt.stage, 'final-verification');
  assert.equal(receipt.completedPhases.publication?.acknowledgedMs, 2400100);
  assert.equal(receipt.completedPhases.delivery?.wsPackets, 72000);
  assert.equal(receipt.completedPhases.drain?.drainElapsedMs, 4500);
  assert.equal(receipt.completedPhases.reconciliation, undefined);
  assert.throws(() => phase.complete('drain', { passed: true }), /already recorded/);
});
