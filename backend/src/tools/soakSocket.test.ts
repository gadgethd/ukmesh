import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import type WebSocket from 'ws';
import { soakSocketScope, soakSocketUrl, socketCloseDetails, waitForInitialState } from './soakSocket.js';
import { SoakPublication } from './soakPublication.js';

const receiver='0102'+'11'.repeat(30);
const columns=['packet_hash','rx_node_id','network'];

test('full-stack scope verifies all72000 ACK rows without changing their count or input',()=>{
  const rows=Array.from({length:72000},(_,index)=>[String(index),receiver,'ukmesh']);
  const before=rows[71999]!.slice();
  const scope=soakSocketScope(columns,rows,72000,true);
  assert.deepEqual(scope,{network:'ukmesh',observer:receiver,ackRows:72000});
  assert.equal(rows.length,72000);assert.deepEqual(rows[71999],before);
  const socket=soakSocketUrl(new URL('http://127.0.0.1:3100/'),scope);
  assert.equal(socket.href,'ws://127.0.0.1:3100/ws?network=ukmesh&observer='+receiver);
  rows[71999]=['last',receiver.slice(0,-1)+'g','ukmesh'];
  assert.throws(()=>soakSocketScope(columns,rows,72000,true),/valid receiver/,'the last row is also checked');
});

test('mixed, absent, invalid receivers, wrong networks and incomplete ACK sets refuse scoped connection',()=>{
  for(const invalid of ['',receiver.slice(1),'z'.repeat(64),' '+receiver]){
    assert.throws(()=>soakSocketScope(columns,[['hash',invalid,'ukmesh']],1,true),/valid receiver/);
  }
  assert.throws(()=>soakSocketScope(columns,[['a',receiver,'ukmesh'],['b','22'.repeat(32),'ukmesh']],2,true),/same receiver/);
  for(const network of ['', 'test', 'UKMESH'])assert.throws(()=>soakSocketScope(columns,[['a',receiver,network]],1,true),/ukmesh on every row/);
  assert.throws(()=>soakSocketScope(['packet_hash'],[['a']],1,true),/receiver and network columns/);
  assert.throws(()=>soakSocketScope(['rx_node_id','network','rx_node_id'],[[receiver,'ukmesh','22'.repeat(32)]],1,true),/unique receiver/);
  assert.throws(()=>soakSocketScope(columns,[['a',receiver,'ukmesh']],72000,true),/every requested row/);
  assert.throws(()=>soakSocketScope(columns,[],0,true),/every requested row/);
  assert.deepEqual(soakSocketScope(columns,[['a','AB'.repeat(32),'ukmesh'],['b','ab'.repeat(32),'ukmesh']],2,true),
    {network:'ukmesh',observer:'ab'.repeat(32),ackRows:2});
});

test('standalone calls preserve their existing unscoped network subscription',()=>{
  const scope=soakSocketScope([],[],72000,false);
  assert.deepEqual(scope,{network:'ukmesh',observer:null,ackRows:72000});
  assert.equal(soakSocketUrl(new URL('http://127.0.0.1:3101/anything'),scope).href,'ws://127.0.0.1:3101/ws?network=ukmesh');
});

test('open alone cannot start replay; real initial_state completes readiness and releases listeners', async () => {
  const events = new EventEmitter();
  let ready = false;
  const waiting = waitForInitialState(events as unknown as WebSocket).then(() => { ready = true; });
  events.emit('open'); await Promise.resolve(); assert.equal(ready, false);
  events.emit('message', Buffer.from('{"type":"packet"}')); await Promise.resolve(); assert.equal(ready, false);
  events.emit('message', Buffer.from('{"type":"initial_state","data":{}}'));
  await waiting;
  assert.equal(events.listenerCount('message'), 0); assert.equal(events.listenerCount('close'), 0);
});

test('closed, failed or malformed subscriptions fail before publishing', async () => {
  for (const event of ['close', 'error', 'malformed']) {
    const events = new EventEmitter();
    const waiting = waitForInitialState(events as unknown as WebSocket);
    if (event === 'malformed') events.emit('message', Buffer.from('{broken'));
    else events.emit(event, new Error('lost subscription'));
    await assert.rejects(waiting);
    assert.equal(events.listenerCount('message'), 0);
  }
});

test('a silent subscription has a bounded readiness deadline', async () => {
  const events = new EventEmitter();
  await assert.rejects(waitForInitialState(events as unknown as WebSocket, 5), /initial state deadline/);
  assert.equal(events.listenerCount('message'), 0);
});

test('the actual pre-initial wire close code and bounded reason survive the failed-publication receipt',async()=>{
  const events=new EventEmitter();
  const waiting=waitForInitialState(events as unknown as WebSocket);
  const reason='initial state is temporarily unavailable';
  events.emit('close',1013,Buffer.from(reason));
  await assert.rejects(waiting,error=>{
    assert.match((error as Error).message,/code 1013/);
    const receipt=new SoakPublication().failure({socketScope:{network:'ukmesh',observer:receiver,ackRows:72000}},error);
    assert.deepEqual(receipt.error.webSocketClose,{code:1013,reason});
    assert.equal(receipt.publicationStarted,false);assert.equal(receipt.ackPublished,0);
    return true;
  });
  assert.equal(events.listenerCount('close'),0);
  assert.deepEqual(socketCloseDetails(undefined,undefined),{code:null,reason:''},'absent wire codes are never invented');
  assert.deepEqual(socketCloseDetails(1006,Buffer.from('x'.repeat(512))),{code:1006,reason:'x'.repeat(256)});
});
