import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { gzipSync } from 'node:zlib';
import type WebSocket from 'ws';
import { readReplayInput, soakSocketUrl, socketCloseDetails, waitForInitialState } from './soakSocket.js';
import { SoakPublication } from './soakPublication.js';

const receiver='0102'+'11'.repeat(30);
const date='2026-11-14T00:00:00.000Z';
const next='2026-11-14T00:00:01.000Z';
const topic=`meshcore/LHR/${receiver}/packets`;
const packetColumns='time,packet_hash,rx_node_id,network,topic';
const rawColumns='time,packet_hash,rx_node_id,raw_hex';
function writeInput(directory: string, packets: string[], raw: string[], headers = packetColumns) {
  return Promise.all([
    writeFile(path.join(directory,'packets.csv.gz'), gzipSync([headers,...packets].join('\n')+'\n')),
    writeFile(path.join(directory,'raw_packets.csv.gz'), gzipSync([rawColumns,...raw].join('\n')+'\n')),
  ]);
}

test('full-stack input validates paired ACK rows and preserves their identities and scope',async t=>{
  const directory=await mkdtemp(path.join(os.tmpdir(),'ukmesh-soak-socket-'));
  t.after(()=>rm(directory,{recursive:true,force:true}));
  const caseReceiver='AB'.repeat(32);
  const packets=[`${date},a,${caseReceiver},ukmesh,${topic}`,`${next},b,${caseReceiver.toLowerCase()},ukmesh,${topic}`];
  const raw=[`${date},a,${caseReceiver},000A`,`${next},b,${caseReceiver.toLowerCase()},000B`];
  await writeInput(directory,packets,raw);
  const { hashes, expected, socketScope }=await readReplayInput(directory,2,true);
  assert.deepEqual(hashes,['a','b']);
  assert.deepEqual(expected,new Set(hashes));
  assert.deepEqual(socketScope,{network:'ukmesh',observer:caseReceiver.toLowerCase(),ackRows:2});
  const socket=soakSocketUrl(new URL('http://127.0.0.1:3100/'),socketScope);
  assert.equal(socket.href,'ws://127.0.0.1:3100/ws?network=ukmesh&observer='+caseReceiver.toLowerCase());
  await assert.rejects(readReplayInput(directory,3,true),/every requested row/);
  await assert.rejects(readReplayInput(directory,1,true),/exactly the requested rows/);
  await writeInput(directory,packets,[...raw].reverse());
  await assert.rejects(readReplayInput(directory,2,true),/unmatched or unordered/);
  await writeInput(directory,[...packets].reverse(),[...raw].reverse());
  await assert.rejects(readReplayInput(directory,2,true),/unmatched or unordered/);
});

test('invalid scope, duplicate input columns and duplicate hashes refuse publication',async t=>{
  const directory=await mkdtemp(path.join(os.tmpdir(),'ukmesh-soak-socket-'));
  t.after(()=>rm(directory,{recursive:true,force:true}));
  const first=`${date},a,${receiver},ukmesh,${topic}`;
  const firstRaw=`${date},a,${receiver},000A`;
  for(const [lastReceiver,network,error] of [
    ['', 'ukmesh', /valid receiver/], ['z'.repeat(64), 'ukmesh', /valid receiver/],
    ['22'.repeat(32), 'ukmesh', /same receiver/], [receiver, 'test', /ukmesh on every row/],
  ] as const){
    await writeInput(directory,[first,`${next},b,${lastReceiver},${network},${topic}`],
      [firstRaw,`${next},b,${lastReceiver},000B`]);
    await assert.rejects(readReplayInput(directory,2,true),error);
  }
  await writeInput(directory,[first,`${next},a,${receiver},ukmesh,${topic}`],[firstRaw,`${next},a,${receiver},000B`]);
  await assert.rejects(readReplayInput(directory,2,true),/distinct hashes/);
  await writeInput(directory,[first],[firstRaw],packetColumns+',rx_node_id');
  await assert.rejects(readReplayInput(directory,1,true),/unique input columns/);
});

test('standalone calls preserve their existing unscoped network subscription',async t=>{
  const directory=await mkdtemp(path.join(os.tmpdir(),'ukmesh-soak-socket-'));
  t.after(()=>rm(directory,{recursive:true,force:true}));
  await writeInput(directory,['a'],[],'packet_hash');
  const { socketScope }=await readReplayInput(directory,1,false);
  assert.deepEqual(socketScope,{network:'ukmesh',observer:null,ackRows:1});
  assert.equal(soakSocketUrl(new URL('http://127.0.0.1:3101/anything'),socketScope).href,'ws://127.0.0.1:3101/ws?network=ukmesh');
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
