import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import type WebSocket from 'ws';
import { waitForInitialState } from './soakSocket.js';

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
