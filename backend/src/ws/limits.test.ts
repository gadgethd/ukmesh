import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BoundedAsyncGate,
  BoundedTaskQueueFullError,
  websocketAdmissionDecision,
} from './limits.js';

test('websocket admission reserves pending handshakes inside total capacity', () => {
  assert.deepEqual(
    websocketAdmissionDecision(
      { activeConnections: 7, pendingHandshakes: 2 },
      { maxConnections: 10, maxPendingHandshakes: 3 },
    ),
    { allowed: true },
  );
  assert.deepEqual(
    websocketAdmissionDecision(
      { activeConnections: 8, pendingHandshakes: 2 },
      { maxConnections: 10, maxPendingHandshakes: 3 },
    ),
    { allowed: false, statusCode: 503, reason: 'connection capacity reached' },
  );
  assert.deepEqual(
    websocketAdmissionDecision(
      { activeConnections: 1, pendingHandshakes: 3 },
      { maxConnections: 10, maxPendingHandshakes: 3 },
    ),
    { allowed: false, statusCode: 503, reason: 'too many pending handshakes' },
  );
});

test('bounded async gate caps active and queued initial-state work', async () => {
  const gate = new BoundedAsyncGate(1, 1);
  let releaseFirst!: () => void;
  const first = gate.run(() => new Promise<string>((resolve) => {
    releaseFirst = () => resolve('first');
  }));
  const second = gate.run(async () => 'second');
  await assert.rejects(
    gate.run(async () => 'third'),
    BoundedTaskQueueFullError,
  );
  assert.deepEqual(gate.stats(), { active: 1, queued: 1 });
  releaseFirst();
  assert.equal(await first, 'first');
  assert.equal(await second, 'second');
  assert.deepEqual(gate.stats(), { active: 0, queued: 0 });
});
