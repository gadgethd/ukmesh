import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAlertForwardPayload,
  deriveDeliveryHealth,
  summarizeAlertPayload,
} from './alert-receiver.js';

test('summarizes Alertmanager notifications without persisting annotations or labels', () => {
  const receipt = summarizeAlertPayload({
    receiver: 'operations-receiver',
    alerts: [
      {
        status: 'firing',
        labels: { alertname: 'BackendDown', secret: 'must-not-be-stored' },
        annotations: { description: 'potentially sensitive detail' },
      },
      {
        status: 'resolved',
        labels: { alertname: 'QueueCapacityHigh' },
      },
    ],
  }, new Date('2026-07-29T12:00:00.000Z'));

  assert.deepEqual(receipt, {
    received_at: '2026-07-29T12:00:00.000Z',
    source: 'alertmanager',
    status: 'firing',
    alert_names: ['BackendDown', 'QueueCapacityHigh'],
    firing: 1,
    resolved: 1,
  });
  assert.equal(JSON.stringify(receipt).includes('must-not-be-stored'), false);
});

test('summarizes synthetic alert and recovery events', () => {
  const alert = summarizeAlertPayload({
    kind: 'alert',
    service: 'meshcore-analytics',
    check: 'dependency_readiness',
  });
  const recovery = summarizeAlertPayload({
    kind: 'recovery',
    service: 'meshcore-analytics',
    check: 'dependency_readiness',
  });

  assert.equal(alert.source, 'synthetic');
  assert.equal(alert.status, 'firing');
  assert.equal(alert.firing, 1);
  assert.equal(recovery.status, 'recovery');
  assert.equal(recovery.resolved, 1);
});

test('builds a bounded Discord-compatible payload without mentions or alert details', () => {
  const receipt = summarizeAlertPayload({
    receiver: 'operations-receiver',
    alerts: [{
      status: 'firing',
      labels: { alertname: '@everyone BackendDown', secret: 'must-not-be-forwarded' },
      annotations: { description: 'potentially sensitive detail' },
    }],
  }, new Date('2026-07-29T12:00:00.000Z'));

  const payload = buildAlertForwardPayload(receipt);

  assert.match(payload.content, /UKMesh alert firing/);
  assert.match(payload.content, /@everyone BackendDown/);
  assert.match(payload.content, /Firing: 1 · Resolved: 0/);
  assert.ok(payload.content.length <= 2_000);
  assert.deepEqual(payload.allowed_mentions, { parse: [] });
  assert.equal(JSON.stringify(payload).includes('must-not-be-forwarded'), false);
  assert.equal(JSON.stringify(payload).includes('potentially sensitive detail'), false);
});

test('bounds Discord content when an Alertmanager notification has many long names', () => {
  const receipt = summarizeAlertPayload({
    alerts: Array.from({ length: 100 }, (_, index) => ({
      status: 'firing',
      labels: { alertname: `${index}-${'x'.repeat(200)}` },
    })),
  });

  assert.equal(buildAlertForwardPayload(receipt).content.length, 2_000);
});

test('reports archive-only mode as degraded even though receipt persistence is available', () => {
  assert.deepEqual(
    deriveDeliveryHealth(false, false, {
      pendingCount: 0,
      oldestQueueAgeSeconds: 0,
      deadLetterCount: 0,
      lastSuccessAt: null,
      lastError: null,
    }),
    { degraded: true, detail: 'archive-only mode: ALERT_FORWARD_URL is not configured' },
  );
});

test('reports forwarding backlog and dead letters as degraded', () => {
  const metrics = {
    pendingCount: 2,
    oldestQueueAgeSeconds: 301,
    deadLetterCount: 1,
    lastSuccessAt: '2026-09-23T09:59:00.000Z',
    lastError: 'endpoint unavailable',
  };
  assert.deepEqual(deriveDeliveryHealth(true, true, metrics), {
    degraded: true,
    detail: '1 alert(s) are dead-lettered',
  });
  assert.deepEqual(deriveDeliveryHealth(true, false, metrics), {
    degraded: true,
    detail: 'durable forwarding queue is not ready',
  });
  assert.deepEqual(deriveDeliveryHealth(true, true, {
    ...metrics,
    oldestQueueAgeSeconds: 0,
    deadLetterCount: 0,
  }), {
    degraded: true,
    detail: 'a forwarding attempt failed; retry is pending',
  });
});
