import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { formatRfEta, RfCoverageStatus } from './RfCoverageStatus.js';
import { rfCoverageTileUrl } from './RfCoverageOverlay.js';

test('coverage tile refresh URLs are revisioned without changing the path contract', () => {
  assert.equal(
    rfCoverageTileUrl('tiles/standard/0-1.png', 'run id:3'),
    '/rf-coverage/tiles/standard/0-1.png?revision=run%20id%3A3',
  );
});

test('coverage ETA is readable on compact mobile controls', () => {
  assert.equal(formatRfEta(20), '<1 min');
  assert.equal(formatRfEta(5_400), '1h 30m');
  assert.equal(formatRfEta(undefined), null);
});

test('a disconnected worker reports queued coverage without a calculation ETA', () => {
  const now = new Date().toISOString();
  const html = renderToStaticMarkup(createElement(RfCoverageStatus, {
    meta: null,
    progress: { stage: 'waiting_for_gpu', done: 0, total: 1, percent: 0,
      message: 'Waiting for GPU worker', updated_at: now, eta_seconds: 600 },
    availableTiers: [],
    tier: 'standard',
    onTierChange: () => {},
    visible: true,
  }));
  assert.match(html, /Waiting for the GPU worker/);
  assert.doesNotMatch(html, /ETA|refresh failed/);
});
