import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { OwnerPanelState } from './OwnerPanelState.js';
import { OwnerHeardNeighbors } from './OwnerHeardNeighbors.js';

function panel(hasData: boolean, error: string | null, loading = false) {
  return renderToStaticMarkup(React.createElement(OwnerPanelState, {
    label: 'Direct Senders', hasData, error, loading, onRetry: () => {},
    children: React.createElement('p', null, 'No direct senders observed.'),
  }));
}

test('owner panels distinguish loading and unavailable from a successful empty result', () => {
  assert.match(panel(false, null), /Loading Direct Senders/);
  assert.doesNotMatch(panel(false, null), /No direct senders observed/);
  assert.match(panel(false, 'timeout'), /role="alert"/);
  assert.match(panel(false, 'timeout'), /Retry Direct Senders/);
  assert.doesNotMatch(panel(false, 'timeout'), /No direct senders observed/);
  assert.match(panel(true, null), /No direct senders observed/);
  assert.doesNotMatch(panel(true, null), /Loading|temporarily unavailable/);
});

test('refresh failures keep prior content visibly marked as out of date', () => {
  const html = panel(true, 'HTTP 503');
  assert.match(html, /last successful data/);
  assert.match(html, /No direct senders observed/);
  assert.match(html, /Retry Direct Senders/);
});

test('background refresh keeps successful content in place without adding a loading row', () => {
  const html = panel(true, null, true);
  assert.match(html, /No direct senders observed/);
  assert.doesNotMatch(html, /Loading|Refreshing/);
});

test('neighbour panel describes packet fallback and does not misreport a published empty list', () => {
  const html = renderToStaticMarkup(React.createElement(OwnerHeardNeighbors, {
    source: 'direct_packets', neighbors: [{ id: 'peer', rssi: -95.5, snr: 0, last_seen_at: null, sampled_at: null }],
  }));
  assert.match(html, /received packets in the last 24 hours/);
  assert.match(html, /Avg RSSI/);
  assert.match(html, /-95.5 dBm/);
  assert.match(html, /0.0 dB/);
  const empty = renderToStaticMarkup(React.createElement(OwnerHeardNeighbors, { source: 'reported', neighbors: [] }));
  assert.match(empty, /latest reported neighbor list is empty/);
  assert.doesNotMatch(empty, /No neighbor list was reported/);
});
