import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { ConfirmedNeighbours, NeighbourContact, sortNeighbourLinks, timeAgo, type NodeLink } from './ConfirmedNeighbours.js';

const now = Date.parse('2026-09-07T12:00:00.000Z');
const day = 86400_000;
const link: NodeLink = {
  peer_id: 'A'.repeat(64), peer_name: 'Quiet repeater', observed_count: 107238,
  itm_path_loss_db: 112.1, count_this_to_peer: 64146, count_peer_to_this: 43092,
  last_observed: new Date(now - 2 * day).toISOString(), recent_count_72h: 0,
};

function contact(overrides: Partial<NodeLink> = {}) {
  return renderToStaticMarkup(React.createElement(NeighbourContact, { link: { ...link, ...overrides }, now }));
}

test('last-contact dots follow the exact 24-hour and seven-day boundaries', () => {
  for (const [age, colour, relative] of [
    [day - 1, 'online', '23h ago'], [day, 'amber', '1d ago'],
    [7 * day, 'amber', '7d ago'], [7 * day + 1, 'danger', '7d ago'],
  ] as const) {
    const iso = new Date(now - age).toISOString();
    const html = contact({ last_observed: iso });
    assert.ok(html.includes(`background:var(--${colour})`));
    assert.ok(html.includes(`dateTime="${iso}"`));
    assert.ok(html.includes(relative));
    assert.match(html, /Last contact:/);
  }
});

test('frequency depends only on recent contact, including zero with a high lifetime count', () => {
  for (const [count, bucket] of [[0, 'RARE'], [1, 'OCCASIONAL'], [9, 'OCCASIONAL'], [10, 'ACTIVE'], [1001, 'ACTIVE']] as const) {
    const html = contact({ recent_count_72h: count });
    assert.ok(html.includes(`>${bucket}</strong>`));
    assert.ok(html.includes(`${count} ${count === 1 ? 'contact' : 'contacts'} in last 72h`));
  }
});

test('unknown timestamps are neutral and clock skew never produces negative relative times', () => {
  for (const last_observed of [null, '', 'invalid']) {
    const html = contact({ last_observed });
    assert.match(html, /Last contact: Unknown/);
    assert.ok(html.includes('background:var(--text-muted)'));
    assert.doesNotMatch(html, /<time|NaN|Invalid Date/);
  }
  assert.match(contact({ last_observed: new Date(now + day).toISOString() }), /0s ago/);
  assert.equal(timeAgo('invalid', now), 'Unknown');
  for (const [age, relative] of [[15_000, '15s ago'], [120_000, '2m ago'], [7200_000, '2h ago'], [2 * day, '2d ago']] as const) {
    assert.equal(timeAgo(new Date(now - age).toISOString(), now), relative);
  }
});

test('sort modes use recency or 72-hour frequency, leave cached input intact, and put unknown contact last', () => {
  const input: NodeLink[] = [
    { ...link, peer_id: 'unknown', last_observed: null },
    { ...link, peer_id: 'frequent', recent_count_72h: 20 },
    { ...link, peer_id: 'newest', last_observed: new Date(now).toISOString(), recent_count_72h: 2 },
  ];
  const snapshot = structuredClone(input);
  assert.deepEqual(sortNeighbourLinks(input, 'contact').map(x => x.peer_id), ['newest', 'frequent', 'unknown']);
  assert.deepEqual(sortNeighbourLinks(input, 'frequency').map(x => x.peer_id), ['frequent', 'newest', 'unknown']);
  assert.deepEqual(input, snapshot);
  assert.deepEqual(sortNeighbourLinks([{ ...link, peer_id: 'b', last_observed: null }, { ...link, peer_id: 'a', last_observed: null }], 'contact').map(x => x.peer_id), ['a', 'b']);
});

test('card renders contact, lifetime context, sort labels, empty state and safe peer names', () => {
  const html = renderToStaticMarkup(React.createElement(ConfirmedNeighbours, { links: [{ ...link, peer_name: '<script>bad</script>' }] }));
  for (const text of ['107238× seen', '112 dB loss', 'TX: 64146', 'RX: 43092', 'RARE', 'Sort neighbours', 'Last contact', 'Most frequent', 'Contact includes relayed traffic.']) {
    assert.ok(html.includes(text), text);
  }
  assert.ok(html.includes('&lt;script&gt;bad&lt;/script&gt;'));
  assert.ok(html.includes('flex-wrap:wrap'));
  assert.ok(html.includes('min-width:0;overflow-wrap:anywhere'));
  assert.match(renderToStaticMarkup(React.createElement(ConfirmedNeighbours, { links: [] })), /No neighbours found/);
});
