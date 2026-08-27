import assert from 'node:assert/strict';
import test from 'node:test';
import { selectInitialViableLinks } from './initialStateLinks.js';

test('initial viable links are capped to the most recent stable snapshot', () => {
  const links = [
    { id: 'old', last_observed: '2026-08-18T00:00:00Z', observed_count: 100, multibyte_observed_count: 10 },
    { id: 'new-low', last_observed: '2026-08-20T00:00:00Z', observed_count: 1, multibyte_observed_count: 0 },
    { id: 'new-high', last_observed: '2026-08-20T00:00:00Z', observed_count: 2, multibyte_observed_count: 1 },
  ];
  assert.deepEqual(
    selectInitialViableLinks(links, 2).map((link) => link.id),
    ['new-high', 'new-low'],
  );
  assert.deepEqual(links.map((link) => link.id), ['old', 'new-low', 'new-high']);
});
