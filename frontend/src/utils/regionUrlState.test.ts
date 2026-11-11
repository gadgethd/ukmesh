import assert from 'node:assert/strict';
import test from 'node:test';
import { hashWithRegion, regionFromHash } from './regionUrlState.js';
import { withScopeParams } from './api.js';

test('region hash round-trips case-sensitive names without changing other URL state', () => {
  const url = new URL('https://app.ukmesh.com/?layers=feed&node=ABC#note=keep&region=eng-ne');
  url.hash = hashWithRegion(url.hash, 'Europe');
  assert.equal(regionFromHash(url.hash), 'Europe');
  assert.equal(url.search, '?layers=feed&node=ABC');
  assert.equal(new URLSearchParams(url.hash.slice(1)).get('note'), 'keep');
  url.hash = hashWithRegion(url.hash);
  assert.equal(regionFromHash(url.hash), undefined);
  assert.equal(url.hash, '#note=keep');
  assert.equal(hashWithRegion('#region=eng-ne'), '');
  assert.equal(regionFromHash('#?region=%23eng-ne'), 'eng-ne');
});

test('invalid or ambiguous hashes do not apply a region', () => {
  for (const hash of ['', '#region=', '#region=eng-ne&region=Europe', '#region=bad%20name', `#region=${'a'.repeat(65)}`, '#region=##eng-ne']) {
    assert.equal(regionFromHash(hash), undefined);
  }
});

test('API scope adds region only when selected; existing request URLs are unchanged', () => {
  assert.equal(withScopeParams('/api/packets/recent?limit=12', { network: 'ukmesh' }), '/api/packets/recent?limit=12&network=ukmesh');
  assert.equal(withScopeParams('/api/nodes', { network: 'ukmesh', observer: 'ABC', region: 'Europe' }), '/api/nodes?network=ukmesh&observer=ABC&region=Europe');
});
