import assert from 'node:assert/strict';
import test from 'node:test';
import { scopeIsReady, scopeKey } from './scope.js';

test('public scope separates network, observer, region and privacy without delimiter collisions', () => {
  const base = { network: 'ukmesh', privacyGeneration: 1 };
  assert.equal(scopeKey(base), scopeKey({ ...base, observer: undefined, region: undefined }));
  const alternatives = [
    base, { ...base, network: undefined }, { ...base, network: 'test' },
    { ...base, observer: 'observer' }, { ...base, region: 'eng-ne' },
    { ...base, privacyGeneration: 2 },
    { ...base, observer: 'one|region-two', region: 'three' },
    { ...base, observer: 'one', region: 'two|region-three' },
  ];
  assert.equal(new Set(alternatives.map(scopeKey)).size, alternatives.length);
  for (const privacyGeneration of [0, -1, NaN, Infinity, 1.5]) {
    assert.equal(scopeIsReady({ ...base, privacyGeneration }), false);
  }
  assert.equal(scopeIsReady(base), true);
});
