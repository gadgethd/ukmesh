import assert from 'node:assert/strict';
import test from 'node:test';
import {
  parseBoundedInteger,
  parseBoolean,
  parseHexIdentifier,
} from './input.js';

test('bounded integers reject ambiguous, non-finite, fractional, negative, and oversized values', () => {
  const parse = (value: unknown) => parseBoundedInteger(value, {
    name: 'limit',
    defaultValue: 10,
    min: 1,
    max: 100,
  });
  assert.equal(parse(undefined), 10);
  assert.equal(parse('100'), 100);
  for (const value of [['1', '2'], 'NaN', 'Infinity', '1.5', '-1', '01', '101']) {
    assert.throws(() => parse(value));
  }
});

test('hashes and booleans accept only bounded canonical values', () => {
  assert.equal(parseHexIdentifier('aB12', { name: 'hash', maxLength: 8 }), 'aB12');
  assert.throws(() => parseHexIdentifier('xyz', { name: 'hash', maxLength: 8 }));
  assert.equal(parseBoolean('true', { name: 'enabled' }), true);
  assert.equal(parseBoolean('0', { name: 'enabled' }), false);
  assert.throws(() => parseBoolean('yes', { name: 'enabled' }));
});
