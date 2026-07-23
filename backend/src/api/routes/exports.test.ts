import assert from 'node:assert/strict';
import test from 'node:test';
import { csvCell } from './exports.js';

test('csvCell escapes delimiters, quotes, and newlines', () => {
  assert.equal(csvCell('simple'), 'simple');
  assert.equal(csvCell('hello, world'), '"hello, world"');
  assert.equal(csvCell('node "alpha"'), '"node ""alpha"""');
  assert.equal(csvCell('line one\nline two'), '"line one\nline two"');
  assert.equal(csvCell(null), '');
});
