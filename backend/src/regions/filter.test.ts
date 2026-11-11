import assert from 'node:assert/strict';
import test from 'node:test';
import { ApiInputError } from '../api/errors.js';
import { parseRegionFilter } from './filter.js';

test('region validation preserves Phase A case and accepts optional hashtag', () => {
  for (const value of ['eng-ne', '#eng-ne', ' #eng-ne ']) {
    assert.equal(parseRegionFilter(value, true), 'eng-ne');
  }
  assert.equal(parseRegionFilter('#Europe', true), 'Europe');
  assert.equal(parseRegionFilter('a'.repeat(64), true), 'a'.repeat(64));
  for (const value of [undefined, null, '']) assert.equal(parseRegionFilter(value, false), undefined);
});

test('invalid and ambiguous region filters never fall back to all regions', () => {
  for (const value of [' ', '#', '##eng-ne', 'a'.repeat(65), 'north east', '%eng', "x' OR 1=1--", 'eng\n-ne', ['eng-ne'], {}, 42]) {
    assert.throws(() => parseRegionFilter(value, true), ApiInputError);
  }
  assert.throws(() => parseRegionFilter('eng-ne', false), { code: 'FEATURE_DISABLED', status: 400 });
});
