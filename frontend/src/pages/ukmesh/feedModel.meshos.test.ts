import assert from 'node:assert/strict';
import test from 'node:test';
import { MESHOS_SIGNED_MARKER, splitMeshosSummary } from './feedModel.js';

test('strips the MeshOS marker and flags the summary', () => {
  const result = splitMeshosSummary(`[public2] Pink Rabbit: hello mesh · ${MESHOS_SIGNED_MARKER}`);

  assert.equal(result.meshosSigned, true);
  assert.equal(result.text, '[public2] Pink Rabbit: hello mesh');
});

test('leaves ordinary summaries untouched', () => {
  const result = splitMeshosSummary('[public2] M0CNQ: morning all');

  assert.equal(result.meshosSigned, false);
  assert.equal(result.text, '[public2] M0CNQ: morning all');
});

test('marker detection is case-insensitive', () => {
  const result = splitMeshosSummary('[public2] Test: hello · meshos-signed (UNVERIFIED)');

  assert.equal(result.meshosSigned, true);
  assert.equal(result.text, '[public2] Test: hello');
});

test('empty summary stays empty', () => {
  const result = splitMeshosSummary('');

  assert.equal(result.meshosSigned, false);
  assert.equal(result.text, '');
});
