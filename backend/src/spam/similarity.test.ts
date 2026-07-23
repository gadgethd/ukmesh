import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  levenshtein,
  levenshteinRatio,
  diceCoefficient,
  jaccard,
  trigrams,
  messageSimilarity,
  usernameSimilarity,
} from './similarity.js';
import { normalizeMessage } from './normalize.js';

test('levenshtein distance basics', () => {
  assert.equal(levenshtein('kitten', 'sitting'), 3);
  assert.equal(levenshtein('', 'abc'), 3);
  assert.equal(levenshtein('same', 'same'), 0);
});

test('levenshteinRatio is 1 for identical, 0 for empty-vs-nonempty scaled', () => {
  assert.equal(levenshteinRatio('abc', 'abc'), 1);
  assert.ok(levenshteinRatio('abcd', 'abce') > 0.7);
});

test('set similarity helpers', () => {
  assert.equal(jaccard(new Set([1, 2, 3]), new Set([2, 3, 4])), 0.5);
  assert.ok(diceCoefficient(trigrams('hello'), trigrams('hallo')) > 0.3);
});

test('messageSimilarity: near-duplicates score high', () => {
  const a = normalizeMessage('Join our channel now for free stuff!!!');
  const b = normalizeMessage('join our channel now for free stuff');
  assert.ok(messageSimilarity(a, b) > 0.9, 'punctuation/case variant should be ~identical');
});

test('messageSimilarity: reordered words still cluster via token set', () => {
  const a = normalizeMessage('free crypto airdrop click here today');
  const b = normalizeMessage('click here today free crypto airdrop');
  assert.ok(messageSimilarity(a, b) > 0.8);
});

test('messageSimilarity: unrelated messages score low', () => {
  const a = normalizeMessage('whats the weather like in leeds today');
  const b = normalizeMessage('repeater offline for maintenance back soon');
  assert.ok(messageSimilarity(a, b) < 0.5);
});

test('messageSimilarity: shared canonical URL is a strong signal', () => {
  const a = normalizeMessage('deal here shop.com/x');
  const b = normalizeMessage('totally different words but shop.com/x');
  assert.ok(messageSimilarity(a, b) >= 0.85);
});

test('usernameSimilarity: similar-but-not-identical names score high', () => {
  assert.ok(usernameSimilarity('John', 'John2') > 0.8);
  assert.ok(usernameSimilarity('SpamBot', 'SpamBot_UK') > 0.8);
  assert.ok(usernameSimilarity('alice', 'alicee') > 0.8);
});

test('usernameSimilarity: clearly different names score low', () => {
  assert.ok(usernameSimilarity('Alice', 'Bob') < 0.4);
  assert.ok(usernameSimilarity('G3KFB Nick', 'Weather Station') < 0.4);
});
