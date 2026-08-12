import { test } from 'node:test';
import assert from 'node:assert/strict';
import { classifyErrorEvent, isNoise } from './clientErrors.js';

test('classifies runtime error events by message', () => {
  const result = classifyErrorEvent({ message: 'boom', error: new Error('boom') });
  assert.equal(result?.kind, 'error');
  assert.equal(result?.message, 'boom');
});

test('classifies promise rejections as unhandledrejection', () => {
  const result = classifyErrorEvent({ reason: new Error('nope') });
  assert.equal(result?.kind, 'unhandledrejection');
  assert.equal(result?.message, 'nope');
});

test('wraps non-Error rejection reasons', () => {
  const result = classifyErrorEvent({ reason: 'string reason' });
  assert.equal(result?.kind, 'unhandledrejection');
  assert.equal(result?.message, 'string reason');
});

test('classifies resource failures from element targets', () => {
  const result = classifyErrorEvent({
    target: { tagName: 'IMG', src: 'https://cdn.example.com/tiles/1.png' },
  });
  assert.equal(result?.kind, 'error');
  assert.equal(result?.message, 'Resource failed: img /tiles/1.png');
});

test('uses href for link element targets', () => {
  const result = classifyErrorEvent({
    target: { tagName: 'LINK', href: 'https://assets.example.com/app.css' },
  });
  assert.equal(result?.message, 'Resource failed: link /app.css');
});

test('returns null for empty-message events without element target', () => {
  assert.equal(classifyErrorEvent({}), null);
  assert.equal(classifyErrorEvent(null), null);
});

test('truncates long messages to 500 chars', () => {
  const result = classifyErrorEvent({ message: 'x'.repeat(2000) });
  assert.equal(result?.message.length, 500);
});

test('noise filter matches terrain-tiles and misses real bugs', () => {
  assert.equal(isNoise('Failed to load /terrain-tiles/12/345/678.png'), true);
  assert.equal(isNoise('real bug'), false);
});
