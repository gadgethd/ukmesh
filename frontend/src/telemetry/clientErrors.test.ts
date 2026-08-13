import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyErrorEvent,
  installClientErrorReporting,
  isNoise,
  isTelemetryDisabled,
  postTelemetry,
} from './clientErrors.js';

function replaceGlobal(name: 'window' | 'navigator' | 'fetch', value: unknown): () => void {
  const previous = Object.getOwnPropertyDescriptor(globalThis, name);
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  return () => {
    if (previous) Object.defineProperty(globalThis, name, previous);
    else Reflect.deleteProperty(globalThis, name);
  };
}

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

test('telemetry query gate is import-safe without window and matches only off', () => {
  const restoreWindow = replaceGlobal('window', undefined);
  try {
    assert.equal(isTelemetryDisabled(), false);
  } finally {
    restoreWindow();
  }

  const restoreOffWindow = replaceGlobal('window', { location: { search: '?telemetry=off' } });
  try {
    assert.equal(isTelemetryDisabled(), true);
  } finally {
    restoreOffWindow();
  }

  const restoreOtherWindow = replaceGlobal('window', { location: { search: '?telemetry=false' } });
  try {
    assert.equal(isTelemetryDisabled(), false);
  } finally {
    restoreOtherWindow();
  }
});

test('telemetry=off suppresses fetch and handler/console-hook installation', () => {
  let fetchCalls = 0;
  let handlerCalls = 0;
  const browserWindow = {
    location: { search: '?telemetry=off', pathname: '/private-query-stripped' },
    addEventListener: () => { handlerCalls += 1; },
  };
  const restoreWindow = replaceGlobal('window', browserWindow);
  const restoreNavigator = replaceGlobal('navigator', { userAgent: 'test-agent' });
  const restoreFetch = replaceGlobal('fetch', () => {
    fetchCalls += 1;
    return Promise.resolve({ ok: true });
  });
  const originalConsoleError = console.error;
  try {
    postTelemetry({ kind: 'error', message: 'suppressed' });
    installClientErrorReporting();
    assert.equal(fetchCalls, 0);
    assert.equal(handlerCalls, 0);
    assert.equal(console.error, originalConsoleError);
  } finally {
    console.error = originalConsoleError;
    restoreFetch();
    restoreNavigator();
    restoreWindow();
  }
});
