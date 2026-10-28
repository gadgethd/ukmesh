import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyErrorEvent,
  createTelemetryReporter,
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

async function withReporter(
  run: (report: ReturnType<typeof createTelemetryReporter>, calls: RequestInit[], setTime: (time: number) => void) => Promise<void>,
  respond: () => Promise<Response> = async () => new Response(null, { status: 202 }),
) {
  const restoreWindow = replaceGlobal('window', { location: { search: '', pathname: '/' } });
  const restoreNavigator = replaceGlobal('navigator', { userAgent: 'test-agent' });
  let time = 0;
  const calls: RequestInit[] = [];
  const report = createTelemetryReporter(async (_input, init) => {
    calls.push(init!);
    return respond();
  }, () => time);
  try {
    await run(report, calls, (value) => { time = value; });
  } finally {
    restoreWindow();
    restoreNavigator();
  }
}

const settleReports = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

test('telemetry deduplicates fingerprints and throttles distinct errors across all hooks', async () => {
  await withReporter(async (report, calls, setTime) => {
    const error = { kind: 'warning' as const, message: 'RF raster dataset is no longer available: dataset-2-z9' };
    for (let i = 0; i < 100; i += 1) report(error);
    await settleReports();
    assert.equal(calls.length, 1);
    report({ kind: 'error', message: 'different error' });
    await settleReports();
    assert.equal(calls.length, 1);
    setTime(10_000);
    report({ ...error, message: 'RF raster dataset is no longer available: dataset-3-z9' });
    await settleReports();
    assert.equal(calls.length, 1, 'rotating dataset counters share a fingerprint');
    report({ kind: 'crash', message: 'different error' });
    await settleReports();
    assert.equal(calls.length, 2);
  });
});

test('telemetry caps distinct reports for the entire page load', async () => {
  await withReporter(async (report, calls, setTime) => {
    for (let i = 0; i < 30; i += 1) {
      setTime(i * 10_000);
      report({ kind: 'error', message: `distinct error ${i}` });
      await settleReports();
    }
    assert.equal(calls.length, 20);
  });
});

test('telemetry stops permanently for this page after a 429', async () => {
  await withReporter(async (report, calls, setTime) => {
    report({ kind: 'warning', message: 'first' });
    await settleReports();
    setTime(3_600_000);
    report({ kind: 'error', message: 'new error after an hour' });
    await settleReports();
    assert.equal(calls.length, 1);
  }, async () => new Response(null, { status: 429 }));
});

test('telemetry backs off exponentially on HTTP and network failures without scheduling retries', async () => {
  let failures = 0;
  await withReporter(async (report, calls, setTime) => {
    report({ kind: 'error', message: 'first' });
    await settleReports();
    setTime(19_999);
    report({ kind: 'error', message: 'too early' });
    await settleReports();
    assert.equal(calls.length, 1);
    setTime(20_000);
    report({ kind: 'error', message: 'second' });
    await settleReports();
    setTime(59_999);
    report({ kind: 'error', message: 'still too early' });
    await settleReports();
    assert.equal(calls.length, 2);
    setTime(60_000);
    report({ kind: 'error', message: 'third' });
    await settleReports();
    assert.equal(calls.length, 3);
  }, async () => {
    failures += 1;
    if (failures === 1) return new Response(null, { status: 503 });
    throw new Error('offline');
  });
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
