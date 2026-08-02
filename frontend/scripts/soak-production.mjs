import { chromium } from '@playwright/test';

function boundedInteger(name, fallback, minimum, maximum) {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  if (!/^(?:0|[1-9][0-9]*)$/.test(raw.trim())) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function percentile(values, percentileValue) {
  if (values.length === 0) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  const index = Math.min(
    ordered.length - 1,
    Math.max(0, Math.ceil(percentileValue / 100 * ordered.length) - 1),
  );
  return ordered[index];
}

function median(values) {
  if (values.length === 0) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  const middle = Math.floor(ordered.length / 2);
  return ordered.length % 2 === 0
    ? (ordered[middle - 1] + ordered[middle]) / 2
    : ordered[middle];
}

const BROWSER_PERFORMANCE_NODE_NAMES = new Set([
  'DOMRectReadOnly',
  'LayoutShift',
  'LayoutShiftAttribution',
  'PerformanceLongAnimationFrameTiming',
  'PerformanceLongTaskTiming',
  'PerformanceResourceTiming',
  'PerformanceServerTiming',
  'TaskAttributionTiming',
]);

const V8_RUNTIME_NATIVE_NODE_NAMES = new Set([
  'system / ProtectedFixedArray',
  'system / SharedFunctionInfoWrapper',
  'system / WeakArrayList',
  'system / WeakCell',
]);

function isRuntimeHeapNode(type, name) {
  return type === 'code'
    || type === 'hidden'
    || type === 'object shape'
    || type === 'synthetic'
    || (type === 'native' && (
      BROWSER_PERFORMANCE_NODE_NAMES.has(name)
      || V8_RUNTIME_NATIVE_NODE_NAMES.has(name)
    ));
}

function summarizeHeapSnapshot(rawSnapshot) {
  const snapshot = JSON.parse(rawSnapshot);
  const fields = snapshot.snapshot.meta.node_fields;
  const nodeTypes = snapshot.snapshot.meta.node_types[0];
  const stride = fields.length;
  const typeIndex = fields.indexOf('type');
  const nameIndex = fields.indexOf('name');
  const selfSizeIndex = fields.indexOf('self_size');
  if (typeIndex < 0 || nameIndex < 0 || selfSizeIndex < 0 || !Array.isArray(nodeTypes)) {
    throw new Error('Unsupported V8 heap snapshot format');
  }

  const summary = {
    retainedDataBytes: 0,
    runtimeBytes: 0,
    totalBytes: 0,
    nodes: 0,
  };
  for (let index = 0; index < snapshot.nodes.length; index += stride) {
    const type = nodeTypes[snapshot.nodes[index + typeIndex]];
    const name = snapshot.strings[snapshot.nodes[index + nameIndex]];
    const selfSize = snapshot.nodes[index + selfSizeIndex];
    summary.totalBytes += selfSize;
    summary.nodes += 1;
    if (isRuntimeHeapNode(type, name)) summary.runtimeBytes += selfSize;
    else summary.retainedDataBytes += selfSize;
  }
  return summary;
}

async function takeHeapSummary(cdp) {
  await cdp.send('HeapProfiler.collectGarbage');
  const chunks = [];
  const onChunk = ({ chunk }) => chunks.push(chunk);
  cdp.on('HeapProfiler.addHeapSnapshotChunk', onChunk);
  try {
    await cdp.send('HeapProfiler.takeHeapSnapshot', {
      reportProgress: false,
      captureNumericValue: true,
    });
  } finally {
    cdp.off('HeapProfiler.addHeapSnapshotChunk', onChunk);
  }
  return summarizeHeapSnapshot(chunks.join(''));
}

function growthRatio(baseline, final) {
  return baseline > 0 ? (final - baseline) / baseline : Number.POSITIVE_INFINITY;
}

const baseUrl = String(process.env.SOAK_BASE_URL ?? 'http://127.0.0.1:3003').replace(/\/$/, '');
const durationMs = boundedInteger('SOAK_DURATION_MS', 30 * 60_000, 60_000, 2 * 60 * 60_000);
const warmupMs = boundedInteger(
  'SOAK_WARMUP_MS',
  Math.min(2 * 60_000, Math.floor(durationMs / 3)),
  10_000,
  durationMs - 30_000,
);
const sampleIntervalMs = boundedInteger('SOAK_SAMPLE_INTERVAL_MS', 30_000, 5_000, 5 * 60_000);
const maximumHeapGrowth = Number(process.env.SOAK_MAX_HEAP_GROWTH_RATIO ?? 0.10);
if (!Number.isFinite(maximumHeapGrowth) || maximumHeapGrowth < 0 || maximumHeapGrowth > 1) {
  throw new Error('SOAK_MAX_HEAP_GROWTH_RATIO must be between 0 and 1');
}

const browser = await chromium.launch({
  headless: true,
  args: ['--enable-precise-memory-info'],
});
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  serviceWorkers: 'allow',
});
const page = await context.newPage();
const cdp = await context.newCDPSession(page);
await cdp.send('Performance.enable');
await cdp.send('HeapProfiler.enable');

await page.addInitScript(() => {
  const state = {
    mapConstructions: 0,
    longTasks: [],
  };
  Object.defineProperty(window, '__meshcoreSoak', {
    value: state,
    configurable: false,
    enumerable: false,
    writable: false,
  });
  const seenMaps = new WeakSet();
  const scan = (root) => {
    const candidates = [];
    if (root instanceof Element && root.matches('.maplibregl-map')) candidates.push(root);
    if (root instanceof Element || root instanceof Document) {
      candidates.push(...root.querySelectorAll('.maplibregl-map'));
    }
    for (const candidate of candidates) {
      if (seenMaps.has(candidate)) continue;
      seenMaps.add(candidate);
      state.mapConstructions += 1;
    }
  };
  const start = () => {
    scan(document);
    new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        if (mutation.type === 'attributes') scan(mutation.target);
        for (const node of mutation.addedNodes) scan(node);
      }
    }).observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
      childList: true,
      subtree: true,
    });
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start, { once: true });
  } else {
    start();
  }
  if ('PerformanceObserver' in window) {
    try {
      new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          state.longTasks.push({
            startTime: entry.startTime,
            duration: entry.duration,
          });
          if (state.longTasks.length > 10_000) state.longTasks.shift();
        }
      }).observe({ type: 'longtask', buffered: true });
    } catch {
      // Older Chromium builds may not expose longtask entries.
    }
  }
});

let requests = 0;
let failedRequests = 0;
let serverErrors = 0;
const pageErrors = [];
page.on('request', () => { requests += 1; });
page.on('requestfailed', () => { failedRequests += 1; });
page.on('response', (response) => {
  if (response.status() >= 500) serverErrors += 1;
});
page.on('pageerror', (error) => {
  pageErrors.push(String(error.message).slice(0, 500));
});

async function snapshot(elapsedMs) {
  await cdp.send('HeapProfiler.collectGarbage');
  const metricsResponse = await cdp.send('Performance.getMetrics');
  const metrics = Object.fromEntries(
    metricsResponse.metrics.map(({ name, value }) => [name, value]),
  );
  const browserState = await page.evaluate(async () => {
    const cacheNames = 'caches' in window ? await caches.keys() : [];
    const cacheEntries = {};
    for (const name of cacheNames) {
      const cache = await caches.open(name);
      cacheEntries[name] = (await cache.keys()).length;
    }
    const storage = navigator.storage?.estimate
      ? await navigator.storage.estimate()
      : {};
    const soak = window.__meshcoreSoak ?? { mapConstructions: 0, longTasks: [] };
    return {
      mapInstances: document.querySelectorAll('.maplibregl-map').length,
      mapConstructions: soak.mapConstructions,
      longTaskCount: soak.longTasks.length,
      longTaskDurationMs: soak.longTasks.reduce((sum, task) => sum + task.duration, 0),
      longestTaskMs: soak.longTasks.reduce((maximum, task) => Math.max(maximum, task.duration), 0),
      cacheEntries,
      storageUsageBytes: storage.usage ?? null,
      storageQuotaBytes: storage.quota ?? null,
      visibilityState: document.visibilityState,
    };
  });
  return {
    elapsedMs,
    heapBytes: metrics.JSHeapUsedSize ?? 0,
    domNodes: metrics.Nodes ?? 0,
    documents: metrics.Documents ?? 0,
    eventListeners: metrics.JSEventListeners ?? 0,
    ...browserState,
  };
}

const navigationStartedAt = Date.now();
const targetUrl = new URL(baseUrl);
targetUrl.searchParams.set('performance-soak', '1');
await page.goto(targetUrl.toString(), { waitUntil: 'networkidle', timeout: 60_000 });
await page.waitForSelector('.maplibregl-map', { timeout: 60_000 });

const startedAt = Date.now();
const samples = [];
let baselineHeapSummary = null;
while (Date.now() - startedAt < durationMs) {
  const elapsedMs = Date.now() - startedAt;
  samples.push(await snapshot(elapsedMs));
  if (baselineHeapSummary === null && elapsedMs >= warmupMs) {
    baselineHeapSummary = await takeHeapSummary(cdp);
  }
  const remainingMs = durationMs - (Date.now() - startedAt);
  if (remainingMs <= 0) break;
  await page.waitForTimeout(Math.min(sampleIntervalMs, remainingMs));
}
samples.push(await snapshot(Date.now() - startedAt));
baselineHeapSummary ??= await takeHeapSummary(cdp);
const finalHeapSummary = await takeHeapSummary(cdp);

const postWarmup = samples.filter((sample) => sample.elapsedMs >= warmupMs);
const comparisonWidth = Math.min(3, Math.max(1, Math.floor(postWarmup.length / 2)));
const baselineSamples = postWarmup.slice(0, comparisonWidth);
const finalSamples = postWarmup.slice(-comparisonWidth);
const observedBaselineHeapBytes = median(baselineSamples.map((sample) => sample.heapBytes));
const observedFinalHeapBytes = median(finalSamples.map((sample) => sample.heapBytes));
const retainedHeapGrowthRatio = growthRatio(
  baselineHeapSummary.retainedDataBytes,
  finalHeapSummary.retainedDataBytes,
);
const runtimeHeapGrowthRatio = growthRatio(
  baselineHeapSummary.runtimeBytes,
  finalHeapSummary.runtimeBytes,
);
const totalSnapshotGrowthRatio = growthRatio(
  baselineHeapSummary.totalBytes,
  finalHeapSummary.totalBytes,
);
const observedHeapGrowthRatio = growthRatio(observedBaselineHeapBytes, observedFinalHeapBytes);
const longestTasks = samples.map((sample) => sample.longestTaskMs);
const maximumMapInstances = Math.max(...samples.map((sample) => sample.mapInstances));
const maximumMapConstructions = Math.max(...samples.map((sample) => sample.mapConstructions));

const checks = {
  retainedHeap: retainedHeapGrowthRatio <= maximumHeapGrowth,
  oneMapInstance: maximumMapInstances === 1 && maximumMapConstructions === 1,
  noPageErrors: pageErrors.length === 0,
  noServerErrors: serverErrors === 0,
  visibleThroughout: samples.every((sample) => sample.visibilityState === 'visible'),
};
const report = {
  format: 'meshcore-browser-soak-v1',
  baseUrl,
  navigationStartedAt: new Date(navigationStartedAt).toISOString(),
  startedAt: new Date(startedAt).toISOString(),
  completedAt: new Date().toISOString(),
  durationMs: Date.now() - startedAt,
  warmupMs,
  sampleIntervalMs,
  samples: samples.length,
  heap: {
    metric: 'v8-heap-snapshot-self-size-v1',
    baselineBytes: baselineHeapSummary.retainedDataBytes,
    finalBytes: finalHeapSummary.retainedDataBytes,
    growthRatio: Number(retainedHeapGrowthRatio.toFixed(6)),
    limitRatio: maximumHeapGrowth,
    runtime: {
      baselineBytes: baselineHeapSummary.runtimeBytes,
      finalBytes: finalHeapSummary.runtimeBytes,
      growthRatio: Number(runtimeHeapGrowthRatio.toFixed(6)),
    },
    totalSnapshot: {
      baselineBytes: baselineHeapSummary.totalBytes,
      finalBytes: finalHeapSummary.totalBytes,
      growthRatio: Number(totalSnapshotGrowthRatio.toFixed(6)),
    },
    observedJsHeap: {
      baselineBytes: observedBaselineHeapBytes,
      finalBytes: observedFinalHeapBytes,
      growthRatio: Number(observedHeapGrowthRatio.toFixed(6)),
    },
  },
  browser: {
    maximumMapInstances,
    maximumMapConstructions,
    maximumDomNodes: Math.max(...samples.map((sample) => sample.domNodes)),
    maximumDocuments: Math.max(...samples.map((sample) => sample.documents)),
    maximumEventListeners: Math.max(...samples.map((sample) => sample.eventListeners)),
    longTaskCount: samples.at(-1)?.longTaskCount ?? 0,
    longTaskDurationMs: Number((samples.at(-1)?.longTaskDurationMs ?? 0).toFixed(3)),
    longestTaskMs: Number(percentile(longestTasks, 100).toFixed(3)),
  },
  network: {
    requests,
    failedRequests,
    serverErrors,
  },
  storage: {
    cacheEntries: samples.at(-1)?.cacheEntries ?? {},
    usageBytes: samples.at(-1)?.storageUsageBytes ?? null,
    quotaBytes: samples.at(-1)?.storageQuotaBytes ?? null,
  },
  sampleSeries: samples,
  pageErrors,
  checks,
  status: Object.values(checks).every(Boolean) ? 'passed' : 'failed',
};

console.log(JSON.stringify(report, null, 2));
await context.close();
await browser.close();
if (report.status !== 'passed') process.exitCode = 1;
