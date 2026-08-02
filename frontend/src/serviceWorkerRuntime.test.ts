import assert from 'node:assert/strict';
import test from 'node:test';
import {
  activateServiceWorkerUpdate,
  getServiceWorkerUpdateSnapshot,
  registerServiceWorker,
  resetServiceWorkerUpdatesForTests,
  serviceWorkerScriptUrl,
} from './serviceWorkerUpdates.js';

const runtimeModulePath: string = new URL('../public/sw-runtime.js', import.meta.url).href;

type RequestLike = Request | string | { url: string };

function requestUrl(value: RequestLike, origin: string): string {
  if (typeof value === 'string') return new URL(value, origin).toString();
  return new URL(value.url, origin).toString();
}

class FakeCache {
  readonly values = new Map<string, Response>();
  keysCalls = 0;

  constructor(readonly origin: string) {}

  async match(request: RequestLike): Promise<Response | undefined> {
    return this.values.get(requestUrl(request, this.origin))?.clone();
  }

  async put(request: RequestLike, response: Response): Promise<void> {
    this.values.set(requestUrl(request, this.origin), response.clone());
  }

  async delete(request: RequestLike): Promise<boolean> {
    return this.values.delete(requestUrl(request, this.origin));
  }

  async keys(): Promise<Request[]> {
    this.keysCalls += 1;
    return [...this.values.keys()].map((url) => new Request(url));
  }
}

class FakeCacheStorage {
  readonly stores = new Map<string, FakeCache>();

  constructor(readonly origin: string) {}

  async open(name: string): Promise<FakeCache> {
    let cache = this.stores.get(name);
    if (!cache) {
      cache = new FakeCache(this.origin);
      this.stores.set(name, cache);
    }
    return cache;
  }

  async keys(): Promise<string[]> {
    return [...this.stores.keys()];
  }

  async delete(name: string): Promise<boolean> {
    return this.stores.delete(name);
  }
}

async function loadRuntimeModule(): Promise<{
  createServiceWorkerRuntime: (environment: Record<string, unknown>) => {
    policy: Record<string, string | number>;
    install: () => Promise<void>;
    activate: () => Promise<void>;
    handleFetch: (
      request: RequestLike & { mode?: string },
      waitUntil?: (promise: Promise<unknown>) => void,
    ) => Promise<Response>;
    handleMessage: (message: unknown, source?: { postMessage?: (value: unknown) => void }) => Promise<void>;
    cleanupOldCaches: (force?: boolean) => Promise<void> | undefined;
    snapshot: () => Promise<{ tileCache: { entries: number; bytes: number } }>;
  };
}> {
  return import(runtimeModulePath);
}

function createRuntimeFixture(options: {
  fetch?: (request: RequestLike) => Promise<Response>;
  now?: () => number;
  policy?: Record<string, number>;
} = {}) {
  const origin = 'https://mesh.test';
  const cacheStorage = new FakeCacheStorage(origin);
  let claims = 0;
  let skipWaitingCalls = 0;
  const messages: unknown[] = [];
  const clients = {
    claim: async () => { claims += 1; },
    matchAll: async () => [{ postMessage: (value: unknown) => messages.push(value) }],
  };
  return loadRuntimeModule().then(({ createServiceWorkerRuntime }) => ({
    cacheStorage,
    clients,
    messages,
    get claims() { return claims; },
    get skipWaitingCalls() { return skipWaitingCalls; },
    runtime: createServiceWorkerRuntime({
      caches: cacheStorage,
      fetch: options.fetch ?? (async () => { throw new Error('offline'); }),
      clients,
      skipWaiting: async () => { skipWaitingCalls += 1; },
      origin,
      now: options.now,
      policy: options.policy,
    }),
  }));
}

test('service worker activation is explicit and tile pruning uses persisted metadata', async () => {
  const fixture = await createRuntimeFixture({
    fetch: async () => new Response('tile', {
      status: 200,
      headers: { 'content-length': '4' },
    }),
    policy: {
      maxTileEntries: 3,
      maxTileBytes: 12,
      pruneTargetRatio: 2 / 3,
      metadataFlushEvery: 2,
    },
  });
  await fixture.runtime.install();
  assert.equal(fixture.skipWaitingCalls, 0);
  await fixture.runtime.handleMessage({ type: 'ACTIVATE_UPDATE' });
  assert.equal(fixture.skipWaitingCalls, 1);

  const tileCache = await fixture.cacheStorage.open(String(fixture.runtime.policy['tileCache']));
  assert.equal(tileCache.keysCalls, 1, 'metadata performs one cold-start cache scan');
  for (let index = 0; index < 8; index += 1) {
    const request = new Request(`https://a.basemaps.cartocdn.com/dark/${index}.png`);
    const response = await fixture.runtime.handleFetch(request);
    assert.equal(await response.text(), 'tile');
  }
  const snapshot = await fixture.runtime.snapshot();
  assert.ok(snapshot.tileCache.entries <= 3);
  assert.ok(snapshot.tileCache.bytes <= 12);
  assert.equal(tileCache.keysCalls, 1, 'tile writes do not rescan cache keys');
});

test('service worker supports warm/cold offline navigation and retained-cache rollback', async () => {
  let currentTime = 100;
  const fixture = await createRuntimeFixture({
    now: () => currentTime,
    policy: { rollbackWindowMs: 10 },
  });
  await fixture.runtime.install();
  const currentApp = await fixture.cacheStorage.open(String(fixture.runtime.policy['appCache']));
  await currentApp.put('/', new Response('current offline shell'));
  const navigation = {
    url: 'https://mesh.test/feed',
    mode: 'navigate',
  };
  assert.equal(await (await fixture.runtime.handleFetch(navigation)).text(), 'current offline shell');

  currentApp.values.clear();
  const cold = await fixture.runtime.handleFetch(navigation);
  assert.equal(cold.status, 0);

  const previousAppName = String(fixture.runtime.policy['previousAppCache']);
  const previousTilesName = String(fixture.runtime.policy['previousTileCache']);
  const previousApp = await fixture.cacheStorage.open(previousAppName);
  await fixture.cacheStorage.open(previousTilesName);
  await previousApp.put('/', new Response('previous offline shell'));
  await fixture.runtime.handleMessage({ type: 'SET_ROLLBACK_MODE', enabled: true });
  assert.equal(await (await fixture.runtime.handleFetch(navigation)).text(), 'previous offline shell');
  await fixture.runtime.handleMessage({ type: 'SET_ROLLBACK_MODE', enabled: false });

  await fixture.runtime.activate();
  assert.equal(fixture.claims, 1);
  assert.ok((await fixture.cacheStorage.keys()).includes(previousAppName));
  currentTime += 11;
  await fixture.runtime.cleanupOldCaches(true);
  assert.ok(!(await fixture.cacheStorage.keys()).includes(previousAppName));
  assert.ok(!(await fixture.cacheStorage.keys()).includes(previousTilesName));
});

test('page update coordinator defers active interactions and reloads once only after explicit activation', async () => {
  resetServiceWorkerUpdatesForTests();
  const originalNavigator = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
  const originalDocument = Object.getOwnPropertyDescriptor(globalThis, 'document');
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
  const containerListeners = new Map<string, () => void>();
  const posted: unknown[] = [];
  let blocked = true;
  let reloads = 0;
  let registeredUrl = '';
  const registration = {
    waiting: { postMessage: (value: unknown) => posted.push(value) },
    installing: null,
    addEventListener: () => {},
  };
  Object.defineProperty(globalThis, 'navigator', {
    configurable: true,
    value: {
      serviceWorker: {
        controller: {},
        register: async (url: string) => { registeredUrl = url; return registration; },
        addEventListener: (name: string, listener: () => void) => containerListeners.set(name, listener),
      },
    },
  });
  Object.defineProperty(globalThis, 'document', {
    configurable: true,
    value: { querySelector: () => blocked ? {} : null },
  });
  Object.defineProperty(globalThis, 'window', {
    configurable: true,
    value: { location: { reload: () => { reloads += 1; } } },
  });

  try {
    registerServiceWorker();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(getServiceWorkerUpdateSnapshot().available, true);
    assert.equal(registeredUrl, '/sw.js?build=unversioned');
    containerListeners.get('controllerchange')?.();
    assert.equal(reloads, 0, 'an update event alone never forces a reload');
    assert.equal(activateServiceWorkerUpdate(), false);
    assert.equal(getServiceWorkerUpdateSnapshot().blocked, true);
    assert.deepEqual(posted, []);

    blocked = false;
    assert.equal(activateServiceWorkerUpdate(), true);
    assert.deepEqual(posted, [{ type: 'ACTIVATE_UPDATE' }]);
    containerListeners.get('controllerchange')?.();
    containerListeners.get('controllerchange')?.();
    assert.equal(reloads, 1);
  } finally {
    if (originalNavigator) Object.defineProperty(globalThis, 'navigator', originalNavigator);
    else Reflect.deleteProperty(globalThis, 'navigator');
    if (originalDocument) Object.defineProperty(globalThis, 'document', originalDocument);
    else Reflect.deleteProperty(globalThis, 'document');
    if (originalWindow) Object.defineProperty(globalThis, 'window', originalWindow);
    else Reflect.deleteProperty(globalThis, 'window');
    resetServiceWorkerUpdatesForTests();
  }
});

test('service worker URL follows the hashed Vite entry bundle', () => {
  const originalDocument = Object.getOwnPropertyDescriptor(globalThis, 'document');
  Object.defineProperty(globalThis, 'document', {
    configurable: true,
    value: {
      querySelector: () => ({
        getAttribute: (name: string) => name === 'src' ? '/assets/index-example123.js' : null,
      }),
    },
  });
  try {
    assert.equal(serviceWorkerScriptUrl(), '/sw.js?build=%2Fassets%2Findex-example123.js');
  } finally {
    if (originalDocument) Object.defineProperty(globalThis, 'document', originalDocument);
    else Reflect.deleteProperty(globalThis, 'document');
  }
});
