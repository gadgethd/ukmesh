export const SW_CACHE_POLICY = Object.freeze({
  version: 'v7',
  tileCache: 'meshcore-tiles-v7',
  appCache: 'meshcore-app-v7',
  metadataCache: 'meshcore-meta-v7',
  previousTileCache: 'meshcore-tiles-v6',
  previousAppCache: 'meshcore-app-v6',
  maxTileEntries: 6_000,
  maxTileBytes: 96 * 1024 * 1024,
  pruneTargetRatio: 0.9,
  metadataFlushEvery: 32,
  rollbackWindowMs: 7 * 24 * 60 * 60_000,
});

function isTileUrl(url) {
  try {
    const parsed = new URL(url);
    return parsed.hostname === 'tiles.openfreemap.org';
  } catch {
    return false;
  }
}

function isAppAssetUrl(url, origin) {
  const parsed = new URL(url, origin);
  return parsed.origin === origin && parsed.pathname.startsWith('/assets/');
}

function estimatedResponseBytes(response) {
  const length = Number(response.headers.get('content-length'));
  return Number.isFinite(length) && length > 0 ? Math.trunc(length) : 16 * 1024;
}

export function createTileMetadata(initial = []) {
  const entries = new Map();
  let bytes = 0;
  for (const item of initial) {
    if (!item || typeof item.url !== 'string') continue;
    const entry = {
      url: item.url,
      bytes: Math.max(1, Number(item.bytes) || 16 * 1024),
      lastAccess: Math.max(0, Number(item.lastAccess) || 0),
    };
    entries.set(entry.url, entry);
    bytes += entry.bytes;
  }

  const touch = (url, entryBytes, now) => {
    const previous = entries.get(url);
    if (previous) {
      bytes -= previous.bytes;
      entries.delete(url);
    }
    const entry = {
      url,
      bytes: Math.max(1, Number(entryBytes) || previous?.bytes || 16 * 1024),
      lastAccess: now,
    };
    entries.set(url, entry);
    bytes += entry.bytes;
  };

  const remove = (url) => {
    const previous = entries.get(url);
    if (!previous) return false;
    entries.delete(url);
    bytes -= previous.bytes;
    return true;
  };

  const pruneCandidates = (policy = SW_CACHE_POLICY) => {
    if (entries.size <= policy.maxTileEntries && bytes <= policy.maxTileBytes) return [];
    const targetEntries = Math.floor(policy.maxTileEntries * policy.pruneTargetRatio);
    const targetBytes = Math.floor(policy.maxTileBytes * policy.pruneTargetRatio);
    const removals = [];
    for (const entry of entries.values()) {
      if (entries.size <= targetEntries && bytes <= targetBytes) break;
      removals.push(entry.url);
      remove(entry.url);
    }
    return removals;
  };

  return {
    touch,
    remove,
    pruneCandidates,
    serialize: () => [...entries.values()],
    snapshot: () => ({ entries: entries.size, bytes }),
  };
}

export function createServiceWorkerRuntime(environment) {
  const policy = { ...SW_CACHE_POLICY, ...(environment.policy ?? {}) };
  const now = environment.now ?? Date.now;
  const metadataUrl = new URL('/__meshcore_sw_metadata_v7__', environment.origin).toString();
  let metadata;
  let metadataState = {
    version: 1,
    activatedAt: 0,
    rollbackMode: false,
    tiles: [],
  };
  let metadataPromise;
  let writesSinceFlush = 0;
  let cleanupPromise = null;
  let lastCleanupAt = 0;

  const readMetadata = async () => {
    if (metadataPromise) return metadataPromise;
    metadataPromise = (async () => {
      const metadataCache = await environment.caches.open(policy.metadataCache);
      const response = await metadataCache.match(metadataUrl);
      if (response) {
        try {
          const parsed = await response.json();
          if (parsed && parsed.version === 1 && Array.isArray(parsed.tiles)) {
            metadataState = {
              version: 1,
              activatedAt: Math.max(0, Number(parsed.activatedAt) || 0),
              rollbackMode: parsed.rollbackMode === true,
              tiles: parsed.tiles,
            };
          }
        } catch {
          // Rebuild metadata once below.
        }
      }
      metadata = createTileMetadata(metadataState.tiles);
      if (!response) {
        // One cold-start scan is permitted; tile writes never call cache.keys().
        const tileCache = await environment.caches.open(policy.tileCache);
        const keys = await tileCache.keys();
        const timestamp = now();
        for (const request of keys) metadata.touch(request.url, 16 * 1024, timestamp);
        await persistMetadata();
      }
      return metadata;
    })();
    return metadataPromise;
  };

  const persistMetadata = async () => {
    if (!metadata) return;
    const metadataCache = await environment.caches.open(policy.metadataCache);
    metadataState.tiles = metadata.serialize();
    await metadataCache.put(metadataUrl, new Response(JSON.stringify(metadataState), {
      headers: { 'content-type': 'application/json' },
    }));
    writesSinceFlush = 0;
  };

  const recordTile = async (cache, request, response) => {
    const tileMetadata = await readMetadata();
    tileMetadata.touch(request.url, estimatedResponseBytes(response), now());
    const removals = tileMetadata.pruneCandidates(policy);
    await Promise.all(removals.map((url) => cache.delete(url)));
    writesSinceFlush += 1;
    if (removals.length > 0 || writesSinceFlush >= policy.metadataFlushEvery) {
      await persistMetadata();
    }
  };

  const cleanupOldCaches = async (force = false) => {
    if (cleanupPromise) return cleanupPromise;
    if (!force && now() - lastCleanupAt < 24 * 60 * 60_000) return;
    cleanupPromise = (async () => {
      await readMetadata();
      const names = await environment.caches.keys();
      const rollbackExpired = metadataState.activatedAt > 0
        && now() - metadataState.activatedAt >= policy.rollbackWindowMs;
      const retained = new Set([
        policy.tileCache,
        policy.appCache,
        policy.metadataCache,
        ...(rollbackExpired ? [] : [policy.previousTileCache, policy.previousAppCache]),
      ]);
      await Promise.all(names.map((name) => (
        name.startsWith('meshcore-') && !retained.has(name)
          ? environment.caches.delete(name)
          : Promise.resolve(false)
      )));
      lastCleanupAt = now();
    })().finally(() => {
      cleanupPromise = null;
    });
    return cleanupPromise;
  };

  const activeAppCacheName = () => (
    metadataState.rollbackMode ? policy.previousAppCache : policy.appCache
  );
  const activeTileCacheName = () => (
    metadataState.rollbackMode ? policy.previousTileCache : policy.tileCache
  );

  const fetchTile = async (request, waitUntil) => {
    await readMetadata();
    const cache = await environment.caches.open(activeTileCacheName());
    const cached = await cache.match(request);
    if (cached && !metadataState.rollbackMode) {
      metadata.touch(request.url, estimatedResponseBytes(cached), now());
      writesSinceFlush += 1;
    }
    const refresh = environment.fetch(request)
      .then(async (response) => {
        if (response.ok && !metadataState.rollbackMode) {
          await cache.put(request, response.clone());
          await recordTile(cache, request, response);
        }
        return response;
      })
      .catch(() => cached ?? Response.error());
    if (cached) {
      waitUntil(refresh.then(() => undefined));
      return cached;
    }
    return refresh;
  };

  const fetchAsset = async (request) => {
    await readMetadata();
    const cache = await environment.caches.open(activeAppCacheName());
    const cached = await cache.match(request);
    if (cached) return cached;
    try {
      const response = await environment.fetch(request);
      if (response.ok && !metadataState.rollbackMode) {
        await cache.put(request, response.clone());
      }
      return response;
    } catch {
      return Response.error();
    }
  };

  const fetchNavigation = async (request) => {
    await readMetadata();
    const activeCache = await environment.caches.open(activeAppCacheName());
    try {
      const response = await environment.fetch(request);
      if (response.ok && !metadataState.rollbackMode) {
        await activeCache.put(request, response.clone());
        const parsed = new URL(request.url);
        if (parsed.pathname === '/') await activeCache.put('/', response.clone());
      }
      return response;
    } catch {
      const direct = await activeCache.match(request);
      if (direct) return direct;
      const root = await activeCache.match('/');
      if (root) return root;
      if (!metadataState.rollbackMode) {
        const previous = await environment.caches.open(policy.previousAppCache);
        return await previous.match(request) ?? await previous.match('/') ?? Response.error();
      }
      return Response.error();
    }
  };

  return {
    policy,
    shouldHandle: (request) => (
      isTileUrl(request.url)
      || isAppAssetUrl(request.url, environment.origin)
      || request.mode === 'navigate'
    ),
    install: async () => {
      // No skipWaiting here: activation is controlled by an explicit message.
      await readMetadata();
    },
    activate: async () => {
      await readMetadata();
      if (!metadataState.activatedAt) {
        metadataState.activatedAt = now();
        await persistMetadata();
      }
      await cleanupOldCaches(true);
      await environment.clients.claim();
    },
    handleFetch: async (request, waitUntil = () => {}) => {
      void cleanupOldCaches(false);
      if (isTileUrl(request.url)) return fetchTile(request, waitUntil);
      if (isAppAssetUrl(request.url, environment.origin)) return fetchAsset(request);
      return fetchNavigation(request);
    },
    handleMessage: async (message, source) => {
      if (!message || typeof message.type !== 'string') return;
      if (message.type === 'ACTIVATE_UPDATE') {
        await environment.skipWaiting();
        return;
      }
      if (message.type === 'SET_ROLLBACK_MODE') {
        await readMetadata();
        metadataState.rollbackMode = message.enabled === true;
        await persistMetadata();
        const clients = await environment.clients.matchAll({ type: 'window' });
        for (const client of clients) {
          client.postMessage({
            type: 'CACHE_ROLLBACK_MODE',
            enabled: metadataState.rollbackMode,
          });
        }
        return;
      }
      if (message.type === 'GET_SW_STATUS') {
        await readMetadata();
        source?.postMessage?.({
          type: 'SW_STATUS',
          version: policy.version,
          rollbackMode: metadataState.rollbackMode,
          tileCache: metadata.snapshot(),
        });
      }
    },
    cleanupOldCaches,
    snapshot: async () => {
      await readMetadata();
      return {
        rollbackMode: metadataState.rollbackMode,
        activatedAt: metadataState.activatedAt,
        tileCache: metadata.snapshot(),
      };
    },
  };
}
