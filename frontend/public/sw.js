// Map tile service worker — caches CartoDB tiles so zooming/panning
// to previously visited areas loads instantly.
const TILE_CACHE_NAME = 'meshcore-tiles-v4';
const APP_CACHE_NAME = 'meshcore-app-v4';
const MAX_TILE_ENTRIES = 8000; // ~120 MB at ~15 KB per tile
const ACTIVE_CACHES = new Set([TILE_CACHE_NAME, APP_CACHE_NAME]);

const isTile = (url) =>
  url.includes('basemaps.cartocdn.com');

const isAppAsset = (url) => {
  const parsed = new URL(url);
  return parsed.pathname.startsWith('/assets/');
};

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const names = await caches.keys();
    await Promise.all(names.map((name) => (ACTIVE_CACHES.has(name) ? Promise.resolve() : caches.delete(name))));
    await self.clients.claim();
  })());
});

// Cache tiles aggressively with stale-while-revalidate
self.addEventListener('fetch', (event) => {
  const url = event.request.url;
  
  // Handle map tiles
  if (isTile(url)) {
    event.respondWith(
      caches.open(TILE_CACHE_NAME).then(async (cache) => {
        const cached = await cache.match(event.request);
        
        const fetchPromise = fetch(event.request).then((response) => {
          if (response.ok) {
            cache.put(event.request, response.clone());
            // Prune if needed
            cache.keys().then((keys) => {
              if (keys.length > MAX_TILE_ENTRIES) {
                keys.slice(0, keys.length - MAX_TILE_ENTRIES).forEach((k) => cache.delete(k));
              }
            });
          }
          return response;
        }).catch(() => cached);
        
        // Return cached immediately, update in background
        return cached || fetchPromise;
      })
    );
    return;
  }
  
  // Handle app assets - cache first, fallback to network
  if (isAppAsset(url)) {
    event.respondWith(
      caches.open(APP_CACHE_NAME).then(async (cache) => {
        const cached = await cache.match(event.request);
        if (cached) return cached;
        
        try {
          const response = await fetch(event.request);
          if (response.ok) {
            cache.put(event.request, response.clone());
          }
          return response;
        } catch {
          // If offline and not cached, return offline page for navigation
          if (event.request.mode === 'navigate') {
            return cache.match('/');
          }
          throw new Error('offline');
        }
      })
    );
    return;
  }

  if (event.request.mode === 'navigate') {
    event.respondWith(
      caches.open(APP_CACHE_NAME).then(async (cache) => {
        try {
          const response = await fetch(event.request);
          if (response.ok) {
            cache.put(event.request, response.clone());
          }
          return response;
        } catch {
          const cached = await cache.match(event.request);
          if (cached) return cached;
          const root = await cache.match('/');
          return root || Response.error();
        }
      })
    );
  }
});
