import { createServiceWorkerRuntime } from './sw-runtime.js';

const runtime = createServiceWorkerRuntime({
  caches,
  fetch: (request) => fetch(request),
  clients,
  skipWaiting: () => self.skipWaiting(),
  origin: self.location.origin,
});

self.addEventListener('install', (event) => {
  // Deliberately remain in "waiting" until the page sends ACTIVATE_UPDATE.
  event.waitUntil(runtime.install());
});

self.addEventListener('activate', (event) => {
  event.waitUntil(runtime.activate());
});

self.addEventListener('fetch', (event) => {
  if (!runtime.shouldHandle(event.request)) return;
  event.respondWith(runtime.handleFetch(
    event.request,
    (promise) => event.waitUntil(promise),
  ));
});

self.addEventListener('message', (event) => {
  event.waitUntil(runtime.handleMessage(event.data, event.source));
});
