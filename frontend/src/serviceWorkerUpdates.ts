export type ServiceWorkerUpdateSnapshot = {
  available: boolean;
  deferred: boolean;
  blocked: boolean;
  applying: boolean;
};

const INITIAL_SNAPSHOT: ServiceWorkerUpdateSnapshot = Object.freeze({
  available: false,
  deferred: false,
  blocked: false,
  applying: false,
});

let snapshot = INITIAL_SNAPSHOT;
let registration: ServiceWorkerRegistration | null = null;
let activationRequested = false;
let reloadIssued = false;
let started = false;
const listeners = new Set<() => void>();

function publish(patch: Partial<ServiceWorkerUpdateSnapshot>): void {
  const next = Object.freeze({ ...snapshot, ...patch });
  if (
    next.available === snapshot.available
    && next.deferred === snapshot.deferred
    && next.blocked === snapshot.blocked
    && next.applying === snapshot.applying
  ) return;
  snapshot = next;
  for (const listener of listeners) listener();
}

function hasBlockingInteraction(): boolean {
  return document.querySelector(
    '[data-update-blocking="true"], [role="dialog"][aria-modal="true"]',
  ) !== null;
}

function noteWaitingWorker(nextRegistration: ServiceWorkerRegistration): void {
  registration = nextRegistration;
  if (nextRegistration.waiting) {
    publish({ available: true, deferred: false, applying: false });
  }
}

/**
 * Version the service-worker URL with Vite's hashed entry bundle. This makes
 * every frontend build discoverable even when sw.js itself did not change.
 */
export function serviceWorkerScriptUrl(): string {
  const entry = document.querySelector('script[type="module"][src*="/assets/"]') as {
    getAttribute?: (name: string) => string | null;
  } | null;
  const buildAsset = entry?.getAttribute?.('src') ?? 'unversioned';
  return `/sw.js?build=${encodeURIComponent(buildAsset)}`;
}

export function registerServiceWorker(): void {
  if (started || !('serviceWorker' in navigator)) return;
  started = true;
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (!activationRequested || reloadIssued) return;
    reloadIssued = true;
    window.location.reload();
  });
  void navigator.serviceWorker.register(serviceWorkerScriptUrl(), {
    type: 'module',
    updateViaCache: 'none',
  }).then((nextRegistration) => {
    noteWaitingWorker(nextRegistration);
    nextRegistration.addEventListener('updatefound', () => {
      const installing = nextRegistration.installing;
      if (!installing) return;
      installing.addEventListener('statechange', () => {
        if (installing.state === 'installed' && navigator.serviceWorker.controller) {
          noteWaitingWorker(nextRegistration);
        }
      });
    });
  }).catch(() => {
    // Offline startup and privacy modes may disable service workers.
  });
}

export function activateServiceWorkerUpdate(): boolean {
  const waiting = registration?.waiting;
  if (!waiting) return false;
  const blocked = hasBlockingInteraction();
  if (blocked) {
    publish({ blocked: true, deferred: true });
    return false;
  }
  activationRequested = true;
  publish({ blocked: false, applying: true });
  waiting.postMessage({ type: 'ACTIVATE_UPDATE' });
  return true;
}

export function deferServiceWorkerUpdate(): void {
  publish({ available: false, deferred: true, blocked: false });
}

export function getServiceWorkerUpdateSnapshot(): ServiceWorkerUpdateSnapshot {
  return snapshot;
}

export function subscribeServiceWorkerUpdates(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function resetServiceWorkerUpdatesForTests(): void {
  snapshot = INITIAL_SNAPSHOT;
  registration = null;
  activationRequested = false;
  reloadIssued = false;
  started = false;
  listeners.clear();
}
