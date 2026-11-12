import { test, expect } from '@playwright/test';
import { mapHandle } from '../browserMapHandle.js';
import { installMapRoutes, installTerrainRoutes } from './mapFixtures.js';

test.use({ serviceWorkers: 'block' });

test('native node sources skip visual no-ops but retain privacy, geometry, expiry and style invalidation', async ({ page }) => {
  await installMapRoutes(page);
  await installTerrainRoutes(page);
  // This source-counter fixture needs a stable style while it observes no-ops.
  // An unused vector source otherwise triggers the real basemap watchdog and
  // replaces the instrumented source; provider recovery has its own suite.
  await page.route('https://tiles.openfreemap.org/styles/**', route => route.fulfill({ json: {
    version: 8, sources: {}, layers: [{ id: 'bg', type: 'background' }],
  } }));
  const now = Date.now();
  // Keep expiry out of the no-op window even on a slow cold renderer. Native
  // timers still run; advance the wall clock explicitly for the expiry check.
  await page.clock.setFixedTime(now);
  const nodes = ['A', 'B', 'C'].map((letter, index) => ({
    node_id: letter.repeat(64), public_key: letter.repeat(64), name: index === 1 ? 'Invented 🚫' : `Invented ${letter}`,
    lat: 54.5 + index * 0.01, lon: -1.3, role: 2, is_online: true,
    last_seen: new Date(now - (index === 2 ? 14 * 86400000 - 10000 : 1000)).toISOString(),
  }));
  await page.route('**/api/**', route => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({ json: path.endsWith('runtime-config') ? {
      version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 300,
    } : path.endsWith('nodes/map') ? { nodes, page: { complete: true, nextCursor: null } }
      : path.endsWith('inferred-nodes') ? { inferredNodes: [], inferredActiveNodeIds: [] }
        : path.endsWith('stats') ? { totalNodes: 3, packetsDay: 1, internationalNodes: 0 } : [] });
  });
  await page.addInitScript(nodes => {
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');
    localStorage.setItem('meshcore-rf-coverage-visible-v1', '0');
    class FixtureSocket extends EventTarget {
      static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
      readyState = 0;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent) => void) | null = null;
      onclose = null; onerror = null;
      constructor() {
        super();
        const send = (type: string, data: unknown) => this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({ type, data, ts: Date.now() }) }));
        Object.defineProperty(window, 'emitSourceFixture', { value: send, configurable: true });
        setTimeout(() => {
          this.readyState = 1; this.onopen?.(new Event('open'));
          send('initial_state', { nodes, packets: [], viable_links: [] });
        }, 0);
      }
      send() {} close() { this.readyState = 3; }
    }
    Object.defineProperty(window, 'WebSocket', { value: FixtureSocket, configurable: true });
  }, nodes);
  await page.goto('/?layers=feed');
  await page.locator('canvas.maplibregl-canvas').waitFor();
  const map = await mapHandle(page);
  const features = () => map.evaluate(async map => {
    const source = map.getSource('nodes') as import('maplibre-gl').GeoJSONSource | undefined;
    return source ? (await source.getData() as GeoJSON.FeatureCollection).features : [];
  });
  await expect.poll(async () => (await features()).length).toBe(3);
  expect((await features()).find(feature => feature.properties?.node_id === 'C'.repeat(64))!.properties?.is_stale).toBe(false);
  await page.waitForTimeout(1500); // Drain the one-second coalesced initial refresh.
  await map.evaluate(map => {
    const source = map.getSource('nodes') as import('maplibre-gl').GeoJSONSource & { testCalls: number };
    const setData = source.setData.bind(source);
    source.testCalls = 0;
    source.setData = (...args) => { source.testCalls++; return setData(...args); };
  });
  const calls = () => map.evaluate(map => (map.getSource('nodes') as unknown as { testCalls: number }).testCalls);
  const emit = (type: string, data: unknown) => page.evaluate(({ type, data }) => {
    (window as unknown as { emitSourceFixture: (type: string, data: unknown) => void }).emitSourceFixture(type, data);
  }, { type, data });
  await emit('node_update', { nodeId: 'A'.repeat(64), ts: Date.now() });
  await page.waitForTimeout(700);
  expect(await calls()).toBe(0);
  await emit('node_upsert', { node_id: 'A'.repeat(64), lat: 55, last_seen: new Date().toISOString() });
  await expect.poll(async () => (await features()).find(feature => feature.properties?.node_id === 'A'.repeat(64))?.geometry)
    .toEqual({ type: 'Point', coordinates: [-1.3, 55] });
  const beforePrivate = (await features()).find(feature => feature.properties?.node_id === 'B'.repeat(64))!.geometry;
  const beforePrivateCalls = await calls();
  await emit('node_update', { nodeId: 'B'.repeat(64), ts: Date.now() });
  await expect.poll(calls).toBeGreaterThan(beforePrivateCalls);
  await expect.poll(async () => (await features()).find(feature => feature.properties?.node_id === 'B'.repeat(64))!.geometry).not.toEqual(beforePrivate);
  await page.clock.setFixedTime(now + 11000);
  await expect.poll(async () => (await features()).find(feature => feature.properties?.node_id === 'C'.repeat(64))!.properties?.is_stale,
    { timeout: 12000 }).toBe(true);
  // A replacement style recreates sources from the current authoritative store.
  await map.evaluate(map => { map.setStyle({ version: 8, sources: {}, layers: [{ id: 'bg', type: 'background' }] }, { diff: false }); });
  await expect.poll(async () => (await features()).length).toBe(3);
  await expect.poll(async () => (await features()).find(feature => feature.properties?.node_id === 'A'.repeat(64))?.geometry)
    .toEqual({ type: 'Point', coordinates: [-1.3, 55] });
});
