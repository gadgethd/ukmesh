import { test, expect, type Page, type Route } from '@playwright/test';
import { mapHandle } from '../browserMapHandle.js';
import { installMapRoutes, installTerrainRoutes } from './mapFixtures.js';

test.use({ serviceWorkers: 'block' });

type NativeOverlay = { _deck?: { props: { layers: Array<{ props: { data?: unknown[] } }> } } };
type NativeMapControls = { _controls: NativeOverlay[] };

async function installNodeSocket(page: Page, nodes: unknown[]) {
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
}

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
  await installNodeSocket(page, nodes);
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

test('native overlays allocate for content, retain devices when cleared, and follow map replacement', async ({ page }) => {
  await installMapRoutes(page);
  await page.route('https://tiles.openfreemap.org/styles/**', route => route.fulfill({ json: {
    version: 8, glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf', sources: {}, layers: [{ id: 'bg', type: 'background' }],
  } }));
  const nodes = ['A', 'B'].map((letter, index) => ({
    node_id: letter.repeat(64), public_key: letter.repeat(64), name: `Invented ${letter}`,
    lat: 54.5 + index * 0.01, lon: -1.3 + index * 0.01, role: 2,
    last_seen: new Date().toISOString(), is_online: true,
  }));
  await page.route('**/api/**', route => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({ json: path.endsWith('runtime-config') ? {
      version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 300,
    } : path.endsWith('nodes/map') ? { nodes, page: { complete: true, nextCursor: null } }
      : path.endsWith('inferred-nodes') ? { inferredNodes: [], inferredActiveNodeIds: [] } : [] });
  });
  await installNodeSocket(page, nodes);
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/?layers=feed');
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy');
  await expect(page.locator('.map-area canvas')).toHaveCount(1);
  await page.evaluate(() => {
    (window as unknown as { emitSourceFixture: (type: string, data: unknown) => void }).emitSourceFixture('packet', {
      id: 'overlay-fixture', packetHash: '1234567890ABCDEF', packetType: 5,
      srcNodeId: 'A'.repeat(64), rxNodeId: 'B'.repeat(64), hopCount: 0,
      summary: 'Invented overlay lifecycle', ts: Date.now(),
    });
  });
  let message = 'Invented overlay lifecycle';
  const pin = () => page.getByRole('button', { name: `Pin GRP packet: ${message}`, exact: true });
  const unpin = () => page.getByRole('button', { name: `Unpin GRP packet: ${message}`, exact: true });
  const mobileFeed = page.locator('.mobile-feed-toggle');
  if (await mobileFeed.isVisible()) await mobileFeed.click();
  await expect(pin()).toBeVisible();
  await expect(page.locator('.map-area canvas')).toHaveCount(1);
  await pin().click();
  await expect(page.locator('.map-area canvas')).toHaveCount(3);

  // Observe the actual native controls/devices rather than a production test API.
  const map = await mapHandle(page);
  let devices = await map.evaluateHandle(map => (map as unknown as NativeMapControls)._controls.filter(control => control._deck));
  const layerCounts = () => devices.evaluate(controls => controls.map(control => control._deck?.props.layers.length ?? 0));
  const hasGeometry = () => devices.evaluate(controls => controls.map(control => control._deck?.props.layers.some(
    layer => Array.isArray(layer.props.data) && layer.props.data.length > 0,
  ) ?? false));
  await expect.poll(hasGeometry).toEqual([true, true]);
  await unpin().click();
  await expect.poll(layerCounts).toEqual([0, 0]);
  await expect(page.locator('.map-area canvas')).toHaveCount(3);
  await pin().click();
  await expect.poll(hasGeometry).toEqual([true, true]);
  await expect(page.locator('.map-area canvas')).toHaveCount(3);

  // Keep devices while metadata is pending, but clear old GPU geometry before
  // unpinning and restore only the latest logical state when the JSON arrives.
  let heldStyle: Route | undefined;
  await page.route('**/overlay-held-style.json', route => { heldStyle = route; });
  const heldStyleUrl = new URL('/overlay-held-style.json', page.url()).href;
  await map.evaluate((map, url) => map.setStyle(url, { diff: false }), heldStyleUrl);
  await expect.poll(() => Boolean(heldStyle)).toBe(true);
  await expect.poll(() => map.evaluate(map => !map.getStyle())).toBe(true);
  await expect.poll(layerCounts).toEqual([0, 0]);
  await expect(page.locator('.map-area canvas')).toHaveCount(3);
  await unpin().click();
  await heldStyle!.fulfill({ json: {
    version: 8, glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
    sources: {}, layers: [{ id: 'held', type: 'background' }],
  } });
  await expect.poll(() => map.evaluate(map => Boolean(map.getStyle()))).toBe(true);
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy');
  await expect.poll(layerCounts).toEqual([0, 0]);
  await expect(page.locator('.map-area canvas')).toHaveCount(3);
  await pin().click();
  await expect.poll(hasGeometry).toEqual([true, true]);
  await expect(page.locator('.map-area canvas')).toHaveCount(3);

  // A style replacement keeps the same devices and live geometry.
  await map.evaluate(map => map.setStyle({ version: 8, glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf', sources: {}, layers: [{ id: 'next', type: 'background' }] }, { diff: false }));
  await expect.poll(() => map.evaluate(map => Boolean(map.getStyle()))).toBe(true);
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy');
  await expect.poll(hasGeometry).toEqual([true, true]);

  // MapLibre clears its style on context loss. Changes during that gap must
  // resume from the latest props when the same native map restores its style.
  const contextControl = await map.evaluateHandle(map => {
    const extension = map.getCanvas().getContext('webgl2')?.getExtension('WEBGL_lose_context');
    if (!extension) throw new Error('Native WebGL context-loss extension is required');
    return extension;
  });
  await map.evaluate((map, control) => new Promise<void>(resolve => {
    map.once('webglcontextlost', () => resolve());
    control.loseContext();
  }), contextControl);
  await expect.poll(() => devices.evaluate(controls => controls.every(control => !control._deck))).toBe(true);
  await expect(page.locator('.map-area canvas')).toHaveCount(1);
  await unpin().click();
  await page.evaluate(() => {
    (window as unknown as { emitSourceFixture: (type: string, data: unknown) => void }).emitSourceFixture('node_upsert', {
      node_id: 'A'.repeat(64), name: 'Invented A 🚫', lat: 54.5, lon: -1.3,
      last_seen: new Date().toISOString(),
    });
    (window as unknown as { emitSourceFixture: (type: string, data: unknown) => void }).emitSourceFixture('packet', {
      id: 'next-overlay-fixture', packetHash: 'FEDCBA0987654321', packetType: 5,
      srcNodeId: 'B'.repeat(64), rxNodeId: 'A'.repeat(64), hopCount: 0,
      summary: 'Invented next overlay', ts: Date.now(),
    });
  });
  message = 'Invented next overlay';
  await contextControl.evaluate(control => control.restoreContext());
  await expect.poll(() => map.evaluate(map => Boolean(map.getStyle()))).toBe(true);
  await expect.poll(() => map.evaluate(async map => {
    const source = map.getSource('nodes') as import('maplibre-gl').GeoJSONSource;
    const data = await source.getData() as GeoJSON.FeatureCollection;
    const geometry = data.features.find(feature => feature.properties?.node_id === 'A'.repeat(64))?.geometry;
    return geometry?.type === 'Point' && (geometry.coordinates[0] !== -1.3 || geometry.coordinates[1] !== 54.5);
  })).toBe(true);
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy');
  await expect.poll(layerCounts).toEqual([0, 0]);
  await expect(page.locator('.map-area canvas')).toHaveCount(1);
  await pin().click();
  await expect(page.locator('.map-area canvas')).toHaveCount(3);
  devices = await map.evaluateHandle(map => (map as unknown as NativeMapControls)._controls.filter(control => control._deck));
  await expect.poll(hasGeometry).toEqual([true, true]);
  await expect.poll(() => devices.evaluate(controls => [...new Set(controls.flatMap(control => (
    control._deck?.props.layers.flatMap(layer => Array.isArray(layer.props.data) ? layer.props.data : []) ?? []
  )).flatMap(row => {
    const hash = (row as { packetHash?: unknown }).packetHash;
    return typeof hash === 'string' ? [hash] : [];
  }))])).toEqual(['FEDCBA0987654321']);

  // Exercise the user's actual renderer-retry lifecycle while a path is active.
  await map.evaluate((map, control) => new Promise<void>(resolve => {
    map.once('webglcontextlost', () => resolve());
    control.loseContext();
  }), contextControl);
  await page.getByRole('button', { name: 'Retry map', exact: true }).click();
  await expect.poll(() => devices.evaluate(controls => controls.every(control => !control._deck))).toBe(true);
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy');
  await expect(page.locator('.map-area canvas')).toHaveCount(3);
  const replacement = await mapHandle(page);
  await expect.poll(() => replacement.evaluate(map => (map as unknown as NativeMapControls)._controls
    .filter(control => control._deck).map(control => control._deck!.props.layers.some(
      layer => Array.isArray(layer.props.data) && layer.props.data.length > 0,
    )))).toEqual([true, true]);
  await unpin().click();
  await expect.poll(() => replacement.evaluate(map => (map as unknown as NativeMapControls)._controls
    .filter(control => control._deck).map(control => control._deck!.props.layers.length))).toEqual([0, 0]);
  expect(errors).toEqual([]);
});
