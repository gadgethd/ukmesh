import { expect, test, type Page } from '@playwright/test';

const STYLE = {
  version: 8,
  sources: { openmaptiles: { type: 'vector', url: 'https://tiles.openfreemap.org/planet' } },
  layers: [
    { id: 'background', type: 'background', paint: { 'background-color': '#080d14' } },
    {
      id: 'roads', type: 'line', source: 'openmaptiles', 'source-layer': 'transportation',
      paint: { 'line-color': '#c3ccd6', 'line-width': 4 },
    },
  ],
};

// An authored MVT v2 line fixture; exercises the real worker's protobuf decoder.
function vectorTile(): Buffer {
  const varint = (value: number): number[] => {
    const bytes: number[] = [];
    do { const next = value & 127; value >>>= 7; bytes.push(next | (value ? 128 : 0)); } while (value);
    return bytes;
  };
  const message = (field: number, bytes: number[]): number[] => [field * 8 + 2, ...varint(bytes.length), ...bytes];
  const geometry = [9, 0, 0, 18, 4096, 4096, 4096, 4095].flatMap(varint);
  const feature = [8, 1, 24, 2, ...message(4, geometry)];
  const layer = [
    ...message(1, [...Buffer.from('transportation')]), ...message(2, feature),
    40, ...varint(4096), 120, 2,
  ];
  return Buffer.from(message(3, layer));
}

async function fixtures(page: Page) {
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');
    class FixtureWebSocket extends EventTarget {
      static OPEN = 1;
      readyState = 1;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent) => void) | null = null;
      constructor() {
        super();
        setTimeout(() => {
          this.onopen?.(new Event('open'));
          this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({
            type: 'initial_state',
            data: { nodes: [{ node_id: 'ab'.repeat(32), name: 'Basemap fixture', role: 2,
              lat: 54.57, lon: -1.23, last_seen: new Date().toISOString() }], packets: [], viable_links: [] },
            ts: Date.now(),
          }) }));
        }, 0);
      }
      send() {}
      close() { this.readyState = 3; }
    }
    Object.defineProperty(window, 'WebSocket', { value: FixtureWebSocket });
  });
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith('/packets/recent')) return route.fulfill({ json: [] });
    if (path.endsWith('/inferred-nodes')) return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
    return route.fulfill({ json: {} });
  });
  await page.route('**/rf-coverage/**', (route) => route.fulfill({ status: 404, body: '' }));
  await page.route('**/terrain-tiles/**', (route) => route.abort());
  // Never fetch community map tiles in automated tests.
  await page.route('https://tile.openstreetmap.org/**', (route) => route.fulfill({
    contentType: 'image/png',
    body: Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGMwCav4DwADxgICzPyR1wAAAABJRU5ErkJggg==', 'base64'),
  }));
}

async function vectorRoutes(page: Page, fails: () => boolean) {
  await page.route('https://tiles.openfreemap.org/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/styles/')) {
      // Simulate metadata pinned by the old worker under the unversioned URL.
      return route.fulfill(url.searchParams.has('v') ? { json: STYLE } : { status: 410 });
    }
    if (url.pathname === '/planet') {
      return route.fulfill(url.searchParams.has('v') ? { json: {
        tilejson: '3.0.0', tiles: ['https://tiles.openfreemap.org/planet/fixture/{z}/{x}/{y}.pbf'], maxzoom: 14,
      } } : { status: 410 });
    }
    if (url.pathname.endsWith('.pbf')) {
      return route.fulfill(fails() ? { status: 503, body: '' } : {
        body: vectorTile(), contentType: 'application/vnd.mapbox-vector-tile',
      });
    }
    return route.abort();
  });
}

test.beforeEach(async ({ page }) => fixtures(page));

test('production worker decodes tiles while old fixed worker and metadata URLs are stale', async ({ page }) => {
  const workerRequests: string[] = [];
  page.on('request', (request) => {
    if (request.url().includes('maplibre-gl-worker')) workerRequests.push(request.url());
  });
  await page.route('**/assets/maplibre-gl-worker.mjs', (route) => route.fulfill({
    contentType: 'application/javascript', body: 'throw new Error("stale cached worker")',
  }));
  await vectorRoutes(page, () => false);
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy', { timeout: 30_000 });
  expect(workerRequests.some((url) => /maplibre-gl-worker-[\w-]+\.js$/.test(url))).toBe(true);
  expect(workerRequests.some((url) => url.endsWith('/maplibre-gl-worker.mjs'))).toBe(false);
});

test('a style request that never completes still reaches the bundled outline', async ({ page }) => {
  await page.route('https://tiles.openfreemap.org/**', () => {});
  await page.route('https://tile.openstreetmap.org/**', (route) => route.abort());
  const outline = page.waitForResponse('**/basemap/uk-land.geojson', { timeout: 40_000 });
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'fallback', { timeout: 30_000 });
  expect((await outline).ok()).toBe(true);
  await expect(page.getByText('Using backup basemap')).toBeVisible();
});

test('failed tiles reach the backup map and a manual retry restores vector tiles', async ({ page }) => {
  let failTiles = true;
  await vectorRoutes(page, () => failTiles);
  const backupTile = page.waitForResponse('https://tile.openstreetmap.org/**', { timeout: 40_000 });
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'fallback', { timeout: 30_000 });
  expect((await backupTile).ok()).toBe(true);
  failTiles = false;
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy', { timeout: 15_000 });
  await expect(page.getByText('Using backup basemap')).toHaveCount(0);
});
