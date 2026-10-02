import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { installMapRoutes, installTerrainRoutes, RF_COVERAGE_TILE } from './mapFixtures.js';

test.beforeEach(async ({ page }) => {
  await installMapRoutes(page);
  await installTerrainRoutes(page);
  await page.route('**/*basemaps.cartocdn.com/**', () => {
    throw new Error('CARTO tiles should not be requested after OpenFreeMap migration');
  });
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');

    const nodes = Array.from({ length: 4_600 }, (_, index) => ({
      node_id: index.toString(16).padStart(64, '0'),
      name: `Load fixture ${index}`,
      role: 2,
      lat: 49.8 + (index % 100) * 0.105,
      lon: -7.8 + (index % 80) * 0.125,
      last_seen: '2026-08-02T00:00:00.000Z',
      is_online: true,
    }));
    class FixtureWebSocket extends EventTarget {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSING = 2;
      static readonly CLOSED = 3;
      readyState = FixtureWebSocket.CONNECTING;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent<string>) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      constructor(_url: string | URL) {
        super();
        window.setTimeout(() => {
          this.readyState = FixtureWebSocket.OPEN;
          this.onopen?.(new Event('open'));
          this.onmessage?.(new MessageEvent('message', {
            data: JSON.stringify({
              type: 'initial_state',
              data: { nodes, packets: [], viable_links: [] },
              ts: Date.now(),
            }),
          }));
        }, 0);
      }
      send(): void {}
      close(): void {
        this.readyState = FixtureWebSocket.CLOSED;
      }
    }
    Object.defineProperty(window, 'WebSocket', { value: FixtureWebSocket, configurable: true });
  });
  let metaPolls = 0;
  await page.route('**/rf-coverage/meta.json*', async (route) => {
    // Two fixture stages; later polls retain the completed stage and tile revision.
    metaPolls = Math.min(metaPolls + 1, 2);
    const tiles = [{ image: 'tiles/standard/0-0.png', bounds: { South: 49, North: 61, West: -9, East: 3 } }];
    if (metaPolls > 1) tiles.push({ image: 'tiles/standard/0-1.png', bounds: { South: 49, North: 61, West: 3, East: 4 } });
    await route.fulfill({ json: {
      generated_at: '2026-08-02T00:00:00Z', source: 'UK Mesh', version: 'v0.1.32+ukmesh', complete: false,
      coverage: {
        standard: {
          tiles,
          frequency_mhz: 868,
          max_search_range_km: 100,
          dem_zoom_level: 11,
          generated_at: '2026-08-02T00:00:00Z',
          assumptions: {
            tx_power_dbm: 22, tx_antenna_gain_dbi: 3, rx_antenna_gain_dbi: 0,
            rx_sensitivity_dbm: -124, fade_margin_db: 20, antenna_height_m: 1,
            rx_height_m: 2, note: 'Canonical HopReach propagation model.',
          },
        },
        ...(metaPolls > 1 ? { precision: {
          tiles: [{ image: 'tiles/precision/0-0.png', bounds: { South: 49, North: 61, West: -9, East: 4 } }],
          frequency_mhz: 868,
          max_search_range_km: 100,
          dem_zoom_level: 13,
          generated_at: '2026-08-02T00:00:00Z',
          assumptions: {
            tx_power_dbm: 22, tx_antenna_gain_dbi: 3, rx_antenna_gain_dbi: 0,
            rx_sensitivity_dbm: -124, fade_margin_db: 20, antenna_height_m: 1,
            rx_height_m: 2, note: 'Precision: 6000 pixels, zoom 13, 2x supersampling.',
          },
        } } : {}),
      },
      run: {
        id: 'fixture-run', started_at: '2026-08-02T00:00:00Z', model: 'hopreach-v0.1.32',
        source_version: '61efac0b4678f55496fe08f53eda0c79eb18655b', completed_tiles: metaPolls,
        total_tiles: 4, tiers: { standard: { state: 'computing', completed_tiles: metaPolls, total_tiles: 2 } },
      },
    } });
  });
  await page.route('**/rf-coverage/progress.json*', (route) => route.fulfill({ json: {
    run_id: 'fixture-run', stage: 'computing_coverage', done: 1, total: 2,
    percent: 50, message: 'Computing Standard', updated_at: '2026-08-02T00:00:00Z',
    eta_seconds: 90, backend: 'cpu',
  } }));
  await page.route('**/rf-coverage/tiles/**/*.png*', (route) => route.fulfill({
    contentType: 'image/png',
    body: RF_COVERAGE_TILE,
  }));
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith('/packets/recent')) return route.fulfill({ json: [] });
    if (path.endsWith('/inferred-nodes')) {
      return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
    }
    if (path.endsWith('/stats')) {
      return route.fulfill({
        json: { mqtt_nodes: 0, map_nodes: 0, total_nodes: 0, stale_nodes: 0, packets_24h: 0 },
      });
    }
    return route.fulfill({ json: {} });
  });
});

test('HopReach tiles arrive progressively while the 4,600-node map remains interactive', async ({ page }, testInfo) => {
  const requests: string[] = [];
  page.on('request', (request) => requests.push(new URL(request.url()).pathname));
  await page.goto('/');

  const rfToggle = page.getByRole('button', { name: 'RF Coverage' }).first();
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  await expect(rfToggle).toHaveAttribute('aria-pressed', 'true', { timeout: 15_000 });
  await expect(page.getByRole('region', { name: 'RF coverage status' })).toContainText('50%');
  await expect(page.getByRole('region', { name: 'RF coverage status' })).toContainText('868.000 MHz');
  await expect.poll(
    () => requests.some((path) => path.startsWith('/rf-coverage/tiles/standard/')),
    { timeout: 15_000 },
  ).toBe(true);
  await expect(page.getByRole('button', { name: 'Precision' })).toBeVisible({ timeout: 8_000 });
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Close menu' }).click();
  }
  await page.getByRole('button', { name: 'Precision' }).click();
  await expect(page.getByRole('button', { name: 'Precision' })).toHaveAttribute('aria-pressed', 'true');
  await expect.poll(
    () => requests.some((path) => path.startsWith('/rf-coverage/tiles/precision/')),
    { timeout: 15_000 },
  ).toBe(true);

  const canvas = page.locator('.maplibregl-canvas');
  const before = await canvas.boundingBox();
  await canvas.dragTo(canvas, { sourcePosition: { x: 180, y: 180 }, targetPosition: { x: 230, y: 210 } });
  await expect(page.getByRole('button', { name: 'Layers' }).first()).toBeEnabled();
  expect(before?.width).toBeGreaterThan(250);
  expect(requests.some((path) => path.startsWith('/api/coverage'))).toBe(false);
});

test('enabled HopReach coverage survives metadata winning the initial map-load race', async ({ page }, testInfo) => {
  const tileRequests: string[] = [];
  page.on('request', (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith('/rf-coverage/tiles/standard/')) tileRequests.push(path);
  });
  await page.unroute('https://tiles.openfreemap.org/**');
  await installMapRoutes(page, true);

  await page.goto('/', { waitUntil: 'domcontentloaded' });

  await page.getByText('Live Map', { exact: true }).waitFor({ state: 'visible', timeout: 15_000 });
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  await expect(page.getByRole('button', { name: 'RF Coverage' }).first()).toHaveAttribute('aria-pressed', 'true');
  await expect.poll(() => tileRequests.length, { timeout: 15_000 }).toBeGreaterThan(0);
});

test('RF coverage remains available with 3D terrain', async ({ page }, testInfo) => {
  // Real DEM decoding and software 3D rendering take longer than rejected HTML tiles.
  test.setTimeout(60_000);
  const tileErrors: string[] = [];
  page.on('console', (message) => {
    if (message.text().startsWith('[terrain] source error:')
      || message.text().startsWith('[rf-coverage] unable to load source tile')) {
      tileErrors.push(message.text());
    }
  });
  const firstTerrainTile = page.waitForResponse((response) =>
    new URL(response.url()).pathname.startsWith('/terrain-tiles/'));
  const firstCoverageTile = page.waitForResponse((response) =>
    new URL(response.url()).pathname.startsWith('/rf-coverage/tiles/'));
  await page.goto('/?layers=feed%2Cterrain%2Ccoverage', { waitUntil: 'domcontentloaded' });
  await page.getByText('Live Map', { exact: true }).waitFor({ state: 'visible', timeout: 15_000 });
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }

  const terrain = page.getByRole('button', { name: '3D Terrain' }).first();
  const coverage = page.getByRole('button', { name: 'RF Coverage' }).first();
  await expect(coverage).toHaveAttribute('aria-pressed', 'true');
  await expect(terrain).toHaveAttribute('aria-pressed', 'true');
  await expect(page).toHaveURL(/layers=[^&]*terrain[^&]*coverage/);
  const tileResponses = await Promise.all([firstTerrainTile, firstCoverageTile]);
  for (const response of tileResponses) {
    expect(response.status()).toBe(200);
    expect(response.headers()['content-type']).toContain('image/png');
  }
  const tileSizes = await page.evaluate((urls) => Promise.all(urls.map(async (url) => {
    const image = await createImageBitmap(await (await fetch(url)).blob());
    const size = { width: image.width, height: image.height };
    image.close();
    return size;
  })), tileResponses.map((response) => response.url()));
  expect(tileSizes).toEqual([{ width: 256, height: 256 }, { width: 256, height: 256 }]);
  expect(tileErrors).toEqual([]);
});

test('map modes update layers and produce a shareable URL', async ({ page }, testInfo) => {
  // Two 4,600-node bootstraps and an axe scan share this budget, including cold Vite compilation.
  test.setTimeout(45_000);
  await page.goto('/');
  await expect(page.getByText('Live Map', { exact: true })).toBeVisible({ timeout: 15_000 });
  const mapArea = page.locator('.map-area');
  await expect(mapArea).toBeVisible();
  expect((await mapArea.boundingBox())?.height).toBeGreaterThan(300);
  const accessibility = await new AxeBuilder({ page }).exclude('.maplibregl-canvas').analyze();
  expect(accessibility.violations
    .filter((violation) => violation.impact === 'critical' || violation.impact === 'serious')
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);

  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  const diagnose = page.getByRole('button', { name: 'Diagnose' }).first();
  await diagnose.click();
  await expect(diagnose).toHaveAttribute('aria-pressed', 'true');
  await expect(page).toHaveURL(/mode=diagnose/);
  await expect(page).toHaveURL(/layers=.*paths/);
  await expect(page).toHaveURL(/layers=.*clashes/);

  await page.reload();
  await expect(page.getByText('Live Map', { exact: true })).toBeVisible({ timeout: 15_000 });
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  await expect(page.getByRole('button', { name: 'Diagnose' }).first()).toHaveAttribute('aria-pressed', 'true');
});
