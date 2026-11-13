import { expect, test, type Response } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { installMapRoutes, installTerrainRoutes, RF_COVERAGE_TILE } from './mapFixtures.js';

// Service-worker fetches bypass page.route and can load the public basemap
// instead of these fixtures. Keep all dashboard tile readiness under routing.
test.use({ serviceWorkers: 'block' });

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
      public_key: index.toString(16).padStart(64, '0'),
      name: `Load fixture ${index}`,
      role: 2,
      lat: 49.8 + (index % 100) * 0.105,
      lon: -7.8 + (index % 80) * 0.125,
      last_seen: new Date().toISOString(),
      is_online: true,
    }));
    class FixtureWebSocket extends EventTarget {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSING = 2;
      static readonly CLOSED = 3;
      static connections = 0;
      readyState = FixtureWebSocket.CONNECTING;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent<string>) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      constructor(_url: string | URL) {
        super();
        Object.assign(window, { dashboardFixturePublish: (type: string, data: Record<string, unknown>) => {
          this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({ type, ts: Date.now(), data: type === 'initial_state' ? { nodes, viable_links: [], ...data } : data }) }));
        } });
        FixtureWebSocket.connections++;
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
    if (path.endsWith('/runtime-config')) return route.fulfill({ json: {
      version: 1, packetArcs: false, heatmap: false, regionScoping: false,
      privacyGeneration: 1, refreshAfterSeconds: 300,
    } });
    if (path.endsWith('/packets/recent')) return route.fulfill({ json: [] });
    if (path.endsWith('/inferred-nodes')) {
      return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
    }
    if (path.endsWith('/stats')) {
      return route.fulfill({
        json: { mqttNodes: 0, mapNodes: 0, totalNodes: 0, staleNodes: 0, packetsDay: 0 },
      });
    }
    return route.fulfill({ json: {} });
  });
});

test('map search and realtime load no nodes without valid privacy authority', async ({ page }, testInfo) => {
  const nodeRequests: string[] = [];
  const dataPaths = new Set(['/api/inferred-nodes', '/api/packets/recent', '/api/stats']);
  page.on('request', request => {
    const url = new URL(request.url());
    if (url.pathname.startsWith('/api/nodes/') || dataPaths.has(url.pathname)) nodeRequests.push(url.pathname + url.search);
  });
  await page.route('**/api/runtime-config', route => route.fulfill({ json: {} }));
  await page.goto(`/?node=${'0'.repeat(64)}`);
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  await page.getByRole('combobox', { name: 'Search map nodes' }).filter({ visible: true }).fill('HarbourBeacon');
  // Observe beyond the 200ms former-name debounce after the app has mounted.
  await page.waitForTimeout(500);
  expect(nodeRequests).toEqual([]);
  expect(await page.evaluate(() => (window.WebSocket as typeof WebSocket & { connections: number }).connections)).toBe(0);
  await expect(page.getByRole('dialog', { name: 'Node details' })).toHaveCount(0);
  await expect(page.getByRole('option', { name: /Ridge current/ })).toHaveCount(0);
});

test('former node names resolve to the current name in map search', async ({ page }, testInfo) => {
  test.setTimeout(90_000);
  const queries: string[] = [];
  let releaseOld!: () => void;
  const oldResponse = new Promise<void>(resolve => { releaseOld = resolve; });
  await page.route('**/api/nodes/map?**', async route => {
    const params = new URL(route.request().url()).searchParams;
    const q = params.get('q');
    expect(params.get('network')).toBe('ukmesh');
    if (q) queries.push(q);
    if (q === 'DelayedFormer') await oldResponse;
    return route.fulfill({ json: {
      nodes: q === 'HarbourBeacon' || q === 'DelayedFormer' ? [{
        node_id: 'A'.repeat(64), name: 'Ridge current', lat: 54.5, lon: -1.5,
        role: 2, last_seen: new Date().toISOString(), is_online: true,
      }] : [],
      page: { complete: true, nextCursor: null, snapshot: new Date().toISOString() },
    } });
  });
  try {
    await page.goto('/');
    if (testInfo.project.name === 'dashboard-mobile') {
      await page.getByRole('button', { name: 'Layers' }).first().click();
    }
    const input = page.getByRole('combobox', { name: 'Search map nodes' }).filter({ visible: true });
    await input.fill('HarbourBeacon');
    await expect(page.getByRole('option', { name: /Ridge current/ })).toBeVisible();
    expect(queries).toContain('HarbourBeacon');
    await input.fill('DelayedFormer');
    await expect.poll(() => queries).toContain('DelayedFormer');
    await input.fill('NoHistoryMatch');
    await expect.poll(() => queries).toContain('NoHistoryMatch');
    releaseOld();
    await expect(page.getByRole('option', { name: /Ridge current/ })).toHaveCount(0);
  } finally { releaseOld(); }
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
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Close menu', exact: true }).click();
  }
  await expect(page.getByRole('region', { name: 'RF coverage status' })).toContainText('50%');
  await expect(page.getByRole('region', { name: 'RF coverage status' })).toContainText('868.000 MHz');
  await expect.poll(
    () => requests.some((path) => path.startsWith('/rf-coverage/tiles/standard/')),
    { timeout: 15_000 },
  ).toBe(true);
  await expect(page.getByRole('button', { name: 'Precision' })).toBeVisible({ timeout: 8_000 });
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

test('RF polling pauses when unused or hidden and follows a visible node job with the layer off', async ({ page }, testInfo) => {
  test.setTimeout(60_000);
  const key = '0'.repeat(64);
  await page.addInitScript(() => localStorage.setItem('meshcore-rf-coverage-visible-v1', '0'));
  let polls = 0;
  let nodeState: 'computing' | 'available' = 'computing';
  await page.route('**/rf-coverage/meta.json*', route => {
    polls++;
    return route.fulfill({ json: { generated_at: new Date().toISOString(), source: 'Synthetic', version: 'fixture', complete: true,
      node_coverage: { [key]: { dataset_id: `n${'1'.repeat(24)}`, run_id: 'invented', state: nodeState,
        position_status: 'active', lat: 49.8, lon: -7.8, requested_at: new Date().toISOString(), updated_at: new Date().toISOString(),
        fresh_until: new Date(Date.now() + 60_000).toISOString(),
        ...(nodeState === 'available' ? { standard: { tiles: [{ image: `tiles/nodes/n${'1'.repeat(24)}/0-0.png`,
          bounds: { South: 49, North: 55, West: -9, East: 3 } }] } } : {}) } },
    } });
  });
  await page.route('**/rf-coverage/nodes/*/request', route => route.fulfill({ status: 202 }));
  await page.goto(`/?node=${key}&layers=feed`);
  await expect(page.getByText('Live Map', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: testInfo.project.name === 'dashboard-mobile' ? 'Layers' : 'Show map layers' }).first().click();
  const toggle = page.getByRole('button', { name: 'RF Coverage', exact: true }).first();
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await page.waitForTimeout(3500);
  expect(polls).toBe(0);
  await toggle.click();
  await expect.poll(() => polls).toBeGreaterThan(0);
  const beforeHidden = polls;
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await page.waitForTimeout(3500);
  expect(polls).toBe(beforeHidden);
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await expect.poll(() => polls).toBeGreaterThan(beforeHidden);
  await toggle.click();
  const beforeUnused = polls;
  await page.waitForTimeout(3500);
  expect(polls).toBe(beforeUnused);
  if (testInfo.project.name === 'dashboard-mobile') await page.getByRole('button', { name: 'Close menu', exact: true }).click();
  await page.getByRole('dialog', { name: 'Node details' }).getByRole('tab', { name: 'Path', exact: true }).click();
  await page.getByRole('button', { name: 'Show RF coverage', exact: true }).first().click();
  await expect.poll(() => polls).toBeGreaterThan(beforeUnused);
  if (testInfo.project.name === 'dashboard-mobile') await page.getByRole('button', { name: 'Layers' }).first().click();
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  const pendingPolls = polls;
  await expect.poll(() => polls, { timeout: 6000 }).toBeGreaterThan(pendingPolls);
  nodeState = 'available';
  const completionPolls = polls;
  await expect.poll(() => polls, { timeout: 6000 }).toBeGreaterThan(completionPolls);
  await page.waitForTimeout(300);
  const completedPolls = polls;
  await page.waitForTimeout(3500);
  expect(polls).toBe(completedPolls);
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
  // Retain responses without starting promises that can reject while navigation
  // or a mobile menu action is still running. Assert each loading stage below.
  const firstTiles = new Map<string, Response>();
  page.on('response', (response) => {
    const path = new URL(response.url()).pathname;
    const kind = path.startsWith('/terrain-tiles/') ? 'terrain'
      : path.startsWith('/rf-coverage/tiles/') ? 'coverage' : null;
    if (kind && !firstTiles.has(kind)) firstTiles.set(kind, response);
  });
  await page.goto('/?layers=feed%2Cterrain%2Ccoverage', { waitUntil: 'domcontentloaded' });
  // Wait on the resources the test needs before interacting with a busy 3D
  // canvas. A visible title does not mean the map sources have initialized.
  await test.step('wait for terrain and RF coverage fixture readiness', async () => {
    await expect.poll(() => ({
      terrain: firstTiles.get('terrain')?.status(),
      coverage: firstTiles.get('coverage')?.status(),
    }), {
      message: 'MapLibre must request both terrain and RF coverage fixture tiles',
      // Share the existing 60s test budget with cold software DEM decoding;
      // leave the remaining budget for layer assertions and bitmap decoding.
      timeout: 45_000,
    }).toEqual({ terrain: 200, coverage: 200 });
  });
  await expect(page.getByText('Live Map', { exact: true })).toBeVisible();
  if (testInfo.project.name === 'dashboard-mobile') {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }

  const terrain = page.getByRole('button', { name: '3D Terrain' }).first();
  const coverage = page.getByRole('button', { name: 'RF Coverage' }).first();
  await expect(coverage).toHaveAttribute('aria-pressed', 'true');
  await expect(terrain).toHaveAttribute('aria-pressed', 'true');
  await expect.poll(() => new URL(page.url()).searchParams.get('layers')?.split(',')).toEqual(
    expect.arrayContaining(['terrain', 'coverage']),
  );
  const tileResponses = [firstTiles.get('terrain')!, firstTiles.get('coverage')!];
  for (const response of tileResponses) {
    expect(response.fromServiceWorker(), 'fixture tile must pass through Playwright routing').toBe(false);
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
  // Keep the complete axe scan and both 4,600-node bootstraps within the
  // standard production-journey budget; this is not an interaction-time SLO.
  test.setTimeout(90_000);
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

test('message reading preserves anchors, counts unique arrivals and keeps Watch separate from Pin', async ({ page }, info) => {
  await page.goto('/?layers=feed');
  if (info.project.name === 'dashboard-mobile') await page.getByRole('button', { name: 'Live', exact: true }).click();
  await expect(page.locator('.packet-feed')).toBeVisible();
  await page.evaluate(() => {
    const packets = Array.from({ length: 200 }, (_, i) => ({ packet_hash: i.toString(16).padStart(64, '0'),
      time: new Date(Date.now() - i * 1000).toISOString(), packet_type: 5, summary: `[Public] Reading fixture ${i}` }));
    (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('initial_state', { packets, messages: packets });
  });
  const feed = page.locator('.packet-feed');
  await expect(feed.locator('.packet-item')).toHaveCount(200);
  await feed.evaluate(element => { element.scrollTop = 400; });
  await expect.poll(() => feed.evaluate(element => element.scrollTop)).toBeGreaterThan(300);
  const anchor = await feed.evaluate(element => {
    const top = element.getBoundingClientRect().top;
    const row = [...element.querySelectorAll<HTMLElement>('[data-packet-key]')].find(item => item.getBoundingClientRect().bottom > top)!;
    return { key: row.dataset.packetKey, offset: row.getBoundingClientRect().top - top };
  });
  const publish = (rx: string) => page.evaluate(receiver => {
    (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('packet', {
      id: `new-${receiver}`, packetHash: 'F'.repeat(64), rxNodeId: receiver.repeat(64), topic: 'ukmesh/LON/fixture/packets',
      packetType: 5, summary: '[Public] A new message', ts: Date.now(),
    });
  }, rx);
  await publish('A');
  const jump = page.getByRole('button', { name: '1 new message. Jump to latest', exact: true });
  await expect(jump).toBeVisible();
  await expect.poll(() => feed.evaluate((element, saved) => {
    const row = [...element.querySelectorAll<HTMLElement>('[data-packet-key]')].find(item => item.dataset.packetKey === saved.key);
    return row ? Math.abs(row.getBoundingClientRect().top - element.getBoundingClientRect().top - saved.offset) : Infinity;
  }, anchor)).toBeLessThan(1);
  await publish('B');
  await expect(feed.getByText('2rx', { exact: true }).last()).toBeAttached();
  await expect(jump).toHaveCount(1);
  const watch = feed.getByRole('button', { name: 'Watch GRP packets', exact: true }).first();
  await watch.focus();
  await page.keyboard.press('Space');
  await expect(feed.locator('.packet-item--pinned')).toHaveCount(0);
  await expect(feed.getByRole('button', { name: 'Stop watching GRP packets', exact: true }).first()).toBeAttached();
  await jump.click();
  await expect(jump).toHaveCount(0);
  await expect.poll(() => feed.evaluate(element => element.scrollHeight - element.clientHeight - element.scrollTop)).toBeLessThan(1);
  await feed.evaluate(element => { element.scrollTop = element.scrollHeight - element.clientHeight - 48; });
  await page.evaluate(() => {
    (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('packet', {
      id: 'within-follow', packetHash: 'E'.repeat(64), packetType: 5, topic: 'ukmesh/LON/fixture/packets', summary: '[Public] Follow arrival', ts: Date.now(),
    });
  });
  await expect(feed.getByText('[Public] Follow arrival', { exact: true })).toBeVisible();
  await expect.poll(() => feed.evaluate(element => element.scrollHeight - element.clientHeight - element.scrollTop)).toBeLessThan(1);
  // When the oldest visible packet is evicted, stay at the oldest retained row.
  await feed.evaluate(element => { element.scrollTop = 0; element.dispatchEvent(new Event('scroll')); });
  const oldest = await feed.locator('.packet-item').first().getAttribute('data-packet-key');
  await page.evaluate(() => (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('packet', {
    id: 'eviction', packetHash: 'C'.repeat(64), packetType: 5, topic: 'ukmesh/LON/fixture/packets', summary: '[Public] Oldest row evicted', ts: Date.now(),
  }));
  await expect(feed.locator('.packet-item')).toHaveCount(200);
  await expect(feed.locator('.packet-item').first()).not.toHaveAttribute('data-packet-key', oldest!);
  await expect(page.locator('.packet-feed__jump')).toHaveText('1 new message. Jump to latest');
  await expect.poll(() => feed.evaluate(element => element.scrollTop)).toBe(0);
});

test('mobile Layers restores focus and a privacy change clears unread messages', async ({ page }, info) => {
  let generation = 1;
  await page.route('**/api/runtime-config*', route => route.fulfill({ json: {
    version: 1, packetArcs: false, heatmap: false, regionScoping: false, privacyGeneration: generation, refreshAfterSeconds: 5,
  } }));
  await page.goto('/?layers=feed');
  await page.waitForFunction(() => typeof (window as unknown as { dashboardFixturePublish?: unknown }).dashboardFixturePublish === 'function');
  if (info.project.name === 'dashboard-mobile') {
    const layers = page.getByRole('button', { name: 'Layers', exact: true }).first();
    await layers.click();
    await expect(page.getByRole('dialog', { name: 'Map settings' })).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog', { name: 'Map settings' })).toHaveCount(0);
    await expect(layers).toBeFocused();
    await layers.click();
    await page.getByRole('button', { name: 'Keyboard shortcuts', exact: true }).click();
    await expect(page.getByRole('dialog', { name: 'Keyboard shortcuts', exact: true })).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(layers).toBeFocused();
    await page.getByRole('button', { name: 'Live', exact: true }).click();
  }
  await page.evaluate(() => {
    const packets = Array.from({ length: 50 }, (_, i) => ({ packet_hash: i.toString(16).padStart(64, '0'), time: new Date(Date.now() - i * 1000).toISOString(), packet_type: 5, summary: `[Public] Before reset ${i}` }));
    (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('initial_state', { packets, messages: packets });
  });
  const feed = page.locator('.packet-feed');
  await expect(feed.locator('.packet-item')).toHaveCount(50);
  await feed.evaluate(element => { element.scrollTop = 0; element.dispatchEvent(new Event('scroll')); });
  await page.evaluate(() => (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('packet', {
    id: 'unread', packetHash: 'D'.repeat(64), packetType: 5, topic: 'ukmesh/LON/fixture/packets', summary: '[Public] Unread before reset', ts: Date.now(),
  }));
  await expect(page.locator('.packet-feed__jump')).toBeVisible();
  generation = 2;
  await expect(page.locator('.packet-feed__jump')).toHaveCount(0, { timeout: 12_000 });
  await expect(feed.locator('.packet-item')).toHaveCount(0);
});

test('map header keeps region, freshness and actions separate at every target width', async ({ page }) => {
  await page.route('**/api/runtime-config*', route => route.fulfill({ json: {
    version: 1, packetArcs: false, heatmap: false, regionScoping: true, privacyGeneration: 1, refreshAfterSeconds: 300,
  } }));
  await page.route('**/api/regions/summary*', route => route.fulfill({ json: { regions: [{ name: 'synthetic-region' }] } }));
  await page.goto('/');
  await expect(page.locator('.region-selector select')).toBeVisible();
  for (const width of [320, 390, 768, 820, 1024, 1440]) {
    await page.setViewportSize({ width, height: 844 });
    const controls = await page.locator('.topbar').evaluate(header => [...header.querySelectorAll<HTMLElement>(
      ':scope > a, :scope > button, :scope > .topbar__logo, :scope > .conn-indicator, :scope > .region-selector',
    )].map(element => ({ name: element.className, rect: element.getBoundingClientRect().toJSON() }))
      .filter(item => item.rect.width && item.rect.height));
    for (let index = 0; index < controls.length; index++) {
      const current = controls[index]!;
      expect(current.rect.left, `${width}: ${current.name}`).toBeGreaterThanOrEqual(0);
      expect(current.rect.right, `${width}: ${current.name}`).toBeLessThanOrEqual(width);
      if (index) expect(current.rect.left, `${width}: ${current.name} follows ${controls[index - 1]!.name}`)
        .toBeGreaterThanOrEqual(controls[index - 1]!.rect.right);
    }
    if (width <= 640) {
      await expect(page.locator('.topbar__shortcut-btn')).toBeHidden();
      const info = await page.getByRole('button', { name: 'Data disclaimer', exact: true }).boundingBox();
      expect(info!.width).toBeGreaterThanOrEqual(44);
      expect(info!.height).toBeGreaterThanOrEqual(44);
    }
  }
});

test('a failed refresh keeps cached messages readable and offers a confirmed retry', async ({ page }, info) => {
  let release!: () => void;
  let failed = true;
  let requests = 0;
  const gate = new Promise<void>(resolve => { release = resolve; });
  await page.route('**/api/stats?**', async route => {
    requests++;
    await gate;
    await route.fulfill(failed ? { status: 503, json: { error: 'Fixture refresh unavailable' } }
      : { json: { mqttNodes: 0, mapNodes: 0, totalNodes: 0, staleNodes: 0, packetsDay: 0 } });
  });
  try {
    if (info.project.name === 'dashboard-mobile') await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/?layers=feed');
    await page.waitForFunction(() => typeof (window as unknown as { dashboardFixturePublish?: unknown }).dashboardFixturePublish === 'function');
    if (info.project.name === 'dashboard-mobile') await page.getByRole('button', { name: 'Live', exact: true }).click();
    await page.evaluate(() => {
      const packet = { packet_hash: 'A'.repeat(64), time: new Date().toISOString(), packet_type: 5, summary: '[Public] Cached message fixture' };
      (window as unknown as { dashboardFixturePublish(type: string, data: object): void }).dashboardFixturePublish('initial_state', { packets: [packet], messages: [packet] });
    });
    const cached = page.locator('.packet-feed').getByText('[Public] Cached message fixture', { exact: true });
    await expect(cached).toBeVisible();
    await expect.poll(() => requests).toBeGreaterThan(0);
    await page.getByRole('button', { name: /^Legend/ }).click();
    release();
    const error = page.locator('.app-data-status');
    await expect(error).toContainText('Showing the last received data.');
    await expect(cached).toBeVisible();
    const errorBox = (await error.boundingBox())!;
    const feedBox = (await page.locator('.packet-feed-frame').boundingBox())!;
    expect(errorBox.y + errorBox.height).toBeLessThanOrEqual(feedBox.y - 8);
    if (info.project.name === 'dashboard-mobile') {
      const legend = (await page.locator('.node-legend').boundingBox())!;
      expect(legend.y + legend.height).toBeLessThanOrEqual(errorBox.y - 8);
    }
    failed = false;
    await error.getByRole('button', { name: 'Retry', exact: true }).click();
    await expect(error).toHaveCount(0);
    await expect(cached).toBeVisible();
  } finally { release(); }
});
