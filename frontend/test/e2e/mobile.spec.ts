import { expect, test, type Page } from '@playwright/test';

const PHONE = { width: 375, height: 667 };

const chartStats = {
  packetsPerHour: [{ hour: '12:00', count: 24 }],
  packetsPerDay: [{ day: 'Mon', count: 120 }],
  radiosPerHour: [{ hour: '12:00', count: 8 }],
  radiosPerDay: [{ day: 'Mon', count: 32 }],
  packetTypes: [{ label: 'Advert', count: 80 }, { label: 'Group', count: 40 }],
  channelTraffic: [{ channel: 'Public', count: 40, pct: 50, allPct: 33 }],
  repeatersPerDay: [{ hour: '12:00', count: 20 }],
  hopDistribution: [{ hops: 1, count: 60 }],
  prefixCollisions: [{ prefix: 'ab', repeats: 3 }],
  observerRegions: [],
  pathHashes: {
    last24hHops: { one_byte: 30, two_byte: 20, three_byte: 10 },
    multibytePackets24h: 30,
    fullyDecodedMultibyte24h: 20,
    latestMultibyteAt: null,
    latestMultibyteHash: null,
    latestFullyDecodedAt: null,
    latestFullyDecodedHash: 'fixture-path',
    latestFullyDecodedHops: 1,
    latestFullyDecodedPath: 'North -> South',
    latestFullyDecodedNodes: [
      { ord: 1, node_id: 'node-north', name: 'North', lat: 54.5, lon: -1.2 },
      { ord: 2, node_id: 'node-south', name: 'South', lat: 53.5, lon: -1.1 },
    ],
    longestFullyDecodedAt: null,
    longestFullyDecodedHash: null,
    longestFullyDecodedHops: null,
    longestFullyDecodedPath: null,
    longestFullyDecodedNodes: [],
  },
  summary: {
    totalPackets24h: 120,
    totalPackets7d: 720,
    uniqueRadios24h: 32,
    activeRepeaters: 20,
    staleRepeaters: 2,
    peakHour: '12:00',
    peakHourCount: 24,
  },
};

const health = {
  system: {
    generated_at: '2026-07-18T12:00:00Z',
    cpu: { load_1m: 0.2, count: 2, load_pct: 10, usage_pct: 12 },
    memory: { total_mb: 1024, used_mb: 512, used_pct: 50 },
    disk: { total_gb: 20, used_gb: 8, used_pct: 40 },
    runtime: { uptime_s: 3600, node_version: 'v20', platform: 'linux', arch: 'x64' },
  },
  workers: [],
  frontend_errors_1h: 0,
  ingest: {
    stale_nodes: 0,
    active_nodes: 2,
    max_stale_minutes: 0,
    stale_threshold_minutes: 10,
    global_last_packet_at: '2026-07-18T12:00:00Z',
  },
};

const repeaterNodes = Array.from({ length: 12 }, (_, index) => ({
  node_id: `node-${index}`,
  name: `Repeater ${String(index + 1).padStart(2, '0')}`,
  lat: 54 + index * 0.01,
  lon: -1 - index * 0.01,
  iata: 'TST',
  role: 2,
  last_seen: '2026-07-18T12:00:00Z',
  is_online: true,
  public_key: `${String(index).padStart(2, '0')}${'a'.repeat(62)}`,
}));

async function installApiFixtures(page: Page) {
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-cookie-consent-v1', '1');
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');
  });

  await page.route('**/api/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname === '/api/stats/charts') return route.fulfill({ json: chartStats });
    if (pathname === '/api/stats') {
      return route.fulfill({
        json: {
          packetsDay: 120,
          totalNodes: 32,
          internationalNodes: 1,
          internationalLastSeen: '2026-07-18T12:00:00Z',
          internationalLastCountry: 'France',
          mqttNodes: 20,
          mapNodes: 30,
          staleNodes: 2,
        },
      });
    }
    if (pathname === '/api/health') return route.fulfill({ json: health });
    if (pathname.endsWith('/packets/recent')) return route.fulfill({ json: [] });
    if (pathname.endsWith('/path-beta/history')) return route.fulfill({ json: { segments: [] } });
    if (pathname.endsWith('/inferred-nodes')) {
      return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
    }
    if (pathname === '/api/nodes') return route.fulfill({ json: repeaterNodes });
    if (pathname === '/api/node-status/latest') return route.fulfill({ json: [] });
    if (pathname === '/api/companion-activity') return route.fulfill({ json: [] });
    if (pathname === '/api/owner/session') return route.fulfill({ json: { ok: false } });
    return route.fulfill({ json: {} });
  });
}

async function expectNoViewportOverflow(page: Page) {
  const dimensions = await page.evaluate(() => ({
    viewportWidth: window.innerWidth,
    documentWidth: document.documentElement.scrollWidth,
    bodyWidth: document.body.scrollWidth,
  }));
  expect(dimensions.documentWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
  expect(dimensions.bodyWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
}

const routedViews = [
  ['UK home', 'http://127.0.0.1:4173/'],
  ['UK feed', 'http://127.0.0.1:4173/feed'],
  ['UK repeaters', 'http://127.0.0.1:4173/repeater'],
  ['UK companions', 'http://127.0.0.1:4173/companion'],
  ['UK install', 'http://127.0.0.1:4173/install'],
  ['UK open source', 'http://127.0.0.1:4173/open-source'],
  ['UK stats', 'http://127.0.0.1:4173/stats'],
  ['UK login', 'http://127.0.0.1:4173/login'],
  ['Teesside install', 'http://127.0.0.1:4176/install'],
  ['Teesside packets', 'http://127.0.0.1:4176/packets'],
  ['Teesside open source', 'http://127.0.0.1:4176/open-source'],
  ['Teesside stats', 'http://127.0.0.1:4176/stats'],
  ['Teesside login', 'http://127.0.0.1:4176/login'],
  ['test diagnostics', 'http://127.0.0.1:4175/'],
] as const;

for (const [name, url] of routedViews) {
  test(`${name} fits a phone viewport`, async ({ page }) => {
    await page.setViewportSize(PHONE);
    await installApiFixtures(page);
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));

    await page.goto(url, { waitUntil: 'networkidle' });
    await expect(page.locator('body')).toBeVisible();
    await expectNoViewportOverflow(page);
    expect(pageErrors).toEqual([]);
  });
}

test('shared navigation stays compact and dismisses without navigation', async ({ page }) => {
  await page.setViewportSize(PHONE);
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4173/feed', { waitUntil: 'networkidle' });

  const openMenu = page.getByRole('button', { name: 'Open menu' });
  await openMenu.click();
  const menu = page.locator('#site-navigation');
  await expect(menu).toBeVisible();
  const box = await menu.boundingBox();
  expect(box?.height ?? PHONE.height).toBeLessThan(PHONE.height * 0.45);
  expect(box?.width ?? PHONE.width).toBeLessThan(PHONE.width);

  await page.keyboard.press('Escape');
  await expect(menu).toBeHidden();
  await openMenu.click();
  await page.locator('.site-main').click({ position: { x: 5, y: 5 } });
  await expect(menu).toBeHidden();
});

test('shared navigation collapses before tablet links can wrap', async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 667 });
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4173/feed', { waitUntil: 'networkidle' });

  const menu = page.locator('#site-navigation');
  const openMenu = page.getByRole('button', { name: 'Open menu' });
  await expect(openMenu).toBeVisible();
  await expect(menu).toBeHidden();

  await openMenu.click();
  await expect(menu).toBeVisible();
  await page.getByRole('button', { name: 'Close menu' }).click();
  await expect(menu).toBeHidden();
});

test('map controls and disclaimer leave the map usable on a phone', async ({ page }) => {
  await page.setViewportSize(PHONE);
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4174/', { waitUntil: 'networkidle' });

  await page.getByRole('button', { name: /Layers/ }).click();
  const controlsBox = await page.locator('.mobile-controls').boundingBox();
  expect(controlsBox?.height ?? PHONE.height).toBeLessThan(PHONE.height * 0.4);
  const mapBox = await page.locator('.map-area').boundingBox();
  expect(mapBox?.height ?? 0).toBeGreaterThan(300);

  await page.getByRole('button', { name: 'Data disclaimer' }).click();
  const dialog = page.getByRole('dialog', { name: 'Data disclaimer' });
  await expect(dialog).toBeVisible();
  const dialogBox = await page.locator('.disclaimer-modal').boundingBox();
  expect(dialogBox?.y ?? -1).toBeGreaterThanOrEqual(0);
  expect((dialogBox?.y ?? PHONE.height) + (dialogBox?.height ?? 1)).toBeLessThanOrEqual(PHONE.height);
  await expect(dialog.getByRole('button', { name: 'Got it' })).toBeVisible();
});

test('repeater search results stay bounded and scroll inside the menu', async ({ page }) => {
  await page.setViewportSize(PHONE);
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4173/repeater', { waitUntil: 'networkidle' });

  await page.getByRole('textbox').fill('Repeater');
  const results = page.locator('.repeater-search-box__results');
  await expect(results).toBeVisible();
  const box = await results.boundingBox();
  expect(box?.height ?? PHONE.height).toBeLessThanOrEqual(PHONE.height * 0.47);
  expect(await results.evaluate((element) => element.scrollHeight)).toBeGreaterThan(
    await results.evaluate((element) => element.clientHeight),
  );
  await expectNoViewportOverflow(page);
});

test('stats path modal fits the viewport and keeps its close action reachable', async ({ page }) => {
  await page.setViewportSize(PHONE);
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4173/stats', { waitUntil: 'networkidle' });

  await page.getByRole('button', { name: 'North -> South' }).click();
  const dialog = page.getByRole('dialog', { name: 'Decoded path map' });
  await expect(dialog).toBeVisible();
  const modalBox = await page.locator('.stats-page__path-modal').boundingBox();
  expect(modalBox?.y ?? -1).toBeGreaterThanOrEqual(0);
  expect((modalBox?.y ?? PHONE.height) + (modalBox?.height ?? 1)).toBeLessThanOrEqual(PHONE.height);
  await expect(dialog.getByRole('button', { name: 'Close' })).toBeVisible();
});

test('narrow 320px pages keep card grids inside the viewport', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 568 });
  await installApiFixtures(page);
  await page.goto('http://127.0.0.1:4176/packets', { waitUntil: 'networkidle' });
  await expectNoViewportOverflow(page);
  const cardBox = await page.locator('.packet-card').first().boundingBox();
  expect(cardBox?.width ?? 320).toBeLessThan(320);
});
