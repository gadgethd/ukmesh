import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { installMapRoutes } from './mapFixtures.js';
import { publicFixture } from '../../scripts/lighthouse-fixtures.mjs';
import { createServer } from 'node:http';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';

async function installPublicPolicy(page: Page, regionScoping = false) {
  await page.route('**/api/runtime-config', route => route.fulfill({ json: {
    version: 1, packetArcs: false, heatmap: false, regionScoping, privacyGeneration: 1, refreshAfterSeconds: 300,
  } }));
}

test.describe('lazy public map delivery', () => {
  test.use({ serviceWorkers: 'block' });
  for (const name of ['Regions', 'Topology'] as const) {
    for (const failure of [false, true]) {
      test(`${name} content survives a ${failure ? 'failed' : 'pending'} renderer chunk`, async ({ page }) => {
        await installMapRoutes(page);
        await page.route('**/api/**', route => {
          const url = new URL(route.request().url());
          const payload = url.pathname === '/api/topology' ? {
            generatedAt: new Date().toISOString(), windowDays: 30, limited: false,
            summary: { nodes: 2, links: 1, observations: 25, connectedComponents: 1, likelyBridges: 0, isolatedNodes: 0 },
            analysis: { connectedComponents: 1, bridgeNodeIds: [], isolatedNodeIds: [] },
            nodes: [
              { nodeId: 'A'.repeat(64), name: 'Invented Alpha', lat: 52, lon: -1, degree: 1, observations: 25 },
              { nodeId: 'B'.repeat(64), name: 'Invented Bravo', lat: 53, lon: -2, degree: 1, observations: 25 },
            ],
            links: [{ source: 'A'.repeat(64), target: 'B'.repeat(64), observations: 25,
              strongObservations: 5, pathLossDb: 110, lastObserved: new Date().toISOString() }],
          } : publicFixture(url.pathname, url.searchParams) ?? {};
          return route.fulfill({ json: payload });
        });
        let release = () => {};
        let requested = 0;
        const held = new Promise<void>(resolve => { release = resolve; });
        await page.route(new RegExp(`/assets/${name}Map-[^/]+\\.js$`), async route => {
          requested++;
          await held;
          return failure ? route.abort('failed') : route.continue();
        });
        try {
          await page.goto(`/${name.toLowerCase()}`);
          await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
          await expect(page.getByRole('heading', { name: name === 'Regions' ? 'Most active regions' : 'Most connected' })).toBeVisible();
          await expect(page.getByText('Loading map…', { exact: true })).toBeVisible();
          await expect.poll(() => requested).toBeGreaterThan(0);
          const container = page.locator(`.${name.toLowerCase()}-page__map`);
          const before = await container.boundingBox();
          expect(before!.height).toBeGreaterThanOrEqual(430);
          release();
          if (failure) {
            await expect(page.getByRole('alert')).toContainText('The map could not be loaded.');
            await expect(page.getByRole('button', { name: 'Reload map' })).toBeVisible();
          } else {
            await expect(container.locator('canvas')).toBeVisible();
            await expect(page.getByText('Loading map…', { exact: true })).toHaveCount(0);
          }
          const after = await container.boundingBox();
          expect(after!.height).toBeCloseTo(before!.height, 0);
          await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
        } finally { release(); }
      });
    }
  }
});

test.describe('public Web Vitals delivery', () => {
  test.use({ serviceWorkers: 'block' });
  test('observed document metrics use only a fixed route and never send URL or interaction attribution', async ({ page }, testInfo) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
    const reports: Record<string, unknown>[] = [];
    const headers: Record<string, string>[] = [];
    const upstream = new URL(String(testInfo.project.use.baseURL));
    expect(['127.0.0.1', 'localhost']).toContain(upstream.hostname);
    // Keep unload-time keepalive requests on a real HTTP transport. DevTools
    // route interception can lose the request when its source frame disappears.
    const server = createServer(async (req, res) => {
      try {
        const url = new URL(req.url!, upstream);
        if (url.pathname === '/api/telemetry/web-vitals') {
          const chunks: Buffer[] = [];
          let bytes = 0;
          for await (const chunk of req) { bytes += chunk.length; if (bytes > 512) throw new Error('report too large'); chunks.push(chunk); }
          reports.push(JSON.parse(Buffer.concat(chunks).toString()));
          headers.push(req.headers as Record<string, string>);
          res.writeHead(202, { 'Content-Type': 'application/json' }); res.end('{"ok":true}');
        } else if (url.pathname.startsWith('/api/')) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(publicFixture(url.pathname, url.searchParams) ?? {}));
        } else if (url.pathname === '/__exit') { res.end('<!doctype html><title>Document exit</title>'); }
        else {
          const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
          res.writeHead(response.status, { 'Content-Type': response.headers.get('content-type') ?? 'application/octet-stream' });
          res.end(Buffer.from(await response.arrayBuffer()));
        }
      } catch { res.writeHead(500); res.end(); }
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      await page.goto(`${origin}/docs?untracked=example#private-fragment`);
      await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
      // A real trusted interaction with a known long event ensures Event Timing
      // has an observed INP candidate; this is transport qualification, not RUM.
      await page.evaluate(() => {
        const state = window as typeof window & { __vitalInteractionObserved: boolean };
        state.__vitalInteractionObserved = false;
        new PerformanceObserver(list => {
          if (list.getEntries().some(entry => Number((entry as PerformanceEventTiming).interactionId) > 0 && entry.duration >= 80)) {
            state.__vitalInteractionObserved = true;
          }
        }).observe({ type: 'event', durationThreshold: 16, buffered: true } as PerformanceObserverInit);
        document.addEventListener('click', () => {
          const end = performance.now() + 80;
          while (performance.now() < end) { /* bounded browser test interaction */ }
        }, { once: true });
      });
      await page.getByRole('button', { name: 'Open menu', exact: true }).click();
      await expect.poll(() => page.evaluate(() => (window as typeof window & { __vitalInteractionObserved: boolean }).__vitalInteractionObserved)).toBe(true);
      await page.evaluate(() => new Promise<void>(resolve => requestIdleCallback(() => resolve())));
      // Exit only after Event Timing and the library's idle processing have
      // published the trusted interaction. Native visibility flushes final data.
      await page.goto(`${origin}/__exit`);
      await expect.poll(() => reports.map(report => report.metric).sort()).toEqual(['CLS', 'INP', 'LCP']);
      for (const report of reports) {
        expect(Object.keys(report).sort()).toEqual(['metric', 'route', 'value']);
        expect(report.route).toBe('/docs');
        expect(Number.isFinite(report.value)).toBe(true);
      }
      expect(headers.every(header => !header.referer && !header.cookie)).toBe(true);
      expect(Number(reports.find(report => report.metric === 'INP')?.value)).toBeGreaterThanOrEqual(80);
    } finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
  });
});

test('repeater search loads no nodes or former names without valid privacy authority', async ({ page }) => {
  const nodeRequests: string[] = [];
  await page.route('**/api/**', route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/api/nodes/')) nodeRequests.push(url.pathname + url.search);
    return route.fulfill({ json: [] });
  });
  await page.route('**/api/runtime-config', route => route.fulfill({ json: [] }));
  await page.goto('/repeater');
  await page.getByRole('combobox', { name: 'Search repeaters' }).fill('HarbourBeacon');
  // Observe beyond the 200ms former-name debounce with authority still unavailable.
  await page.waitForTimeout(500);
  expect(nodeRequests).toEqual([]);
  await expect(page.getByRole('option', { name: /Ridge current/ })).toHaveCount(0);
});

test('former node names resolve to the current name in repeater search', async ({ page }) => {
  const current = {
    node_id: 'A'.repeat(64), name: 'Ridge current', lat: 54.5, lon: -1.5,
    role: 2, last_seen: new Date().toISOString(), is_online: true,
  };
  const queries: string[] = [];
  await page.route('**/api/**', route => route.fulfill({ json: [] }));
  await page.route('**/api/nodes/map?**', route => {
    const params = new URL(route.request().url()).searchParams;
    expect(params.get('network')).toBe('ukmesh');
    const q = params.get('q');
    if (q) queries.push(q);
    return route.fulfill({ json: {
      nodes: !q || q === 'HarbourBeacon' ? [current] : [],
      page: { complete: true, nextCursor: null, snapshot: new Date().toISOString() },
    } });
  });
  await installPublicPolicy(page);
  await page.goto('/repeater');
  const input = page.getByRole('combobox', { name: 'Search repeaters' });
  await input.fill('HarbourBeacon');
  const result = page.getByRole('option', { name: /Ridge current/ });
  await expect(result).toBeVisible();
  await expect(result).toHaveCount(1);
  expect(queries).toContain('HarbourBeacon');
  await result.click();
  await expect(page.getByRole('heading', { name: 'Ridge current' })).toBeVisible();
  await expect(page.getByText(`${'A'.repeat(16)}…${'A'.repeat(8)}`, { exact: true })).toBeVisible();
});

test('local exploration keeps a former-name search, map link and partial repeater details', async ({ page }) => {
  const id = 'A'.repeat(64);
  const node = { node_id: id, name: 'Ridge current', lat: 54.5, lon: -1.5, role: 2,
    last_seen: new Date().toISOString(), is_online: true };
  const nearer = { ...node, node_id: 'C'.repeat(64), name: 'Hill repeater', lat: 54.6 };
  const indexRequests: string[] = [];
  let linkAttempts = 0;
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-cookie-consent-v1', '1');
    Object.defineProperty(navigator, 'geolocation', { configurable: true, value: { getCurrentPosition: (_success, error) =>
      error({ code: 1, PERMISSION_DENIED: 1 }) } });
  });
  await page.route('**/api/**', route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/nodes/map') {
      const query = url.searchParams.get('q') ?? '';
      indexRequests.push(query);
      return route.fulfill({ json: { nodes: query ? [node] : [node, nearer], page: { complete: true, snapshot: 'fixture' } } });
    }
    if (url.pathname.endsWith('/links')) return ++linkAttempts === 1
      ? route.fulfill({ status: 502, json: {} }) : route.fulfill({ json: [] });
    if (url.pathname.endsWith('/history')) return route.fulfill({ json: [{
      time: new Date().toISOString(), src_node_id: 'B'.repeat(64), hop_count: 2,
    }] });
    if (url.pathname.endsWith('/adverts')) return route.fulfill({ json: [] });
    if (url.pathname === '/api/node-status/latest') return route.fulfill({
      json: url.searchParams.get('observer') === id ? [{ radio: '869.525,62.5,12,5' }] : [],
    });
    return route.fulfill({ json: publicFixture(url.pathname, url.searchParams) ?? [] });
  });
  await installPublicPolicy(page);
  await page.route('https://app.ukmesh.com/**', route => route.fulfill({ body: '<title>Live Map</title>' }));
  await page.goto('/');
  await page.getByRole('button', { name: 'Find a repeater', exact: true }).click();
  const input = page.getByRole('combobox', { name: 'Find a repeater' });
  await input.fill('HarbourBeacon');
  const result = page.getByRole('option', { name: /Ridge current/ });
  await expect(result).toBeVisible();
  expect(indexRequests).toEqual(['HarbourBeacon']);
  expect(await page.evaluate(() => performance.getEntriesByType('resource').map(entry => entry.name)
    .filter(url => /maplibre|MapLibreMap|deckgl/i.test(url)))).toEqual([]);
  await input.press('ArrowDown');
  await input.press('Enter');
  await expect(page.getByRole('heading', { name: 'Ridge current' })).toBeVisible();
  await expect(page.getByRole('combobox', { name: 'Search repeaters' })).toHaveValue('HarbourBeacon');
  await expect(page.getByText('Could not load neighbours.')).toBeVisible();
  await expect(page.getByRole('table')).toBeVisible();
  await expect(page.getByText('869.525 MHz · 62.5 kHz · SF12 · CR5')).toBeVisible();
  await page.getByRole('button', { name: 'Retry neighbours' }).click();
  await expect(page.getByText('Could not load neighbours.')).toHaveCount(0);
  await expect.poll(() => linkAttempts).toBe(2);
  const map = page.getByRole('link', { name: 'View on Live Map' });
  expect(new URL((await map.getAttribute('href'))!).searchParams.get('node')).toBe(id);
  await map.click();
  // Back navigation reloads the index and restores the debounced name lookup.
  // Wait for both before measuring requests caused by nearby selection.
  const restoredLookups = Promise.all(['', 'HarbourBeacon'].map(query => page.waitForResponse(response => {
    const url = new URL(response.url());
    return response.ok() && url.pathname === '/api/nodes/map' && (url.searchParams.get('q') ?? '') === query;
  })));
  await page.goBack();
  await restoredLookups;
  await expect(page.getByRole('heading', { name: 'Ridge current' })).toBeVisible();
  await page.getByRole('button', { name: 'Find nearby' }).click();
  await expect(page.getByText('Location access was denied.', { exact: false })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Ridge current' })).toBeVisible();
  const lookupCount = indexRequests.length;
  await page.evaluate(() => {
    Object.defineProperty(navigator, 'geolocation', { value: { getCurrentPosition: (success) =>
      success({ coords: { latitude: 54.60123, longitude: -1.50123 } }) } });
  });
  await page.getByRole('button', { name: 'Find nearby' }).click();
  const nearest = page.getByRole('list', { name: 'Nearby repeaters' }).getByRole('button').first();
  await expect(nearest).toContainText('Hill repeater');
  await page.keyboard.press('Tab');
  await expect(nearest).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('heading', { name: 'Hill repeater' })).toBeVisible();
  await expect(page.getByText('869.525 MHz · 62.5 kHz · SF12 · CR5')).toHaveCount(0);
  expect(indexRequests).toHaveLength(lookupCount);
  expect(page.url()).not.toContain('54.60123');
});

test('a named region survives reload and Back and opens the same scope on the map', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await installMapRoutes(page);
  await page.route('**/api/**', route => {
    const url = new URL(route.request().url());
    return route.fulfill({ json: publicFixture(url.pathname, url.searchParams) ?? {} });
  });
  await installPublicPolicy(page, true);
  const fixture = publicFixture('/api/regions/summary') as { regions: Array<{ name: string }> };
  const name = fixture.regions[0]!.name;
  await page.goto(`/regions?region=${encodeURIComponent(name)}`);
  const region = page.getByRole('combobox', { name: 'Filter by region' });
  await expect(region).toHaveValue(name);
  const map = page.getByRole('link', { name: 'Open region on Live Map' });
  expect(new URL((await map.getAttribute('href'))!).hash).toBe(`#region=${name}`);
  await page.reload();
  await expect(region).toHaveValue(name);
  await region.selectOption('');
  await expect(region).toHaveValue('');
  await page.goBack();
  await expect(region).toHaveValue(name);
  await page.goto('/regions?region=missing-public-region');
  await expect(page.getByText('This region is unavailable.', { exact: false })).toBeVisible();
  await expect(map).toHaveCount(0);
});

test('Health Check reviews a public repeater without handing its identity to the other service', async ({ page }) => {
  const key = 'A'.repeat(64);
  await page.route('**/api/**', route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/nodes/map') {
      return route.fulfill({ json: { nodes: [{ node_id: key, name: 'Public relay', role: 2 }] } });
    }
    return route.fulfill({ json: publicFixture(url.pathname, url.searchParams) ?? {} });
  });
  await installPublicPolicy(page);
  await page.goto(`/health-check?node=${key}`);
  await expect(page.getByRole('heading', { name: 'Health Check', exact: true })).toBeVisible();
  await expect(page.getByText('Public relay', { exact: true })).toBeVisible();
  const handoff = page.getByRole('link', { name: 'Open Health Check', exact: true });
  await expect(handoff).toHaveAttribute('href', 'https://healthcheck.ukmesh.com');
  await expect(handoff).toHaveAttribute('referrerpolicy', 'no-referrer');
});

for (const width of [375, 768]) {
  test(`navigation collapses and can be toggled at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 667 });
    await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
    await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

    await page.goto('/feed');

    const menu = page.locator('#site-navigation');
    const toggle = page.locator('button[aria-controls="site-navigation"]');
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-label', 'Open menu');
    await expect(menu).toBeHidden();

    await toggle.click();
    await expect(menu).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-expanded', 'true');
    await expect(toggle).toHaveAttribute('aria-label', 'Close menu');

    await page.keyboard.press('Escape');
    await expect(menu).toBeHidden();
    await expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await expect(toggle).toHaveAttribute('aria-label', 'Open menu');
  });
}

test('phone layouts give primary content the full available width', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 667 });
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

  await page.goto('/');
  const homeGrid = page.locator('.site-home__grid');
  const introBox = await page.locator('.site-home__intro').boundingBox();
  const panelBox = await page.locator('.site-home__panel').boundingBox();
  expect(introBox?.width ?? 0).toBeGreaterThan(330);
  expect(panelBox?.width ?? 0).toBeGreaterThan(330);
  expect(await homeGrid.evaluate((element) => getComputedStyle(element).gridTemplateColumns.split(' ').length)).toBe(1);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBe(375);

  await page.goto('/feed');
  const channelsBox = await page.locator('.uk-feed-channels').boundingBox();
  const chatBox = await page.locator('.uk-feed-chat').boundingBox();
  expect(channelsBox?.height ?? 200).toBeLessThan(64);
  expect(chatBox?.width ?? 0).toBeGreaterThan(360);
  await expect(page.locator('.uk-feed-map')).toBeHidden();
  await expect(page.locator('.uk-feed-stats__row')).toBeVisible();
});

test('tablet topology uses the full-width map workspace', async ({ page }) => {
  await installPublicPolicy(page);
  await page.setViewportSize({ width: 768, height: 667 });
  await page.route('**/api/topology**', (route) => route.fulfill({
    json: {
      generatedAt: '2026-07-19T12:00:00Z',
      windowDays: 30,
      limited: false,
      summary: { nodes: 0, links: 0, observations: 0, connectedComponents: 0, likelyBridges: 0, isolatedNodes: 0 },
      analysis: { connectedComponents: 0, bridgeNodeIds: [], isolatedNodeIds: [] },
      nodes: [],
      links: [],
    },
  }));

  await page.goto('/topology');
  const workspace = page.locator('.topology-page__workspace');
  expect(await workspace.evaluate((element) => getComputedStyle(element).gridTemplateColumns.split(' ').length)).toBe(1);
  const map = page.getByLabel('Geographic repeater topology map');
  await expect(map).toBeVisible();
  expect((await map.boundingBox())?.width ?? 0).toBeGreaterThan(700);
});

test('public site exposes its primary journeys', async ({ page }) => {
  await installPublicPolicy(page);
  await page.route('**/api/stats**', async (route) => {
    await route.fulfill({
      json: {
        totalNodes: 120,
        packetsDay: 42_000,
        internationalNodes: 2,
        internationalLastSeen: '2026-07-11T11:55:00Z',
        internationalLastCountry: 'France',
      },
    });
  });
  await page.route('**/api/topology**', async (route) => {
    await route.fulfill({
      json: {
        generatedAt: '2026-07-11T12:00:00Z', windowDays: 30, limited: false,
        summary: { nodes: 3, links: 2, observations: 25, connectedComponents: 1, likelyBridges: 0, isolatedNodes: 0 },
        analysis: { connectedComponents: 1, bridgeNodeIds: [], isolatedNodeIds: [] },
        nodes: [
          { nodeId: 'A'.repeat(64), name: 'Alpha', lat: 52, lon: -1, degree: 1, observations: 20 },
          { nodeId: 'B'.repeat(64), name: 'Bravo', lat: 53, lon: -2, degree: 1, observations: 20 },
          { nodeId: 'C'.repeat(64), name: 'No location', lat: null, lon: null, degree: 1, observations: 5 },
        ],
        links: [
          {
            source: 'A'.repeat(64), target: 'B'.repeat(64), observations: 20,
            strongObservations: 5, pathLossDb: 110, lastObserved: '2026-07-11T11:00:00Z',
          },
          {
            source: 'A'.repeat(64), target: 'C'.repeat(64), observations: 5,
            strongObservations: 1, pathLossDb: 120, lastObserved: '2026-07-11T10:00:00Z',
          },
        ],
      },
    });
  });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'UK Mesh Network' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Open live map' }).first()).toBeVisible();
  await expect(page.getByText(/regions need attention/i)).toHaveCount(0);
  await expect(page.getByText(/support the creators of meshcore/i)).toHaveCount(0);
  await expect(page.getByRole('link', { name: /donate/i })).toHaveCount(0);
  const homeAccessibility = await new AxeBuilder({ page }).analyze();
  expect(homeAccessibility.violations
    .filter((violation) => violation.impact === 'critical' || violation.impact === 'serious')
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);

  await page.getByRole('link', { name: 'Install MeshCore' }).first().click();
  await expect(page).toHaveURL(/\/install$/);
  await expect(page.getByRole('heading', { level: 1 })).toBeVisible();

  await page.locator('.site-nav__group > summary').filter({ hasText: 'Tools' }).click();
  await page.getByRole('link', { name: 'Topology' }).click();
  await expect(page).toHaveURL(/\/topology$/);
  await expect(page.getByRole('heading', { name: 'Repeater topology' })).toBeVisible();
  await expect(page.getByLabel('Geographic repeater topology map')).toBeVisible();
  await expect(page.getByText('2 mapped repeaters · 2 observed relationships')).toBeVisible();

  await expect(page.getByRole('link', { name: 'Health', exact: true })).toHaveCount(0);
});

// Guards issue #3 (contrast): the feed chrome renders the muted/secondary text
// tokens (--text-muted / --text-secondary) that the repeater-tree window reuses,
// so a colour-contrast-scoped scan here catches any regression in those tokens.
test('feed page text meets WCAG AA colour contrast', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

  await page.goto('/feed');
  await expect(page.locator('.uk-feed-channels')).toBeVisible();

  const contrast = await new AxeBuilder({ page })
    .withRules(['color-contrast'])
    .exclude('.maplibregl-canvas')
    .analyze();
  expect(contrast.violations
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);
});


test('navigation groups support keyboard opening, Escape and mobile focus restoration', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await page.route('**/api/**', route => {
    const url = new URL(route.request().url());
    return route.fulfill({ json: publicFixture(url.pathname, url.searchParams) ?? {} });
  });
  await page.goto('/');
  const tools = page.locator('.site-nav__group > summary').filter({ hasText: 'Tools' });
  await tools.focus();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('link', { name: 'Topology', exact: true })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.getByRole('link', { name: 'Topology', exact: true })).toBeHidden();
  await expect(tools).toBeFocused();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole('link', { name: 'Live Map (external app)', exact: true })).toBeVisible();
  const menu = page.locator('.site-nav__hamburger');
  await menu.focus();
  await page.keyboard.press('Enter');
  const menuLinks = page.locator('.site-nav__links');
  await expect(menuLinks.getByRole('link', { name: 'Regions', exact: true })).toBeVisible();
  for (const name of ['Repeaters', 'Regions', 'Feed', 'Health Check']) {
    await page.keyboard.press('Tab');
    await expect(menuLinks.getByRole('link', { name, exact: true })).toBeFocused();
  }
  await page.keyboard.press('Tab');
  await expect(tools).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('link', { name: 'Topology', exact: true })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(menu).toBeFocused();
  await expect(menu).toHaveAttribute('aria-expanded', 'false');
});
