import { expect, test, type Page, type Route, type TestInfo } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const NODE_ID = 'A'.repeat(64);

function dashboardBaseUrl(info: TestInfo): string {
  const url = info.config.projects.find((project) => project.name === 'dashboard-desktop')?.use.baseURL;
  if (typeof url !== 'string') throw new Error('dashboard-desktop baseURL is required');
  return url;
}


const TEST_MAP_STYLE = {
  version: 8,
  sources: {
    openmaptiles: {
      type: 'vector',
      url: 'https://tiles.openfreemap.org/planet',
    },
  },
  glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  layers: [
    { id: 'bg', type: 'background', paint: { 'background-color': '#080d14' } },
  ],
};



const repeater = {
  node_id: NODE_ID,
  name: 'Alpha Repeater',
  lat: 54.5,
  lon: 0,
  iata: 'TST',
  role: 2,
  last_seen: '2026-07-18T12:00:00Z',
  is_online: true,
  hardware_model: 'Fixture',
  advert_count: 12,
  elevation_m: 100,
};

async function fulfillApi(route: Route) {
  const pathname = new URL(route.request().url()).pathname;
  if (pathname === '/api/runtime-config') return route.fulfill({ json: {
    version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 300,
  } });
  if (pathname === '/api/feed/regions') return route.fulfill({ json: {
    network: 'ukmesh', observer: null, visibilityGeneration: 1, regions: ['LHR'],
  } });
  if (pathname === '/api/stats') {
    return route.fulfill({
      json: {
        totalNodes: 32,
        packetsDay: 120,
        internationalNodes: 1,
        internationalLastSeen: '2026-07-18T12:00:00Z',
        internationalLastCountry: 'France',
      },
    });
  }
  if (pathname === '/api/topology') {
    return route.fulfill({
      json: {
        generatedAt: '2026-07-18T12:00:00Z',
        windowDays: 30,
        limited: false,
        summary: {
          nodes: 0,
          links: 0,
          observations: 0,
          connectedComponents: 0,
          likelyBridges: 0,
          isolatedNodes: 0,
        },
        analysis: { connectedComponents: 0, bridgeNodeIds: [], isolatedNodeIds: [] },
        nodes: [],
        links: [],
      },
    });
  }
  if (pathname === '/api/rf-validation') {
    return route.fulfill({
      json: {
        methodology: 'Fixture RF validation methodology.',
        summary: {
          evaluated: 0,
          matches: 0,
          mismatches: 0,
          observedUnexpected: 0,
          operatorOverrides: 0,
          weakModelEvidence: 0,
        },
        mismatches: [],
      },
    });
  }
  if (pathname === '/api/nodes/map') {
    return route.fulfill({
      json: {
        nodes: [repeater],
        page: {
          snapshot: 'fixture',
          nextCursor: null,
          complete: true,
          returned: 1,
          rowLimit: 2_000,
        },
      },
    });
  }
  if (/^\/api\/nodes\/[^/]+\/(?:links|history|adverts)$/.test(pathname)) {
    return route.fulfill({ json: [] });
  }
  if (pathname === '/api/repeaters/firmware') return route.fulfill({ json: { total: 0, versions: [] } });
  if (pathname === '/api/companion-activity') return route.fulfill({ json: [] });
  if (pathname === '/api/node-status/latest') return route.fulfill({ json: [] });
  if (pathname === '/api/owner/session') return route.fulfill({ json: { ok: false } });
  if (pathname.endsWith('/packets/recent')) return route.fulfill({ json: [] });
  if (pathname.endsWith('/inferred-nodes')) {
    return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
  }
  return route.fulfill({ json: {} });
}

async function installMapRoutes(page: Page) {
  await page.route('https://tiles.openfreemap.org/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname === '/styles/dark' || pathname === '/styles/positron') {
      await route.fulfill({ json: TEST_MAP_STYLE });
      return;
    }
    await route.abort();
  });
  await page.route('**/*basemaps.cartocdn.com/**', (route) => route.abort());
}

async function installFixtures(page: Page, dismissDisclaimer = true) {
  await page.addInitScript((dismiss) => {
    localStorage.setItem('meshcore-cookie-consent-v1', '1');
    if (dismiss) localStorage.setItem('meshcore-disclaimer-dismissed', '1');
  }, dismissDisclaimer);
  await installMapRoutes(page);
  await page.route('**/api/**', fulfillApi);
}

async function installDashboardWebSocket(page: Page) {
  await page.addInitScript(({ fixtureNode, nodeId }) => {
    const NativeWebSocket = window.WebSocket;
    class FixtureWebSocket extends EventTarget {
      static readonly CONNECTING = 0; static readonly OPEN = 1;
      static readonly CLOSING = 2; static readonly CLOSED = 3;
      readyState = FixtureWebSocket.CONNECTING;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent<string>) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      constructor(url: string | URL, protocols?: string | string[]) {
        super();
        if (new URL(url).pathname !== '/ws') {
          return new NativeWebSocket(url, protocols) as unknown as FixtureWebSocket;
        }
        window.setTimeout(() => {
          if (this.readyState !== FixtureWebSocket.CONNECTING) return;
          this.readyState = FixtureWebSocket.OPEN;
          this.onopen?.(new Event('open'));
          this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({
            type: 'initial_state', ts: Date.now(), data: {
              nodes: [{ ...fixtureNode, last_seen: new Date().toISOString(), network: 'ukmesh', public_key: nodeId }],
              packets: [{ time: new Date().toISOString(), network: 'ukmesh', packet_hash: 'fixture-message',
                rx_node_id: nodeId, src_node_id: nodeId, packet_type: 5, hop_count: 1,
                summary: 'Keyboard fixture message', path_hashes: [] }],
              viable_links: [],
            },
          }) }));
        }, 0);
      }
      send() {} close() { this.readyState = FixtureWebSocket.CLOSED; }
    }
    Object.defineProperty(window, 'WebSocket', { value: FixtureWebSocket, configurable: true });
  }, { fixtureNode: repeater, nodeId: NODE_ID });
}

async function expectNoSeriousAxeIssues(page: Page) {
  await page.evaluate(async () => {
    await document.fonts.ready;
    await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  });
  const result = await new AxeBuilder({ page })
    .exclude('.maplibregl-canvas')
    .analyze();
  expect(result.violations
    .filter((violation) => violation.impact === 'critical' || violation.impact === 'serious')
    .map((violation) => ({
      id: violation.id,
      targets: violation.nodes.map((node) => node.target.join(' ')),
    }))).toEqual([]);
}

const publicRoutes = [
  '/',
  '/feed',
  '/feed/packets',
  '/repeater',
  '/companion',
  '/install',
  '/docs',
  '/login',
  '/open-source',
  '/topology',
  '/spam',
] as const;

test('old stats links lead to live packets without requesting charts', async ({ page }) => {
  await installFixtures(page);
  const chartRequests: string[] = [];
  page.on('request', request => {
    if (new URL(request.url()).pathname === '/api/stats/charts') chartRequests.push(request.url());
  });
  await page.goto('/stats');
  await expect(page).toHaveURL(/\/feed\/packets$/);
  await expect(page.getByRole('button', { name: 'LHR', exact: true })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Stats', exact: true, includeHidden: true })).toHaveCount(0);
  expect(chartRequests).toEqual([]);
});

for (const route of publicRoutes) {
  test(`public route ${route} has no serious or critical axe issue`, async ({ page }) => {
    await installFixtures(page);
    await page.goto(route);
    await expect(page.locator('.site-main')).toBeVisible();
    await expectNoSeriousAxeIssues(page);
  });
}

test('dashboard dialogs trap focus, close with Escape, and restore the trigger', async ({ page }, info) => {
  await installFixtures(page, false);
  await page.goto(dashboardBaseUrl(info));

  const initialDialog = page.getByRole('dialog', { name: 'Data disclaimer' });
  await expect(initialDialog).toBeVisible();
  expect(await initialDialog.evaluate((dialog) => (
    dialog === document.activeElement || dialog.contains(document.activeElement)
  ))).toBe(true);
  await page.keyboard.press('Escape');
  await expect(initialDialog).toBeHidden();

  const trigger = page.getByRole('button', { name: 'Data disclaimer' });
  await trigger.focus();
  await trigger.click();
  await expect(initialDialog).toBeVisible();
  await expect.poll(() => initialDialog.evaluate((dialog) => (
    dialog === document.activeElement || dialog.contains(document.activeElement)
  ))).toBe(true);
  await page.keyboard.press('Escape');
  await expect(initialDialog).toBeHidden();
  await expect(trigger).toBeFocused();
});

test('repeater search exposes keyboard selection and tolerates IME composition', async ({ page }) => {
  await installFixtures(page);
  await page.goto('/repeater');

  const search = page.getByRole('combobox', { name: 'Search repeaters' });
  await search.fill('Alpha');
  await expect(page.getByRole('option', { name: /Alpha Repeater/ })).toBeVisible();
  await search.press('ArrowDown');
  await search.press('Enter');
  await expect(page.getByRole('heading', { name: 'Alpha Repeater' })).toBeVisible();
  await expect(page.getByText(/54\.50000, 0\.00000/)).toBeVisible();

  // Let Chromium perform composition. Assigning input.value updates React's
  // value tracker before a synthetic input event and does not model user input.
  await search.selectText();
  const ime = await page.context().newCDPSession(page);
  try {
    await ime.send('Input.imeSetComposition', { text: '東', selectionStart: 1, selectionEnd: 1 });
    await ime.send('Input.insertText', { text: '東' });
  } finally { await ime.detach(); }
  await expect(search).toHaveAttribute('aria-autocomplete', 'list');
  await expect(search).toHaveValue('東');
  await expect(page).toHaveURL(url => url.searchParams.get('q') === '東');
});

test('reduced-motion preference removes repeating CSS animation', async ({ page }, info) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await installFixtures(page);
  await page.goto(dashboardBaseUrl(info));
  const repeating = await page.evaluate(() => Array.from(document.querySelectorAll('*'))
    .filter((element) => {
      const style = getComputedStyle(element);
      return style.animationName !== 'none' && style.animationIterationCount === 'infinite';
    })
    .map((element) => `${element.tagName.toLowerCase()}.${element.className}`));
  expect(repeating).toEqual([]);
});

test('keyboard-only dashboard flow selects a node, reads details, and toggles a packet row', async ({ page }, info) => {
  await installFixtures(page);
  await installDashboardWebSocket(page);
  await page.goto(dashboardBaseUrl(info));

  const packet = page.getByRole('button', { name: /^(?:Pin|Unpin) GRP packet: Keyboard fixture message$/ });
  await expect(packet).toHaveAttribute('aria-label', /Pin GRP packet/);
  await packet.focus();
  await page.keyboard.press('Space');
  await expect(packet).toHaveAttribute('aria-pressed', 'true');

  const search = page.getByRole('combobox', { name: 'Search map nodes' });
  await search.fill('Alpha');
  await expect(page.getByRole('option', { name: /Alpha Repeater/ })).toBeVisible();
  await search.press('ArrowDown');
  await search.press('Enter');
  const nodeDetails = page.getByRole('dialog', { name: 'Node details' });
  await expect(nodeDetails.getByRole('heading', { name: 'Alpha Repeater' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Info' })).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('listbox', { name: /Search map nodes/ })).toBeHidden();

  const theme = page.getByTitle('Toggle map theme');
  await theme.focus();
  await page.keyboard.press('Enter');
  await expect(theme).toContainText('Light');
});
