import { expect, test, type Page, type Route } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const NODE_ID = 'A'.repeat(64);

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

const chartStats = {
  packetsPerHour: [{ hour: '12:00', count: 24 }],
  packetsPerDay: [{ day: 'Mon', count: 120 }],
  radiosPerHour: [{ hour: '12:00', count: 8 }],
  radiosPerDay: [{ day: 'Mon', count: 32 }],
  packetTypes: [{ label: 'Advert', count: 80 }, { label: 'Group', count: 40 }],
  channelTraffic: [{ channel: 'Public', count: 40, pct: 50, allPct: 33 }],
  hopDistribution: [{ hops: 1, count: 60 }],
  prefixCollisions: [{ prefix: 'ab', repeats: 3 }],
  observerRegions: [],
  observerDiversity: {
    averageObserversPerPacket: 2.5,
    maxObserversPerPacket: 6,
    totalPackets24h: 120,
    singleObserverPackets24h: 20,
    singleObserverPct24h: 16.7,
  },
  signalSummary: {
    avgRssi: -92,
    medianRssi: -91,
    avgSnr: 4.5,
    medianSnr: 4.2,
    rssiSamples24h: 120,
    snrSamples24h: 120,
  },
  routeTypes: [],
  transportCodes: [],
  pathDecodeTrend: [],
  pathHashes: {
    last24hHops: { one_byte: 30, two_byte: 20, three_byte: 10 },
    multibytePackets24h: 30,
    fullyDecodedMultibyte24h: 20,
    latestMultibyteAt: '2026-07-18T12:00:00Z',
    latestMultibyteHash: 'fixture-multibyte',
    latestFullyDecodedAt: '2026-07-18T12:00:00Z',
    latestFullyDecodedHash: 'fixture-path',
    latestFullyDecodedHops: 1,
    latestFullyDecodedPath: 'North -> South',
    latestFullyDecodedNodes: [
      { ord: 1, node_id: 'node-north', name: 'North', lat: 54.5, lon: -1.2 },
      { ord: 2, node_id: 'node-south', name: 'South', lat: 53.5, lon: -1.1 },
    ],
    longestFullyDecodedAt: '2026-07-18T12:00:00Z',
    longestFullyDecodedHash: 'fixture-longest',
    longestFullyDecodedHops: 1,
    longestFullyDecodedPath: 'North -> South',
    longestFullyDecodedNodes: [
      { ord: 1, node_id: 'node-north', name: 'North', lat: 54.5, lon: -1.2 },
      { ord: 2, node_id: 'node-south', name: 'South', lat: 53.5, lon: -1.1 },
    ],
  },
  summary: {
    totalPackets24h: 120,
    totalPackets7d: 720,
    uniqueRadios24h: 32,
    peakHour: '12:00',
    peakHourCount: 24,
  },
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
  if (pathname === '/api/stats/charts') return route.fulfill({ json: chartStats });
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
  await page.routeWebSocket(/\/ws(?:\?|$)/, (socket) => {
    socket.send(JSON.stringify({
      type: 'initial_state',
      ts: Date.now(),
      data: {
        nodes: [{
          ...repeater,
          public_key: NODE_ID,
        }],
        packets: [{
          time: '2026-07-18T12:00:00Z',
          packet_hash: 'fixture-message',
          rx_node_id: NODE_ID,
          src_node_id: NODE_ID,
          packet_type: 5,
          hop_count: 1,
          summary: 'Keyboard fixture message',
          path_hashes: [],
        }],
        viable_links: [],
      },
    }));
  });
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
  '/repeater',
  '/companion',
  '/install',
  '/docs',
  '/login',
  '/open-source',
  '/stats',
  '/topology',
  '/spam',
] as const;

for (const route of publicRoutes) {
  test(`public route ${route} has no serious or critical axe issue`, async ({ page }) => {
    await installFixtures(page);
    await page.goto(route);
    await expect(page.locator('.site-main')).toBeVisible();
    await expectNoSeriousAxeIssues(page);
  });
}

test('dashboard dialogs trap focus, close with Escape, and restore the trigger', async ({ page }) => {
  await installFixtures(page, false);
  await page.goto('http://127.0.0.1:4174/');

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

test('stats tabs support Arrow, Home, and End and the path dialog restores focus', async ({ page }) => {
  await installFixtures(page);
  await page.goto('/stats');

  const overview = page.getByRole('tab', { name: 'Overview' });
  await expect(overview).toHaveAttribute('aria-selected', 'true');
  await overview.focus();
  await page.keyboard.press('End');
  await expect(page.getByRole('tab', { name: 'Signal' })).toHaveAttribute('aria-selected', 'true');
  await page.keyboard.press('Home');
  await expect(overview).toHaveAttribute('aria-selected', 'true');
  await page.keyboard.press('ArrowRight');
  await expect(page.getByRole('tab', { name: 'Traffic' })).toHaveAttribute('aria-selected', 'true');

  const paths = page.getByRole('tab', { name: 'Paths' });
  await paths.click();
  const pathTrigger = page.getByRole('button', { name: 'North -> South' }).first();
  await pathTrigger.click();
  const dialog = page.getByRole('dialog', { name: 'Decoded path map' });
  await expect(dialog).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(dialog).toBeHidden();
  await expect(pathTrigger).toBeFocused();
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

  await search.evaluate((input: HTMLInputElement) => {
    input.focus();
    input.dispatchEvent(new CompositionEvent('compositionstart', { bubbles: true, data: '東' }));
    input.value = '東';
    input.dispatchEvent(new InputEvent('input', {
      bubbles: true,
      data: '東',
      inputType: 'insertCompositionText',
      isComposing: true,
    }));
    input.dispatchEvent(new CompositionEvent('compositionend', { bubbles: true, data: '東' }));
  });
  await expect(search).toHaveAttribute('aria-autocomplete', 'list');
  await expect(search).toHaveValue('東');
});

test('reduced-motion preference removes repeating CSS animation', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await installFixtures(page);
  await page.goto('http://127.0.0.1:4174/');
  const repeating = await page.evaluate(() => Array.from(document.querySelectorAll('*'))
    .filter((element) => {
      const style = getComputedStyle(element);
      return style.animationName !== 'none' && style.animationIterationCount === 'infinite';
    })
    .map((element) => `${element.tagName.toLowerCase()}.${element.className}`));
  expect(repeating).toEqual([]);
});

test('keyboard-only dashboard flow selects a node, reads details, and toggles a packet row', async ({ page }) => {
  await installFixtures(page);
  await installDashboardWebSocket(page);
  await page.goto('http://127.0.0.1:4174/');

  const packet = page.locator('.packet-item').filter({ hasText: 'Keyboard fixture message' });
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
