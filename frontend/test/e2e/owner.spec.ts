import { expect, test } from '@playwright/test';

const NODE_ID = 'A'.repeat(64);

const dashboard = {
  nodes: [{
    node_id: NODE_ID,
    name: 'Alpha Repeater',
    network: 'ukmesh',
    last_seen: '2026-07-16T10:00:00Z',
    advert_count: 12,
    lat: 54.5,
    lon: -1.2,
    iata: 'TST',
    role: 2,
  }],
  totals: {
    ownedNodes: 1,
    packets24h: 25,
    packets7d: 100,
    packetsReceived24h: 20,
  },
  roadmap: [],
};

test('session polling does not reset the repeater owner content', async ({ page }) => {
  let sessionRequests = 0;
  let liveRequests = 0;
  let liveRequestLimit: number | null = null;

  await page.clock.install({ time: new Date('2026-07-16T12:00:00Z') });
  await page.route('https://*.basemaps.cartocdn.com/**', (route) => route.abort());
  await page.route('**/api/owner/session', async (route) => {
    sessionRequests += 1;
    await route.fulfill({ json: { ok: true, dashboard, mqttUsername: 'alpha-owner' } });
  });
  await page.route('**/api/owner/live?**', async (route) => {
    liveRequests += 1;
    // A dashboard-object dependency used to start an extra live request at the
    // 15-second session refresh. Leave that unexpected request pending so the
    // old behaviour visibly resets to "Unnamed" and fails this regression test.
    if (liveRequestLimit != null && liveRequests > liveRequestLimit) return;
    await route.fulfill({
      json: {
        nodeId: NODE_ID,
        ownerNode: dashboard.nodes[0],
        incomingPeers: [],
        heardBy: [],
        linkHealth: [],
        advertTrend24h: [],
        telemetry24h: [],
        packetsSent24h: 25,
        packetsReceived24h: 20,
        alerts: [],
        recentPackets: [],
      },
    });
  });
  await page.route('**/api/owner/live-last-hop?**', async (route) => {
    await route.fulfill({ json: { points: [] } });
  });

  await page.goto('/login');
  await expect(page.getByText('Alpha Repeater', { exact: true })).toBeVisible();

  const initialSessionRequests = sessionRequests;
  const initialLiveRequests = liveRequests;
  liveRequestLimit = initialLiveRequests + 1;
  const sessionRefresh = page.waitForResponse((response) => (
    new URL(response.url()).pathname === '/api/owner/session'
    && sessionRequests > initialSessionRequests
  ));
  await page.clock.fastForward(15_001);
  await sessionRefresh;
  await page.clock.fastForward(100);
  await expect(page.getByText('Alpha Repeater', { exact: true })).toBeVisible();
  await expect(page.getByText('Unnamed', { exact: true })).toHaveCount(0);
  expect(liveRequests).toBe(initialLiveRequests + 1);
});

test('owner map construction remains one across repeated live polls', async ({ page }) => {
  let liveRequests = 0;
  await page.clock.install({ time: new Date('2026-07-16T12:00:00Z') });
  await page.addInitScript(() => {
    const state = { constructions: 0 };
    Object.defineProperty(window, '__ownerMapLifecycle', { value: state });
    new MutationObserver((records) => {
      for (const record of records) {
        for (const node of record.addedNodes) {
          if (!(node instanceof Element)) continue;
          if (node.matches('.maplibregl-canvas')) state.constructions += 1;
          state.constructions += node.querySelectorAll('.maplibregl-canvas').length;
        }
      }
    }).observe(document, { childList: true, subtree: true });
  });
  await page.route('https://*.basemaps.cartocdn.com/**', (route) => route.abort());
  await page.route('**/api/owner/session', (route) => route.fulfill({
    json: { ok: true, dashboard, mqttUsername: 'alpha-owner' },
  }));
  await page.route('**/api/owner/live?**', (route) => {
    liveRequests += 1;
    return route.fulfill({
      json: {
        nodeId: NODE_ID,
        ownerNode: dashboard.nodes[0],
        incomingPeers: [],
        heardBy: [],
        linkHealth: [],
        advertTrend24h: [],
        telemetry24h: [],
        packetsSent24h: 25,
        packetsReceived24h: 20,
        alerts: [],
        recentPackets: [],
      },
    });
  });
  await page.route('**/api/owner/live-last-hop?**', (route) => route.fulfill({ json: { points: [] } }));

  await page.goto('/login');
  await page.getByRole('button', { name: 'live', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Direct Sender Map' })).toBeVisible();
  await expect(page.locator('.owner-map')).toHaveCount(1);
  await expect.poll(() => page.evaluate(() => (
    (window as typeof window & { __ownerMapLifecycle: { constructions: number } })
      .__ownerMapLifecycle.constructions
  ))).toBeGreaterThan(0);
  const initialCanvasCreations = await page.evaluate(() => (
    (window as typeof window & { __ownerMapLifecycle: { constructions: number } })
      .__ownerMapLifecycle.constructions
  ));

  const initialLiveRequests = liveRequests;
  await page.clock.fastForward(30_001);
  await expect.poll(() => liveRequests).toBeGreaterThan(initialLiveRequests);
  expect(await page.evaluate(() => (
    (window as typeof window & { __ownerMapLifecycle: { constructions: number } })
      .__ownerMapLifecycle.constructions
  ))).toBe(initialCanvasCreations);
});
