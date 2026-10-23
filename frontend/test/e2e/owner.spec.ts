import { expect, test } from '@playwright/test';
import { installMapRoutes } from './mapFixtures.js';

const NODE_ID = 'A'.repeat(64);

const dashboard = {
  nodes: [{
    node_id: NODE_ID,
    canonicalId: NODE_ID,
    members: [NODE_ID],
    name: 'Alpha Repeater',
    network: 'ukmesh',
    last_seen: '2026-07-16T10:00:00Z',
    advert_count: 12,
    lat: 54.5,
    lon: -1.2,
    iata: 'TST',
    role: 2,
  }],
};

test.beforeEach(async ({ page }) => {
  await installMapRoutes(page);
  await page.route('**/api/runtime-config', (route) => route.fulfill({ json: {
    version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 30,
  } }));
  await page.route('**/*basemaps.cartocdn.com/**', (route) => route.abort());
});

test('unlinked owner sees the broker denial fix and can open the exact setup section', async ({ page }) => {
  await page.route('**/api/owner/session', route => route.fulfill({ status: 401, json: { error: 'Not logged in' } }));
  await page.route('**/api/owner/csrf', route => route.fulfill({ json: { csrfToken: 'fixture-csrf' } }));
  await page.route('**/api/owner/login', route => route.fulfill({ status: 403, json: {
    error: 'The broker denied a publish to "meshcore". Set Topic root to meshcore/{IATA}/{PUBLIC_KEY}/packets and apply the settings.',
  } }));
  await page.goto('/login');
  await page.getByLabel('MQTT username', { exact: true }).fill('unlinked-owner');
  await page.getByLabel('MQTT password', { exact: true }).fill('fixture-password');
  await page.getByRole('button', { name: 'Login', exact: true }).click();
  const error = page.getByRole('alert').filter({ hasText: 'The broker denied' });
  await expect(error).toContainText('meshcore/{IATA}/{PUBLIC_KEY}/packets');
  await expect(page.getByRole('heading', { name: 'Repeater owner dashboard' })).toHaveCount(0);
  await error.getByRole('link', { name: 'MQTT setup and troubleshooting' }).click();
  await expect(page).toHaveURL(/\/install#mqtt-observer$/);
  await expect(page.getByRole('heading', { name: 'Firmware MQTT settings: change the default topic root' })).toBeVisible();
  await expect(page.getByLabel('Firmware MQTT topic root', { exact: true }).locator('pre')).toHaveText('meshcore/{IATA}/{PUBLIC_KEY}/packets');
  await expect(page.getByLabel('Firmware MQTT topic root repair commands')).toContainText('get mqtt.1.topic.root');
});

test('linked owner sees a recent default-root denial and the hint clears on session refresh', async ({ page }) => {
  let showDiagnostic = true;
  let requests = 0;
  await page.clock.install();
  await page.route('**/api/owner/session', route => {
    requests += 1;
    return route.fulfill({ json: { ok: true, mqttUsername: 'linked-owner', dashboard: {
      ...dashboard,
      mqttDiagnostic: showDiagnostic ? {
        code: 'MQTT_DEFAULT_TOPIC_ROOT',
        message: 'The broker denied a publish to "meshcore". Set Topic root to meshcore/{IATA}/{PUBLIC_KEY}/packets.',
        observedAt: new Date().toISOString(),
      } : null,
    } } });
  });
  await page.route('**/api/owner/live?**', route => route.fulfill({ status: 503, json: { error: 'Fixture has no live data' } }));
  await page.route('**/api/owner/live-last-hop?**', route => route.fulfill({ json: { points: [] } }));
  await page.route('**/api/owner/public-feed', route => route.fulfill({ status: 503, json: { error: 'Fixture' } }));
  await page.route('**/api/owner/packet-sharing?**', route => route.fulfill({ status: 503, json: { error: 'Fixture' } }));
  await page.goto('/login');
  const hint = page.getByRole('status').filter({ hasText: 'Recent MQTT publish denial' });
  await expect(hint).toContainText('meshcore/{IATA}/{PUBLIC_KEY}/packets');
  await expect(page.getByLabel('Owned repeater identities')).toContainText('Alpha Repeater');
  showDiagnostic = false;
  const before = requests;
  await page.clock.fastForward(15_001);
  await expect.poll(() => requests).toBeGreaterThan(before);
  await expect(hint).toHaveCount(0);
  await expect(page.getByLabel('Owned repeater identities')).toContainText('Alpha Repeater');
});

test('session polling does not reset the repeater owner content', async ({ page }) => {
  // Production owner polling predates main's /api/owner/session cadence and is
  // authoritative for Stage 1; session-polling parity is a Stage 2 item.
  test.fixme(true, 'deferred: owner session-polling parity (Stage 2)');
  let sessionRequests = 0;
  let liveRequests = 0;
  let liveRequestLimit: number | null = null;

  await page.clock.install({ time: new Date('2026-07-16T12:00:00Z') });
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
  const identities = page.getByLabel('Owned repeater identities');
  await expect(identities.getByText('Alpha Repeater', { exact: true })).toBeVisible({ timeout: 15_000 });
  await expect(page.locator('.owner-section-tabs')).toHaveCount(0);
  await expect(page.locator('.owner-settings')).toHaveCount(0);

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
  await expect(identities.getByText('Alpha Repeater', { exact: true })).toBeVisible();
  await expect(page.getByText('Unnamed', { exact: true })).toHaveCount(0);
  expect(liveRequests).toBe(initialLiveRequests + 1);
});

test('owner map construction remains one across repeated live polls', async ({ page }) => {
  // Same main-only owner cadence as the session-polling spec above; the
  // deployed owner client is authoritative for Stage 1.
  test.fixme(true, 'deferred: owner live-poll map lifecycle parity (Stage 2)');
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
  await expect(page.getByRole('heading', { name: 'Direct Sender Map' })).toBeVisible({ timeout: 15_000 });
  await expect(page.getByRole('heading', { name: 'Node Telemetry' })).toBeVisible();
  await expect(page.locator('.owner-section-tabs')).toHaveCount(0);
  await expect(page.locator('.owner-settings')).toHaveCount(0);
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
