import assert from 'node:assert/strict';
import test from 'node:test';
import { chromium, expect, type Route } from '@playwright/test';

const browserTest = process.env['OWNER_BROWSER_TESTS'] === '1' ? test : test.skip;
const base = process.env['OWNER_BROWSER_BASE_URL'] ?? 'http://127.0.0.1:3004';
const first = 'A'.repeat(64), second = 'B'.repeat(64), peerId = 'C'.repeat(64);
const node = (id: string, name: string) => ({ node_id: id, canonicalId: id, members: [id], name,
  network: 'ukmesh', iata: 'MME', lat: 54.5, lon: -1.3, role: 2, advert_count: 8, last_seen: '2026-09-08T01:00:00Z' });
function live(id = first) {
  const peer = { ...node(peerId, 'Direct peer'), packets_24h: 20 };
  return { nodeId: id, ownerNode: node(id, id === first ? 'First repeater' : 'Second repeater'),
    incomingPeers: [peer], heardBy: [{ ...peer, name: 'Receiving peer', packets_7d: 80, best_hops: 1 }],
    linkHealth: [{ peer_node_id: peerId, peer_name: 'RF peer', peer_network: 'ukmesh', owner_to_peer: 8,
      peer_to_owner: 20, observed_count: 28, itm_path_loss_db: 115, itm_viable: true, force_viable: false, last_observed: '2026-09-08T01:00:00Z' }],
    advertTrend24h: [{ bucket: '2026-09-08T01:00:00Z', adverts: 8 }],
    telemetry24h: [{ bucket: '2026-09-08T01:00:00Z', batteryPct: 80, batteryMv: 4100, uptimeSecs: 10000, channelUtilPct: 10, airUtilTxPct: 2 }],
    status: null, heardNeighbors: [{ id: peerId, rssi: -95, snr: 4.5, last_seen_at: '2026-09-08T01:00:00Z', sampled_at: null }],
    heardNeighborsSource: 'direct_packets', heardNeighborsSampledAt: null,
    packetsSent24h: 25, packetsReceived24h: 100,
    alerts: [{ level: 'info', message: 'An actual diagnostic' }],
    recentPackets: [{ time: '2026-09-08T01:00:00Z', packet_hash: 'packet', packet_type: 5, route_type: 1, hop_count: 0, src_node_id: peerId, src_node_name: 'Packet peer', sender: 'Sender', body: 'Packet content' }],
  };
}
const labels = ['Direct Sender Map', 'Alerts', 'Advert Trend', 'RF Link Health', 'Direct Senders', 'Nodes That Heard This Repeater', 'Heard neighbors', 'Live packets'];

async function setup(handler: (route: Route) => Promise<void>) {
  const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
  const context = await browser.newContext({ serviceWorkers: 'block' });
  const page = await context.newPage();
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await context.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== base) { await route.abort(); return; }
    if (url.pathname === '/api/owner/session') {
      await route.fulfill({ json: { ok: true, mqttUsername: 'fixture-owner', dashboard: { nodes: [node(first, 'First repeater'), node(second, 'Second repeater')] } } }); return;
    }
    if (url.pathname === '/api/owner/live') { await handler(route); return; }
    if (url.pathname === '/api/owner/live-last-hop') { await route.fulfill({ json: { points: [] } }); return; }
    if (url.pathname === '/api/runtime-config') {
      await route.fulfill({ json: { version: 1, packetArcs: false, heatmap: false, privacyGeneration: 0, refreshAfterSeconds: 300 } }); return;
    }
    if (url.pathname.startsWith('/api/')) { await route.fulfill({ json: {} }); return; }
    await route.continue();
  });
  return { browser, context, page, errors };
}

for (const failure of ['http503', 'timeout', 'invalidSchema'] as const) {
  browserTest(`owner panels show ${failure} and immediate retry restores every panel`, { timeout: 35_000 }, async () => {
    let attempts = 0;
    const { browser, page, errors } = await setup(async route => {
      attempts += 1;
      if (attempts === 1) {
        if (failure === 'http503') await route.fulfill({ status: 503, json: { error: 'unavailable' } });
        if (failure === 'invalidSchema') await route.fulfill({ json: {} });
        return; // A real pending request exercises the browser/poller deadline.
      }
      await route.fulfill({ json: live() });
    });
    try {
      await page.goto(`${base}/login`, { waitUntil: 'domcontentloaded' });
      if (failure === 'timeout') {
        for (const label of labels) await expect(page.getByText(`Loading ${label}…`, { exact: true })).toBeVisible();
        await expect(page.getByText('No direct sender nodes found in the last 24 hours.')).toHaveCount(0);
      }
      for (const label of labels) await expect(page.getByRole('button', { name: `Retry ${label}`, exact: true })).toBeVisible({ timeout: 14_000 });
      await expect(page.getByRole('heading', { name: /RX Strength by Last Hop/ })).toBeVisible();
      await page.getByRole('button', { name: 'Retry Direct Senders', exact: true }).click();
      await expect(page.getByText('An actual diagnostic', { exact: true })).toBeVisible();
      await expect(page.locator('.owner-map-wrap canvas')).toBeVisible();
      await expect(page.locator('.owner-panel--trend .owner-trend')).toBeVisible();
      await expect(page.getByText('RF peer', { exact: true }).first()).toBeVisible();
      await expect(page.getByText('Direct peer', { exact: true })).toBeVisible();
      await expect(page.getByText('Receiving peer', { exact: true })).toBeVisible();
      await expect(page.getByText('Avg RSSI', { exact: true })).toBeVisible();
      await expect(page.getByText('Packet content', { exact: true })).toBeVisible();
      await expect(page.getByRole('alert').filter({ hasText: 'temporarily unavailable' })).toHaveCount(0);
      assert.equal(attempts, 2);
      assert.deepEqual(errors, []);
    } finally { await browser.close(); }
  });
}

browserTest('owner refresh failure retains data and changing node cannot show the old response', { timeout: 35_000 }, async () => {
  let attempts = 0;
  let pendingSecond: Route | undefined;
  const { browser, page, errors } = await setup(async route => {
    if (new URL(route.request().url()).searchParams.get('nodeId') === second) { pendingSecond = route; return; }
    attempts += 1;
    if (attempts === 2) await route.fulfill({ status: 500, json: { error: 'failed refresh' } });
    else await route.fulfill({ json: live() });
  });
  try {
    await page.goto(`${base}/login`, { waitUntil: 'domcontentloaded' });
    await expect(page.getByText('Packet content', { exact: true })).toBeVisible();
    await expect(page.getByRole('alert').filter({ hasText: 'Showing the last successful data' }).first()).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText('Packet content', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Retry Live packets', exact: true }).click();
    await expect(page.getByRole('alert').filter({ hasText: 'temporarily unavailable' })).toHaveCount(0);
    await page.locator(`article[data-canonical-id="${second}"]`).click();
    await expect(page.getByText('Loading Live packets…', { exact: true })).toBeVisible();
    await expect(page.getByText('Packet content', { exact: true })).toHaveCount(0);
    await expect.poll(() => Boolean(pendingSecond)).toBe(true);
    await pendingSecond!.fulfill({ json: { ...live(second), incomingPeers: [], heardBy: [], linkHealth: [], recentPackets: [], alerts: [], heardNeighbors: [], heardNeighborsSource: 'reported' } });
    await expect(page.getByText('No packets received by this node in the last 30 days.')).toBeVisible();
    await expect(page.getByText('The latest reported neighbor list is empty.', { exact: true })).toBeVisible();
    await expect(page.getByText('No active alerts.', { exact: true })).toBeVisible();
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});
