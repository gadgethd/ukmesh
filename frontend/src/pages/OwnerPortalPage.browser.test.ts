import { browserLane, browserLaunchOptions } from '../../test/browserLane.js';
import assert from 'node:assert/strict';
import { chromium, expect, type Route } from '@playwright/test';

const { browserTest, origin: base } = browserLane('OWNER_BROWSER_TESTS', 'OWNER_BROWSER_BASE_URL', 'http://127.0.0.1:3004');
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

async function setup(handler: (route: Route) => Promise<void>, extra?: (route: Route) => Promise<boolean>) {
  const browser = await chromium.launch(browserLaunchOptions());
  const context = await browser.newContext({ serviceWorkers: 'block' });
  const page = await context.newPage();
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await context.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== base) { await route.abort(); return; }
    if (await extra?.(route)) return;
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

const sharingSettings = (nodeId: string, selected: string[] = []) => ({ nodeId, featureEnabled: true, enabled: selected.length > 0,
  destinations: [{ id: 'relaylens', name: 'RelayLens' }, { id: 'another-site', name: 'Another site' }].map(item => ({ ...item,
    websiteUrl: null, description: null, configured: true, selected: selected.includes(item.id), lastForwardedAt: null })) });
const publicFeedSettings = (enabled = false) => ({ featureEnabled: true, configured: true, brokerUrl: 'wss://mqtt.example.test:443',
  enabled, nodeCount: 2, eligible: true, recentPacketCount: 3, topicFilters: ['public/+/+/packets'], lastForwardedAt: null });
const finishPublication = (page: import('@playwright/test').Page) => page.evaluate(() =>
  new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));

browserTest('a completed A write cannot replace B settings or its next destination write', { timeout: 35_000 }, async () => {
  let pending: Route | undefined, holdBReads = false;
  const writes: Array<{ nodeId: string; destinationIds: string[] }> = [];
  const { browser, page, errors } = await setup(async route => {
    await route.fulfill({ json: live(new URL(route.request().url()).searchParams.get('nodeId') ?? first) });
  }, async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/owner/csrf') { await route.fulfill({ json: { csrfToken: 'fixture-csrf' } }); return true; }
    if (url.pathname !== '/api/owner/packet-sharing') return false;
    if (route.request().method() === 'POST') {
      const body = route.request().postDataJSON(); writes.push(body);
      if (body.nodeId === first) { pending = route; return true; }
      await route.fulfill({ json: { nodeId: body.nodeId, enabled: true, destinationIds: body.destinationIds } }); return true;
    }
    const id = url.searchParams.get('nodeId') ?? first;
    if (id === second && holdBReads) return true;
    await route.fulfill({ json: sharingSettings(id, id === second ? ['another-site'] : []) }); return true;
  });
  try {
    await page.goto(`${base}/login`);
    await expect(page.getByRole('switch', { name: 'Share packets with RelayLens' })).not.toBeChecked();
    await page.getByTitle('Turn packet sharing on for RelayLens').click();
    await expect.poll(() => Boolean(pending)).toBe(true);
    await page.locator(`article[data-canonical-id="${second}"]`).click();
    await expect(page.getByRole('switch', { name: 'Share packets with Another site' })).toBeChecked();
    holdBReads = true;
    const response = page.waitForResponse(value => value.url().endsWith('/api/owner/packet-sharing') && value.request().method() === 'POST');
    await pending!.fulfill({ json: { nodeId: first, enabled: true, destinationIds: ['relaylens'] } });
    await (await response).finished(); await finishPublication(page);
    await expect(page.getByRole('switch', { name: 'Share packets with RelayLens' })).not.toBeChecked();
    await expect(page.getByRole('switch', { name: 'Share packets with Another site' })).toBeChecked();
    await page.getByTitle('Turn packet sharing on for RelayLens').click();
    await expect.poll(() => writes.find(write => write.nodeId === second)?.destinationIds).toEqual(['another-site', 'relaylens']);
    assert.equal(writes[0]?.nodeId, first, 'the accepted write remains a write to A');
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

browserTest('an old public-feed write cannot publish into a new login session', { timeout: 35_000 }, async () => {
  let actor: string | null = 'fixture-owner', oldWrite: Route | undefined, newReads = 0;
  const dashboard = { nodes: [node(first, 'First repeater'), node(second, 'Second repeater')] };
  const { browser, page, errors } = await setup(async route => { await route.fulfill({ json: live() }); }, async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/owner/csrf') { await route.fulfill({ json: { csrfToken: 'fixture-csrf' } }); return true; }
    if (url.pathname === '/api/owner/session') {
      await route.fulfill(actor ? { json: { ok: true, mqttUsername: actor, dashboard } } : { status: 401, json: { error: 'No session' } }); return true;
    }
    if (url.pathname === '/api/owner/logout') { actor = null; await route.fulfill({ json: { ok: true } }); return true; }
    if (url.pathname === '/api/owner/login') { actor = 'second-owner'; await route.fulfill({ json: { ok: true, mqttUsername: actor, dashboard } }); return true; }
    if (url.pathname !== '/api/owner/public-feed') return false;
    if (route.request().method() === 'POST') { oldWrite = route; return true; }
    if (actor === 'second-owner' && ++newReads > 1) return true;
    await route.fulfill({ json: publicFeedSettings() }); return true;
  });
  try {
    await page.goto(`${base}/login`);
    const control = page.getByRole('switch', { name: 'Publish packets and enable read-only public feed access' });
    await expect(control).not.toBeChecked();
    await page.getByTitle('Turn the public feed on', { exact: true }).click();
    await expect.poll(() => Boolean(oldWrite)).toBe(true);
    await page.getByRole('button', { name: 'Logout', exact: true }).click();
    await page.getByLabel('MQTT username', { exact: true }).fill('second-owner');
    await page.getByLabel('MQTT password', { exact: true }).fill('fixture-password');
    await page.getByRole('button', { name: 'Login', exact: true }).click();
    await expect(control).toBeEnabled(); await expect(control).not.toBeChecked();
    const response = page.waitForResponse(value => value.url().endsWith('/api/owner/public-feed') && value.request().method() === 'POST');
    await oldWrite!.fulfill({ json: publicFeedSettings(true) });
    await (await response).finished(); await finishPublication(page);
    await expect(control).not.toBeChecked();
    await expect(page.getByText('Public feed is off', { exact: true })).toBeVisible();
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

browserTest('a new comparison window hides old repeater series until every new series arrives', { timeout: 35_000 }, async () => {
  let pendingSecond: Route | undefined;
  const point = (name: string, samples: number) => ({ bucket: '2026-11-12T12:00:00Z', lastHopNodeId: peerId,
    lastHopName: name, resolution: 'direct', avgSnr: samples / 10, avgRssi: -90, sampleCount: samples });
  const { browser, page, errors } = await setup(async route => { await route.fulfill({ json: live() }); }, async route => {
    const url = new URL(route.request().url());
    if (url.pathname !== '/api/owner/live-last-hop') return false;
    const id = url.searchParams.get('nodeId'), next = url.searchParams.get('window') === '30d';
    if (next && id === second) { pendingSecond = route; return true; }
    await route.fulfill({ json: { points: [point(next ? 'New hop' : 'Old hop', next ? 30 : id === second ? 20 : 10)] } }); return true;
  });
  try {
    await page.goto(`${base}/login`);
    await expect(page.getByRole('button', { name: 'Old hop (10)', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Add Second repeater to the RX strength comparison', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Second repeater (20)', exact: true })).toBeVisible();
    await page.getByRole('group', { name: 'RX strength time window' }).getByRole('button', { name: '30d', exact: true }).click();
    await expect.poll(() => Boolean(pendingSecond)).toBe(true);
    await expect(page.getByRole('button', { name: 'New hop (30)', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Second repeater (20)', exact: true })).toHaveCount(0);
    await pendingSecond!.fulfill({ json: { points: [point('New hop', 40)] } });
    await expect(page.getByRole('button', { name: 'Second repeater (40)', exact: true })).toBeVisible();
    await expect(page.getByRole('img', { name: 'Comparing 2 repeaters: average SNR over the last 30 days', exact: true })).toBeVisible();
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

browserTest('a delayed CSRF lookup cannot submit an unmounted owner action under a later session', { timeout: 35_000 }, async () => {
  let actor = 'fixture-owner', heldCsrf: Route | undefined, csrfReads = 0;
  const writes: unknown[] = [];
  const { browser, page, errors } = await setup(async route => { await route.fulfill({ json: live() }); }, async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/owner/session') {
      await route.fulfill({ json: { ok: true, mqttUsername: actor,
        dashboard: { nodes: [node(first, actor === 'fixture-owner' ? 'First repeater' : 'New owner repeater')] } } }); return true;
    }
    if (url.pathname === '/api/owner/csrf') {
      if (++csrfReads === 1) { heldCsrf = route; return true; }
      await route.fulfill({ json: { csrfToken: 'fixture-csrf' } }); return true;
    }
    if (url.pathname !== '/api/owner/packet-sharing') return false;
    if (route.request().method() === 'POST') {
      writes.push(route.request().postDataJSON());
      await route.fulfill({ json: { nodeId: first, enabled: true, destinationIds: ['relaylens'] } }); return true;
    }
    await route.fulfill({ json: sharingSettings(first) }); return true;
  });
  try {
    await page.goto(`${base}/login`);
    await expect(page.getByRole('switch', { name: 'Share packets with RelayLens' })).not.toBeChecked();
    await page.getByTitle('Turn packet sharing on for RelayLens').click();
    await expect.poll(() => Boolean(heldCsrf)).toBe(true);
    await page.getByRole('link', { name: 'Privacy', exact: true }).click();
    await expect(page.getByRole('heading', { name: /Privacy/i }).first()).toBeVisible();
    actor = 'second-owner';
    await page.locator('a[href="/login"]').first().click();
    await expect(page.locator(`article[data-canonical-id="${first}"]`)).toContainText('New owner repeater');
    const response = page.waitForResponse(value => value.url().endsWith('/api/owner/csrf'));
    await heldCsrf!.fulfill({ json: { csrfToken: 'fixture-csrf' } });
    await (await response).finished();
    await page.waitForLoadState('networkidle');
    assert.deepEqual(writes, [], 'the old action was never accepted, so it must not be submitted as the later actor');
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

for (const target of ['packet-sharing', 'public-feed'] as const) {
  browserTest(`failed ${target} saves retain the draft through polling and confirm only after retry`, { timeout: 35_000 }, async () => {
    let writes = 0, reads = 0, confirmed = false;
    const { browser, page, errors } = await setup(async route => { await route.fulfill({ json: live() }); }, async route => {
      const path = new URL(route.request().url()).pathname;
      if (path === '/api/owner/csrf') { await route.fulfill({ json: { csrfToken: 'fixture-csrf' } }); return true; }
      if (path !== `/api/owner/${target}`) return false;
      if (route.request().method() === 'POST') {
        writes++;
        if (writes === 1) { await route.fulfill({ status: 503, json: { error: 'Save unavailable' } }); return true; }
        confirmed = true;
        await route.fulfill({ json: target === 'packet-sharing'
          ? { nodeId: first, enabled: true, destinationIds: ['relaylens'] }
          : publicFeedSettings(true) });
      } else {
        reads++;
        await route.fulfill({ json: target === 'packet-sharing' ? sharingSettings(first, confirmed ? ['relaylens'] : []) : publicFeedSettings(confirmed) });
      }
      return true;
    });
    try {
      await page.goto(`${base}/login`);
      const section = page.getByRole('region', { name: target === 'packet-sharing' ? 'Share packets' : 'Public feed', exact: true });
      const toggle = section.getByRole('switch').first();
      await expect(toggle).not.toBeChecked();
      await toggle.check();
      await expect(section.getByRole('alert')).toContainText('Your selection is kept');
      await expect(toggle).toBeChecked();
      await expect(section.getByText(target === 'packet-sharing' ? 'Packet sharing is off' : 'Public feed is off', { exact: true })).toBeVisible();
      const before = reads;
      await expect.poll(() => reads, { timeout: 12_000 }).toBeGreaterThan(before);
      await expect(toggle).toBeChecked();
      await expect(section.getByRole('alert')).toContainText('Your selection is kept');
      await section.getByRole('button', { name: 'Retry saving', exact: true }).click();
      await expect.poll(() => writes).toBe(2);
      await expect(section.getByRole('alert')).toHaveCount(0);
      await expect(section.getByText(target === 'packet-sharing' ? '1 site enabled' : 'Public feed is on', { exact: true })).toBeVisible();
      assert.deepEqual(errors, []);
    } finally { await browser.close(); }
  });
}
