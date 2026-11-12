import { browserLane, browserLaunchOptions } from '../../test/browserLane.js';
import assert from 'node:assert/strict';
import { chromium, expect, type Page } from '@playwright/test';
import { mapHandle } from '../../test/browserMapHandle.js';

// Run against an isolated local website; never change the deployed site.
const { browserTest, origin } = browserLane('REGIONS_BROWSER_TESTS', 'REGIONS_TEST_ORIGIN', 'http://127.0.0.1:4204');
const style = {
  version: 8 as const, sources: {},
  layers: [{ id: 'background', type: 'background' as const, paint: { 'background-color': '#182838' } }],
};
const members = [
  { nodeId: 'A', name: 'Alpha', role: 2, lat: 54.4, lon: -2.5, packets: 40 },
  { nodeId: 'B', name: 'Bravo', role: 2, lat: 54.8, lon: -2.1, packets: 40 },
  { nodeId: 'C', name: 'Charlie', role: 2, lat: 54.4, lon: -2.1, packets: 40 },
];

async function mockApi(page: Page): Promise<void> {
  await page.route('**/api/**', async route => {
    const path = new URL(route.request().url()).pathname;
    const json = path === '/api/regions/summary'
      ? { windowDays: 92, regions: [{ name: 'northwest', packetsTotal: 100, memberCount: 3,
          repeaterMemberCount: 3, confidence: 'confirmed', firstSeen: null, lastSeen: null, members }] }
      : path === '/api/regions/repeaters'
        ? { repeaters: members.map((member, index) => ({ ...member,
            status: index === 0 ? 'red' : 'green', scopedPackets: index === 0 ? 0 : 40,
            unscopedPackets7d: 10, regions: { northwest: 40 }, evidence: 'scoped' })) }
        : path === '/api/regions/unattributed'
          ? { generatedAt: null, windowDays: 92, clusters: [], totals: { clusters: 0, packets: 0, nodes: 0 } }
        : path === '/api/runtime-config'
          ? { version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 300 }
          : {};
    await route.fulfill({ json });
  });
}


browserTest('regions survive repeated style loads with current filters and visibility', { timeout: 120_000 }, async () => {
  const browser = await chromium.launch(browserLaunchOptions());
  try {
    const page = await browser.newPage({ serviceWorkers: 'block' });
    const errors: string[] = [];
    page.on('pageerror', error => errors.push(error.message));
    await mockApi(page);
    await page.route('https://tiles.openfreemap.org/**', route => route.fulfill({ json: style }));
    await page.goto(`${origin}/regions`);
    await page.locator('.regions-page__map canvas').waitFor({ timeout: 60_000 });
    const map = await mapHandle(page);
    await expect.poll(() => map.evaluate(map => Boolean(map.getLayer('region-repeater'))
      && map.queryRenderedFeatures({ layers: ['region-repeater'] }).length > 0), { timeout: 30_000 }).toBe(true);
    await page.locator('.regions-page__controls select').nth(1).selectOption('green');
    await page.getByLabel('Region shapes', { exact: true }).uncheck();
    for (let attempt = 0; attempt < 2; attempt += 1) {
      await map.evaluate((map, style) => { map.setStyle(style, { diff: false }); }, style);
      await expect.poll(() => map.evaluate(map => Boolean(map.getLayer('region-repeater'))), { timeout: 15_000 }).toBe(true);
      await expect.poll(() => map.evaluate(map => map.queryRenderedFeatures({ layers: ['region-repeater'] })
        .map(feature => feature.properties['status']))).toEqual(['green', 'green']);
      assert.equal(await map.evaluate(map => map.getLayoutProperty('region-areas-fill', 'visibility')), 'none');
      assert.equal(await page.locator('.maplibregl-ctrl-zoom-in').count(), 1);
    }
    await page.getByLabel('Region shapes', { exact: true }).check();
    await expect.poll(() => map.evaluate(map => map.queryRenderedFeatures({ layers: ['region-areas-fill'] }).length)).toBeGreaterThan(0);
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

browserTest('a stalled sprite shows a visible failure and retry restores the map', { timeout: 120_000 }, async () => {
  const browser = await chromium.launch(browserLaunchOptions());
  try {
    const page = await browser.newPage({ serviceWorkers: 'block' });
    await mockApi(page);
    let stalled = true;
    await page.route('https://tiles.openfreemap.org/**', async route => {
      const path = new URL(route.request().url()).pathname;
      if (path.endsWith('.png')) {
        if (stalled) return; // Real pending image request, not a synthetic error event.
        await route.fulfill({ contentType: 'image/png', body: Buffer.from(
          'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=', 'base64') });
      } else {
        await route.fulfill({ json: path.endsWith('.json') ? {} : { ...style, sprite: 'https://tiles.openfreemap.org/test/sprite' } });
      }
    });
    await page.goto(`${origin}/regions`);
    await page.locator('.regions-page__map canvas').waitFor({ timeout: 60_000 });
    await expect(page.locator('.regions-page__map').getByRole('alert')).toBeVisible({ timeout: 20_000 });
    stalled = false;
    await page.getByRole('button', { name: 'Retry map' }).click();
    await expect(page.locator('.regions-page__map').getByRole('alert')).toHaveCount(0);
    await expect.poll(async () => {
      const map = await mapHandle(page);
      try { return await map.evaluate(map => Boolean(map.getLayer('region-repeater')) && map.loaded()); }
      finally { await map.dispose(); }
    }, { timeout: 30_000 }).toBe(true);
  } finally { await browser.close(); }
});

for (const failure of ['style-http-error', 'webgl-unavailable'] as const) {
  browserTest(`${failure} is visible and retry can recover`, { timeout: 120_000 }, async () => {
    const browser = await chromium.launch(browserLaunchOptions());
    try {
      const page = await browser.newPage({ serviceWorkers: 'block' });
      await mockApi(page);
      let failed = true;
      await page.route('https://tiles.openfreemap.org/**', route => route.fulfill(
        failure === 'style-http-error' && failed ? { status: 503, body: 'Unavailable' } : { json: style },
      ));
      // Raw script keeps tsx's function-name helper out of the browser realm.
      if (failure === 'webgl-unavailable') await page.addInitScript({ content: `(() => {
        const originalGetContext = HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext = function (...args) {
          if (args[0] === 'webgl2') return null;
          return originalGetContext.apply(this, args);
        };
        window.restoreWebGL = () => { HTMLCanvasElement.prototype.getContext = originalGetContext; };
      })();` });
      await page.goto(`${origin}/regions`);
      await expect(page.locator('.regions-page__map').getByRole('alert')).toBeVisible({ timeout: 60_000 });
      failed = false;
      if (failure === 'webgl-unavailable') await page.evaluate(() => {
        (window as unknown as { restoreWebGL: () => void }).restoreWebGL();
      });
      await page.getByRole('button', { name: 'Retry map' }).click();
      await expect(page.locator('.regions-page__map').getByRole('alert')).toHaveCount(0);
      await expect.poll(async () => {
        const map = await mapHandle(page);
        try { return await map.evaluate(map => Boolean(map.getLayer('region-repeater')) && map.loaded()); }
        finally { await map.dispose(); }
      }, { timeout: 30_000 }).toBe(true);
    } finally { await browser.close(); }
  });
}


browserTest('regions wait for privacy authority and clear the map when its refresh fails', { timeout: 30_000 }, async () => {
  const browser = await chromium.launch(browserLaunchOptions());
  try {
    const page = await browser.newPage({ serviceWorkers: 'block' });
    await mockApi(page);
    await page.route('https://tiles.openfreemap.org/**', route => route.fulfill({ json: style }));
    let authoritative = false;
    let unavailable = false;
    let summaryReads = 0;
    await page.route('**/api/runtime-config', async route => {
      return route.fulfill(unavailable ? { status: 503, json: { error: 'Unavailable' } } : { json: {
        version: 1, packetArcs: false, heatmap: false, privacyGeneration: authoritative ? 1 : 0, refreshAfterSeconds: 5,
      } });
    });
    await page.route('**/api/regions/summary**', route => { summaryReads++; return route.fallback(); });
    await page.goto(`${origin}/regions`);
    await expect(page.getByText('Loading region scoping…')).toBeVisible();
    assert.equal(summaryReads, 0);
    authoritative = true;
    await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
    // The server requested five-second authority polls; a visible-to-visible
    // event does not shorten that cadence. Allow its next poll and map startup.
    await expect(page.locator('.regions-page__map canvas')).toBeVisible({ timeout: 12_000 });
    assert.equal(summaryReads, 1);
    unavailable = true;
    await expect(page.locator('.regions-page__map canvas')).toHaveCount(0, { timeout: 10_000 });
    assert.equal(summaryReads, 1, 'unavailable policy never starts another public snapshot read');
  } finally { await browser.close(); }
});
