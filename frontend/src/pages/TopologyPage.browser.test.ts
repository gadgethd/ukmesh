import assert from 'node:assert/strict';
import test from 'node:test';
import { chromium, expect } from '@playwright/test';

const browserTest = process.env['AUDITWAVE_BROWSER_TESTS'] === '1' ? test : test.skip;
const topology = {
  generatedAt: '2026-09-07T12:00:00Z', windowDays: 30, limited: false,
  summary: { nodes: 2, links: 1, observations: 12, connectedComponents: 1, likelyBridges: 0, isolatedNodes: 0 },
  nodes: [
    { nodeId: 'A'.repeat(64), name: 'Alpha', lat: 52, lon: -1, degree: 1, observations: 12 },
    { nodeId: 'B'.repeat(64), name: 'Bravo', lat: 52.1, lon: -1.1, degree: 1, observations: 12 },
  ],
  links: [{ source: 'A'.repeat(64), target: 'B'.repeat(64), observations: 12, strongObservations: 3, pathLossDb: 130, lastObserved: '2026-09-07T12:00:00Z' }],
  analysis: { connectedComponents: 1, bridgeNodeIds: [], isolatedNodeIds: [] },
};
const rf = {
  methodology: 'Observed relay evidence compared with stored ITM.',
  summary: { evaluated: 1, matches: 0, mismatches: 1, observedUnexpected: 1, operatorOverrides: 0, weakModelEvidence: 0 },
  mismatches: [{ source: 'A', target: 'B', sourceName: 'Alpha', targetName: 'Bravo',
    observations: 12, strongObservations: 3, pathLossDb: 130, classification: 'observed_unexpected' }],
};

for (const failure of ['http503', 'timeout'] as const) {
  browserTest(`deployed topology retains graph and RF panel after ${failure}, then retry restores results`, { timeout: 35_000 }, async () => {
    const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
    try {
      const context = await browser.newContext({ serviceWorkers: 'block' });
      const page = await context.newPage();
      const errors: string[] = [];
      page.on('pageerror', error => errors.push(error.message));
      let attempts = 0;
      await context.route('**/*', async route => {
        const url = new URL(route.request().url());
        // Only load assets from the authorized local website. All API data,
        // map tiles and telemetry are intercepted; this sends no external calls.
        if (url.hostname !== '127.0.0.1') { await route.abort(); return; }
        if (url.pathname === '/api/topology') { await route.fulfill({ json: topology }); return; }
        if (url.pathname === '/api/runtime-config') {
          await route.fulfill({ json: { version: 1, packetArcs: false, heatmap: false, privacyGeneration: 0, refreshAfterSeconds: 300 } });
          return;
        }
        if (url.pathname === '/api/rf-validation') {
          attempts += 1;
          if (attempts === 1) {
            if (failure === 'http503') await route.fulfill({ status: 503, json: { error: 'temporarily unavailable' } });
            // Leave the first request pending to exercise the real browser's
            // AbortSignal timeout, not an artificial immediate network error.
            return;
          }
          await route.fulfill({ json: rf });
          return;
        }
        if (url.pathname.startsWith('/api/')) { await route.fulfill({ status: 200, json: {} }); return; }
        await route.continue();
      });
      await page.goto('http://127.0.0.1:3004/topology', { waitUntil: 'domcontentloaded' });
      await expect(page.getByRole('heading', { name: 'Predicted versus observed RF' })).toBeVisible();
      await expect(page.locator('.topology-page__workspace')).toBeVisible();
      await expect(page.locator('.topology-page__workspace canvas')).toBeVisible();
      if (failure === 'timeout') await expect(page.getByText('Loading RF validation…')).toBeVisible();
      await expect(page.getByRole('alert').filter({ hasText: 'RF validation is temporarily unavailable' })).toBeVisible({ timeout: 18_000 });
      await expect(page.locator('.topology-page__workspace')).toBeVisible();
      await page.getByRole('button', { name: 'Retry RF validation' }).click();
      await expect(page.getByRole('table', { name: 'RF model mismatches' })).toBeVisible();
      await expect(page.getByRole('cell', { name: '130.0 dB', exact: true })).toBeVisible();
      assert.equal(attempts, 2);
      assert.deepEqual(errors, []);
      await context.close();
    } finally { await browser.close(); }
  });
}
