import { expect, test } from '@playwright/test';
import { fixtures, vectorRoutes } from './basemapFixtures.js';

test.beforeEach(async ({ page }) => fixtures(page));

test('production worker decodes tiles while old fixed worker and metadata URLs are stale', async ({ page }) => {
  const workerRequests: string[] = [];
  page.on('request', (request) => {
    if (request.url().includes('maplibre-gl-worker')) workerRequests.push(request.url());
  });
  await page.route('**/assets/maplibre-gl-worker.mjs', (route) => route.fulfill({
    contentType: 'application/javascript', body: 'throw new Error("stale cached worker")',
  }));
  await vectorRoutes(page, () => false);
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy', { timeout: 30_000 });
  expect(workerRequests.some((url) => /maplibre-gl-worker-[\w-]+\.js$/.test(url))).toBe(true);
  expect(workerRequests.some((url) => url.endsWith('/maplibre-gl-worker.mjs'))).toBe(false);
});

test('a style request that never completes still reaches the bundled outline', async ({ page }) => {
  await page.route('https://tiles.openfreemap.org/**', () => {});
  await page.route('https://tile.openstreetmap.org/**', (route) => route.abort());
  const outline = page.waitForResponse('**/basemap/uk-land.geojson', { timeout: 40_000 });
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'fallback', { timeout: 30_000 });
  expect((await outline).ok()).toBe(true);
  await expect(page.getByText('Using backup basemap')).toBeVisible();
});

test('failed tiles reach the backup map and a manual retry restores vector tiles', async ({ page }) => {
  let failTiles = true;
  await vectorRoutes(page, () => failTiles);
  const backupTile = page.waitForResponse('https://tile.openstreetmap.org/**', { timeout: 40_000 });
  await page.goto('/');
  await page.locator('.maplibregl-map').waitFor({ timeout: 20_000 });
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'fallback', { timeout: 30_000 });
  expect((await backupTile).ok()).toBe(true);
  failTiles = false;
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state', 'healthy', { timeout: 15_000 });
  await expect(page.getByText('Using backup basemap')).toHaveCount(0);
});
