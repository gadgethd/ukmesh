import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');
  });
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith('/packets/recent')) return route.fulfill({ json: [] });
    if (path.endsWith('/inferred-nodes')) {
      return route.fulfill({ json: { inferredNodes: [], inferredActiveNodeIds: [] } });
    }
    if (path.endsWith('/stats')) {
      return route.fulfill({
        json: { mqtt_nodes: 0, map_nodes: 0, total_nodes: 0, stale_nodes: 0, packets_24h: 0 },
      });
    }
    return route.fulfill({ json: {} });
  });
});

test('map modes update layers and produce a shareable URL', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText('Live Map', { exact: true })).toBeVisible();
  const mapArea = page.locator('.map-area');
  await expect(mapArea).toBeVisible();
  expect((await mapArea.boundingBox())?.height).toBeGreaterThan(300);
  const accessibility = await new AxeBuilder({ page }).exclude('.maplibregl-canvas').analyze();
  expect(accessibility.violations
    .filter((violation) => violation.impact === 'critical' || violation.impact === 'serious')
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);

  const diagnose = page.getByRole('button', { name: 'Diagnose' }).first();
  await diagnose.click();
  await expect(diagnose).toHaveAttribute('aria-pressed', 'true');
  await expect(page).toHaveURL(/mode=diagnose/);
  await expect(page).toHaveURL(/layers=.*paths/);
  await expect(page).toHaveURL(/layers=.*clashes/);

  await page.reload();
  await expect(page.getByRole('button', { name: 'Diagnose' }).first()).toHaveAttribute('aria-pressed', 'true');
});
