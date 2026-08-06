import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

for (const width of [375, 768]) {
  test(`navigation collapses and can be toggled at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 667 });
    await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
    await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

    await page.goto('/feed');

    const menu = page.locator('#site-navigation');
    const toggle = page.locator('button[aria-controls="site-navigation"]');
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-label', 'Open menu');
    await expect(menu).toBeHidden();

    await toggle.click();
    await expect(menu).toBeVisible();
    await expect(toggle).toHaveAttribute('aria-expanded', 'true');
    await expect(toggle).toHaveAttribute('aria-label', 'Close menu');

    await page.keyboard.press('Escape');
    await expect(menu).toBeHidden();
    await expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await expect(toggle).toHaveAttribute('aria-label', 'Open menu');
  });
}

test('phone layouts give primary content the full available width', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 667 });
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

  await page.goto('/');
  const homeGrid = page.locator('.site-home__grid');
  const introBox = await page.locator('.site-home__intro').boundingBox();
  const panelBox = await page.locator('.site-home__panel').boundingBox();
  expect(introBox?.width ?? 0).toBeGreaterThan(330);
  expect(panelBox?.width ?? 0).toBeGreaterThan(330);
  expect(await homeGrid.evaluate((element) => getComputedStyle(element).gridTemplateColumns.split(' ').length)).toBe(1);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBe(375);

  await page.goto('/feed');
  const channelsBox = await page.locator('.uk-feed-channels').boundingBox();
  const chatBox = await page.locator('.uk-feed-chat').boundingBox();
  expect(channelsBox?.height ?? 200).toBeLessThan(64);
  expect(chatBox?.width ?? 0).toBeGreaterThan(360);
  await expect(page.locator('.uk-feed-right')).toBeHidden();
});

test('tablet topology uses the full-width graph workspace', async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 667 });
  await page.route('**/api/topology**', (route) => route.fulfill({
    json: {
      generatedAt: '2026-07-19T12:00:00Z',
      windowDays: 30,
      limited: false,
      summary: { nodes: 0, links: 0, observations: 0, connectedComponents: 0, likelyBridges: 0, isolatedNodes: 0 },
      analysis: { connectedComponents: 0, bridgeNodeIds: [], isolatedNodeIds: [] },
      nodes: [],
      links: [],
    },
  }));

  await page.goto('/topology');
  const workspace = page.locator('.topology-page__workspace');
  expect(await workspace.evaluate((element) => getComputedStyle(element).gridTemplateColumns.split(' ').length)).toBe(1);
  expect((await page.locator('.topology-page__graph').boundingBox())?.width ?? 0).toBeGreaterThan(700);
});

test('public site exposes its primary journeys', async ({ page }) => {
  await page.route('**/api/stats**', async (route) => {
    await route.fulfill({
      json: {
        totalNodes: 120,
        packetsDay: 42_000,
        internationalNodes: 2,
        internationalLastSeen: '2026-07-11T11:55:00Z',
        internationalLastCountry: 'France',
      },
    });
  });
  await page.route('**/api/topology**', async (route) => {
    await route.fulfill({
      json: {
        generatedAt: '2026-07-11T12:00:00Z', windowDays: 30, limited: false,
        summary: { nodes: 3, links: 2, observations: 25, connectedComponents: 1, likelyBridges: 0, isolatedNodes: 0 },
        analysis: { connectedComponents: 1, bridgeNodeIds: [], isolatedNodeIds: [] },
        nodes: [
          { nodeId: 'A'.repeat(64), name: 'Alpha', lat: 52, lon: -1, degree: 1, observations: 20 },
          { nodeId: 'B'.repeat(64), name: 'Bravo', lat: 53, lon: -2, degree: 1, observations: 20 },
          { nodeId: 'C'.repeat(64), name: 'No location', lat: null, lon: null, degree: 1, observations: 5 },
        ],
        links: [
          {
            source: 'A'.repeat(64), target: 'B'.repeat(64), observations: 20,
            strongObservations: 5, pathLossDb: 110, lastObserved: '2026-07-11T11:00:00Z',
          },
          {
            source: 'A'.repeat(64), target: 'C'.repeat(64), observations: 5,
            strongObservations: 1, pathLossDb: 120, lastObserved: '2026-07-11T10:00:00Z',
          },
        ],
      },
    });
  });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'UK Mesh Network' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Open live map' }).first()).toBeVisible();
  await expect(page.getByText(/regions need attention/i)).toHaveCount(0);
  await expect(page.getByText(/support the creators of meshcore/i)).toHaveCount(0);
  await expect(page.getByRole('link', { name: /donate/i })).toHaveCount(0);
  const homeAccessibility = await new AxeBuilder({ page }).analyze();
  expect(homeAccessibility.violations
    .filter((violation) => violation.impact === 'critical' || violation.impact === 'serious')
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);

  await page.getByRole('link', { name: 'Install MeshCore' }).first().click();
  await expect(page).toHaveURL(/\/install$/);
  await expect(page.getByRole('heading', { level: 1 })).toBeVisible();

  await page.getByRole('link', { name: 'Topology' }).click();
  await expect(page).toHaveURL(/\/topology$/);
  await expect(page.getByRole('heading', { name: 'Repeater topology' })).toBeVisible();
  await expect(page.getByLabel('Geographic repeater topology graph')).toBeVisible();
  await expect(page.getByRole('group', { name: '2 positioned repeaters and 1 links' })).toBeVisible();

  await expect(page.getByRole('link', { name: 'Health', exact: true })).toHaveCount(0);
});

// Guards issue #3 (contrast): the feed chrome renders the muted/secondary text
// tokens (--text-muted / --text-secondary) that the repeater-tree window reuses,
// so a colour-contrast-scoped scan here catches any regression in those tokens.
test('feed page text meets WCAG AA colour contrast', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('meshcore-cookie-consent-v1', '1'));
  await page.route('**/api/**', (route) => route.fulfill({ json: {} }));

  await page.goto('/feed');
  await expect(page.locator('.uk-feed-channels')).toBeVisible();

  const contrast = await new AxeBuilder({ page })
    .withRules(['color-contrast'])
    .exclude('.maplibregl-canvas')
    .analyze();
  expect(contrast.violations
    .map((violation) => ({ id: violation.id, targets: violation.nodes.map((node) => node.target.join(' ')) }))).toEqual([]);
});
