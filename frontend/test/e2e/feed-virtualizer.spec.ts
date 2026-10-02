import { expect, test, type Page } from '@playwright/test';

async function assertContiguous(page: Page) {
  const geometry = await page.locator('.uk-feed-packets-list').evaluate(container => {
    const rows = [...container.querySelectorAll('article')].map(row => row.getBoundingClientRect());
    const top = (container.firstElementChild as HTMLElement).getBoundingClientRect();
    const bottom = (container.lastElementChild as HTMLElement).getBoundingClientRect();
    return { count: rows.length, gaps: rows.slice(1).map((row, i) => row.top - rows[i]!.bottom),
      firstGap: rows[0]!.top - top.bottom, lastGap: bottom.top - rows.at(-1)!.bottom,
      heights: rows.map(row => row.height) };
  });
  expect(geometry.count).toBeGreaterThan(1);
  expect(geometry.count).toBeLessThan(50);
  for (const gap of [...geometry.gaps, geometry.firstGap, geometry.lastGap]) expect(Math.abs(gap)).toBeLessThan(1);
  // At wider breakpoints one additional metadata line is about 11.7px.
  expect(Math.max(...geometry.heights) - Math.min(...geometry.heights)).toBeGreaterThan(10);
}

async function anchor(page: Page) {
  return page.locator('.uk-feed-packets-list').evaluate(container => {
    const top = container.scrollHeight > container.clientHeight + 1 ? container.getBoundingClientRect().top : 0;
    const row = [...container.querySelectorAll('article')].find(element => element.getBoundingClientRect().bottom > top + 1)!;
    return { label: row.getAttribute('aria-label'), offset: row.getBoundingClientRect().top - top };
  });
}

async function assertAnchor(page: Page, expected: Awaited<ReturnType<typeof anchor>>) {
  // Browser scroll positions round fractional border-box offsets to pixels.
  await expect.poll(async () => {
    const current = await anchor(page);
    return current.label === expected.label && Math.abs(current.offset - expected.offset) < 1;
  }).toBe(true);
}

async function scrollList(page: Page, position: number) {
  await page.locator('.uk-feed-packets-list').evaluate((element, target) => {
    if (element.scrollHeight > element.clientHeight + 1) element.scrollTop = target;
    else window.scrollTo(0, window.scrollY + element.getBoundingClientRect().top + target);
  }, position);
}

for (const width of [390, 1280]) {
  test(`measured feed rows survive wrapping, enrichment, prepends and resize at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 800 });
    const errors: string[] = [];
    page.on('pageerror', error => {
      errors.push(error.message);
      console.error('[feed-virtualizer] page error:', error.message);
    });
    await page.route('**/api/**', route => {
      const url = route.request().url();
      return route.fulfill({ json: url.includes('runtime-config') ? { version: 1, packetArcs: false, heatmap: false, privacyGeneration: 1, refreshAfterSeconds: 30 }
        : url.includes('stats') ? { observerRegions: [] } : [] });
    });
    await page.addInitScript(() => {
      localStorage.setItem('meshcore-cookie-consent-v1', '1');
      const packets = Array.from({ length: 50 }, (_, index) => ({
        packet_hash: index.toString(16).toUpperCase().padStart(64, 'A'),
        time: new Date(Date.now() - index * 1000).toISOString(),
        network: 'ukmesh', packet_type: 5, hop_count: index % 8,
        observer_iatas: index % 3 === 0 ? Array.from({ length: 10 }, (_, i) => `REGION${i}`) : ['ABC'],
        summary: `fixture ${index} ${'a wrapped message '.repeat(index % 3 === 0 ? 20 : 1)}`,
      }));
      class FixtureWebSocket extends EventTarget {
        static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
        readyState = 0;
        onopen: ((event: Event) => void) | null = null;
        onmessage: ((event: MessageEvent) => void) | null = null;
        send() {}
        close() { this.readyState = 3; }
        constructor() {
          super();
          let arrivals = 0;
          const publish = () => this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({
            type: 'initial_state', data: { nodes: [], packets }, ts: Date.now(),
          }) }));
          Object.assign(window, {
            enrichFeed: () => {
              // Late observer enrichment changes the natural row height.
              for (const packet of packets) packet.observer_iatas.push('EXTRA1', 'EXTRA2');
              publish();
            },
            prependFeed: () => {
              arrivals++;
              packets.unshift({ ...packets[1]!, packet_hash: arrivals.toString(16).toUpperCase().padStart(64, 'F'),
                time: new Date(Date.now() + arrivals * 1000).toISOString(), summary: `new arrival ${arrivals}` });
              packets.pop();
              publish();
            },
          });
          setTimeout(() => { this.readyState = 1; this.onopen?.(new Event('open')); publish(); }, 0);
        }
      }
      Object.defineProperty(window, 'WebSocket', { value: FixtureWebSocket, configurable: true });
    });
    await page.goto('/feed');
    const list = page.locator('.uk-feed-packets-list');
    // Initial code loading and runtime-config bootstrap can be cold in the
    // full matrix. Keep the geometry checks on their normal assertion budget.
    await expect(list.locator('article').first()).toBeVisible({ timeout: 15_000 });
    await assertContiguous(page);
    await scrollList(page, 1700);
    await expect.poll(() => list.evaluate(element => element.scrollHeight > element.clientHeight + 1
      ? element.scrollTop : -element.getBoundingClientRect().top)).toBeGreaterThan(1600);
    const beforeEnrichment = await anchor(page);
    await page.evaluate(() => (window as unknown as { enrichFeed(): void }).enrichFeed());
    await expect(list.locator('.uk-feed-packet-row__observer').first()).toContainText('EXTRA1');
    await assertAnchor(page, beforeEnrichment);
    await assertContiguous(page);
    const beforePrepend = await anchor(page);
    await page.evaluate(() => (window as unknown as { prependFeed(): void }).prependFeed());
    await assertAnchor(page, beforePrepend);
    // Filtering a desktop list until it no longer overflows must reset its
    // own scroll position without scrolling the page past the search controls.
    if (width === 1280) {
      const pageScroll = await page.evaluate(() => window.scrollY);
      await page.getByPlaceholder(/search/i).fill('new arrival');
      await expect(list.locator('article')).toHaveCount(1);
      await expect.poll(() => list.evaluate(element => element.scrollTop)).toBe(0);
      await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(pageScroll);
      await page.getByPlaceholder(/search/i).fill('');
      await expect(list.locator('article').first()).toBeVisible();
      await scrollList(page, 1700);
    }
    const beforeModeSwitch = await anchor(page);
    await page.setViewportSize({ width: width === 390 ? 1280 : 390, height: 650 });
    await assertContiguous(page);
    await expect.poll(async () => (await anchor(page)).label).toBe(beforeModeSwitch.label);
    await page.setViewportSize({ width: width === 390 ? 700 : 850, height: 650 });
    await assertContiguous(page);
    await scrollList(page, 100_000);
    await expect.poll(() => list.locator('article').last().getAttribute('aria-label')).toContain('A30');
    await expect.poll(() => list.locator('article').last().evaluate(element => element.getBoundingClientRect().bottom)).toBeLessThan(651);
    await page.getByPlaceholder(/search/i).fill('new arrival');
    await expect(list.locator('article')).toHaveCount(1);
    await expect.poll(() => list.evaluate(element => element.scrollTop)).toBe(0);
    // Empty filters must clear spacers, and restoring traffic must remount
    // measured rows correctly in both internal and document scrolling modes.
    await page.getByPlaceholder(/search/i).fill('no fixture matches this query');
    await expect(list.locator('article')).toHaveCount(0);
    await expect(list.locator('.dev-status-empty')).toBeVisible();
    await page.getByPlaceholder(/search/i).fill('');
    await expect(list.locator('article').first()).toBeVisible();
    await assertContiguous(page);
    await scrollList(page, 0);
    await page.evaluate(() => (window as unknown as { prependFeed(): void }).prependFeed());
    await expect(list.locator('article').first()).toHaveAttribute('aria-label', `Open packet ${'F'.repeat(63)}2`);
    await expect(list.locator('article').first()).toBeVisible();
    await expect.poll(() => list.evaluate(element => element.scrollHeight > element.clientHeight + 1
      ? element.scrollTop : -element.getBoundingClientRect().top)).toBeLessThan(1);
    expect(errors).toEqual([]);
  });
}
