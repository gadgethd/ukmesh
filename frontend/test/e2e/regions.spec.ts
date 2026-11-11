import { expect, test, type Page } from '@playwright/test';
import { installMapRoutes } from './mapFixtures.js';

test.setTimeout(60_000);

async function installRegionFixture(page: Page, enabled: boolean, delayEng = false) {
  await installMapRoutes(page);
  await page.addInitScript(() => {
    localStorage.setItem('meshcore-disclaimer-dismissed', '1');
    const fixture = window as typeof window & { regionSockets: FixtureWebSocket[] };
    fixture.regionSockets = [];
    const NativeWebSocket = window.WebSocket;
    class FixtureWebSocket extends EventTarget {
      static readonly CONNECTING = 0; static readonly OPEN = 1;
      static readonly CLOSING = 2; static readonly CLOSED = 3;
      readyState = FixtureWebSocket.CONNECTING;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent<string>) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      readonly url: string;
      constructor(url: string | URL, protocols?: string | string[]) {
        super();
        if (new URL(url).pathname !== '/ws') {
          return new NativeWebSocket(url, protocols) as unknown as FixtureWebSocket;
        } this.url = String(url); fixture.regionSockets.push(this);
        const region = new URL(url).searchParams.get('region');
        const names = region ? [region] : ['eng-ne', 'Europe'];
        window.setTimeout(() => {
          if (this.readyState === FixtureWebSocket.CLOSED) return;
          this.readyState = FixtureWebSocket.OPEN;
          this.onopen?.(new Event('open'));
          this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({
            type: 'initial_state', ts: Date.now(), data: {
              nodes: names.map((name) => ({
                node_id: (name === 'Europe' ? 'B' : 'A').repeat(64), name,
                role: 2, lat: 54, lon: -1, last_seen: new Date().toISOString(), is_online: true,
              })),
              packets: names.map((name) => ({
                packet_hash: name, packet_type: 5, summary: `${name} message`, time: new Date().toISOString(),
              })), viable_links: [],
            },
          }) }));
        }, 0);
      }
      send() {} close() { this.readyState = FixtureWebSocket.CLOSED; }
    }
    Object.defineProperty(window, 'WebSocket', { value: FixtureWebSocket, configurable: true });
  });
  let releaseEng = () => {};
  const held = new Promise<void>((resolve) => { releaseEng = resolve; });
  const requested: URL[] = [];
  const crashes: unknown[] = [];
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url());
    requested.push(url);
    if (url.pathname.endsWith('/telemetry/frontend-error')) {
      const crash = route.request().postDataJSON();
      crashes.push(crash);
      console.log('Browser telemetry:', JSON.stringify(crash));
      return route.fulfill({ json: { ok: true } });
    }
    if (url.pathname.endsWith('/runtime-config')) {
      return route.fulfill({ json: { version: 1, packetArcs: false, heatmap: false,
        regionScoping: enabled, privacyGeneration: 1, refreshAfterSeconds: 30 } });
    }
    if (url.pathname.endsWith('/regions/summary')) {
      return route.fulfill({ json: { regions: [{ name: 'eng-ne' }, { name: 'Europe' }] } });
    }
    if (url.pathname.endsWith('/packets/recent')) {
      const region = url.searchParams.get('region');
      if (delayEng && region === 'eng-ne') await held;
      return route.fulfill({ json: [{ packet_hash: region ?? 'all', packet_type: 5,
        summary: `${region ?? 'all'} poll`, time: new Date().toISOString() }] }).catch(() => {});
    }
    if (url.pathname.endsWith('/stats')) return route.fulfill({ json: {
      mqttNodes: 2, mapNodes: 2, totalNodes: 2, staleNodes: 0, packetsDay: 2,
    } });
    return route.fulfill({ json: {} });
  });
  return { requested, crashes, releaseEng, disable: () => { enabled = false; } };
}

async function snapshot(page: Page) {
  return page.evaluate(async () => {
    const moduleUrl = '/src/hooks/useNodes.ts';
    const { nodeStore } = await import(/* @vite-ignore */ moduleUrl);
    const state = nodeStore.getState();
    return { names: [...state.nodes.values()].map((node: { name: string }) => node.name).sort(),
      hashes: state.packets.map((packet: { packetHash: string }) => packet.packetHash), scope: state.scopeKey };
  });
}

test('default-off ignores region hashes and preserves global socket/poll requests', async ({ page }, testInfo) => {
  const fixture = await installRegionFixture(page, false);
  await page.goto('/#region=eng-ne');
  await page.locator('.app-shell').waitFor({ timeout: 30_000 });
  if (testInfo.project.name.includes('mobile')) {
    await page.getByRole('button', { name: 'Layers' }).first().click();
  }
  const search = page.getByRole('combobox', { name: 'Search map nodes' });
  await search.fill('Europe');
  await expect(page.getByRole('option', { name: /Europe/ })).toBeVisible();
  await search.fill('eng-ne');
  await expect(page.getByRole('option', { name: /eng-ne/ })).toBeVisible();
  await expect(page.getByRole('combobox', { name: 'Region', exact: true })).toHaveCount(0);
  expect(fixture.requested.some((url) => url.pathname.endsWith('/regions/summary'))).toBe(false);
  expect(fixture.requested.some((url) => url.searchParams.has('region'))).toBe(false);
  expect(await page.evaluate(() => (window as unknown as { regionSockets: { url: string }[] }).regionSockets.every((socket) => !new URL(socket.url).searchParams.has('region')))).toBe(true);
  expect(fixture.crashes).toEqual([]);
});

test('region selection round-trips reload, browser history and All regions', async ({ page }, testInfo) => {
  const fixture = await installRegionFixture(page, true);
  await page.goto('/?layers=feed#note=keep&region=eng-ne');
  const select = page.getByRole('combobox', { name: 'Region', exact: true });
  await expect(select).toHaveValue('eng-ne', { timeout: 30_000 });
  await expect.poll(async () => (await snapshot(page)).names, { timeout: 15_000 }).toEqual(['eng-ne']);
  await expect.poll(() => fixture.requested.some((url) => url.pathname.endsWith('/packets/recent') && url.searchParams.get('region') === 'eng-ne')).toBe(true);
  await select.selectOption('Europe');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe']);
  await expect(page).toHaveURL(/#note=keep&region=Europe$/);
  await page.reload();
  await page.locator('.app-shell').waitFor({ timeout: 30_000 });
  await expect(select).toHaveValue('Europe');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe']);
  await select.selectOption('');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe', 'eng-ne']);
  await expect(page).toHaveURL(/#note=keep$/);
  await page.goBack();
  await expect(select).toHaveValue('Europe');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe']);
  await page.goForward();
  await expect(select).toHaveValue('');
  await select.selectOption('Europe');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe']);
  await expect(select).toBeVisible();
  expect(fixture.crashes).toEqual([]);
  const bounds = await select.boundingBox();
  expect(bounds!.x).toBeGreaterThanOrEqual(0);
  expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(page.viewportSize()!.width);
  await page.screenshot({ path: `../.ukmesh-tools/regions-${testInfo.project.name}.png` });
});

test('late old-region responses cannot repopulate the new scope and runtime disable restores default', async ({ page }) => {
  const fixture = await installRegionFixture(page, true, true);
  await page.goto('/#region=eng-ne');
  await page.locator('.app-shell').waitFor({ timeout: 30_000 });
  const select = page.getByRole('combobox', { name: 'Region', exact: true });
  await expect.poll(async () => (await snapshot(page)).names, { timeout: 15_000 }).toEqual(['eng-ne']);
  await expect.poll(() => fixture.requested.some((url) => url.searchParams.get('region') === 'eng-ne')).toBe(true);
  await page.evaluate(async () => {
    const nodesUrl = '/src/hooks/useNodes.ts', overlayUrl = '/src/store/overlayStore.ts';
    const [{ nodeStore }, { useOverlayStore }] = await Promise.all([import(/* @vite-ignore */ nodesUrl), import(/* @vite-ignore */ overlayUrl)]);
    useOverlayStore.getState().togglePinnedPacket(nodeStore.getState().packets[0]);
  });
  await select.selectOption('Europe');
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe']);
  fixture.releaseEng();
  await page.evaluate(() => {
    const sockets = (window as unknown as { regionSockets: WebSocket[] }).regionSockets;
    const old = sockets.find((socket) => new URL(socket.url).searchParams.get('region') === 'eng-ne')!;
    old.onmessage?.(new MessageEvent('message', { data: JSON.stringify({
      type: 'node_upsert', data: { node_id: 'C'.repeat(64), name: 'Stale region' }, ts: Date.now(),
    }) }));
  });
  await expect.poll(async () => (await snapshot(page)).hashes.every((hash: string) => !hash.startsWith('ENG-NE'))).toBe(true);
  expect((await snapshot(page)).names).toEqual(['Europe']);
  expect(await page.evaluate(async () => {
    const moduleUrl = '/src/store/overlayStore.ts';
    const { useOverlayStore } = await import(/* @vite-ignore */ moduleUrl);
    return useOverlayStore.getState().pinnedPacketId;
  })).toBeNull();
  fixture.disable();
  await page.evaluate(async () => {
    const moduleUrl = '/src/config/runtimeFeatures.ts';
    const { refreshRuntimeFeatures } = await import(/* @vite-ignore */ moduleUrl);
    await refreshRuntimeFeatures();
  });
  await expect(select).toHaveCount(0);
  await expect.poll(async () => (await snapshot(page)).names).toEqual(['Europe', 'eng-ne']);
  expect((await snapshot(page)).scope).not.toContain('region-');
  expect(fixture.crashes).toEqual([]);
});
