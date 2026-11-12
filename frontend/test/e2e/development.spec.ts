import {expect,test} from '@playwright/test';
import {fixtures,vectorRoutes} from './basemapFixtures.js';
test('development renderer and worker load through queried TypeScript modules',async({page})=>{
 const errors:string[]=[];
 page.on('pageerror',e=>errors.push(e.message));
 page.on('console',m=>{if(m.type()==='error'&&/SyntaxError|TypeError|ReferenceError/.test(m.text()))errors.push(m.text());});
 await fixtures(page);await vectorRoutes(page,()=>false);
 await page.goto('/');
 await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state','healthy',{timeout:60000});
 expect(errors).toEqual([]);
});

test('an interrupted owner render cannot replace committed request callbacks or mutation ownership', async ({ page }) => {
  await fixtures(page); await vectorRoutes(page, () => false);
  const first = 'A'.repeat(64), second = 'B'.repeat(64);
  let csrf: import('@playwright/test').Route | undefined;
  let write: import('@playwright/test').Route | undefined;
  let selected: string[] = [];
  const reads: string[] = [];
  await page.route('**/api/owner/**', async route => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith('/csrf')) { csrf = route; return; }
    if (url.pathname.endsWith('/packet-sharing')) {
      if (route.request().method() === 'POST') { write = route; return; }
      const id = url.searchParams.get('nodeId') ?? first;
      reads.push(id);
      return route.fulfill({ json: { nodeId: id, featureEnabled: true, enabled: selected.length > 0,
        destinations: [{ id: 'relaylens', name: 'RelayLens', configured: true, selected: selected.includes('relaylens'),
          websiteUrl: null, description: null, lastForwardedAt: null }] } });
    }
    return route.fulfill({ json: {} });
  });
  const harness = '/test/e2e/ownerScopeHarness.tsx';
  await page.goto('/');
  await page.evaluate(async path => { const module = await import(/* @vite-ignore */ path); module.mount(); }, harness);
  try {
    await expect(page.getByTestId('fixture-owner')).toHaveText(first);
    await page.getByRole('button', { name: 'Enable fixture sharing' }).click();
    await expect.poll(() => Boolean(csrf)).toBe(true);
    await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).transitionToB(); }, harness);
    await expect.poll(() => page.evaluate(async path => (await import(/* @vite-ignore */ path)).bWasRendered(), harness)).toBe(true);
    await expect(page.getByTestId('fixture-owner')).toHaveText(first);
    await csrf!.fulfill({ json: { csrfToken: 'fixture-csrf' } });
    await expect.poll(() => Boolean(write)).toBe(true);
    expect(write!.request().postDataJSON().nodeId).toBe(first);
    selected = ['relaylens'];
    await write!.fulfill({ json: { nodeId: first, enabled: true, destinationIds: selected } });
    await expect(page.getByTestId('fixture-destinations')).toHaveText('relaylens');
    expect(reads).not.toContain(second);
    await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).discardB(); }, harness);
    await expect(page.getByTestId('fixture-owner')).toHaveText(first);
    await expect(page.getByTestId('fixture-destinations')).toHaveText('relaylens');
  } finally {
    if (!page.isClosed()) await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).unmount(); }, harness).catch(() => {});
  }
});

test('a scope commit closes its old socket before passive cleanup and rejects buffered callbacks', async ({ page }) => {
  await fixtures(page); await vectorRoutes(page, () => false);
  const harness = '/test/e2e/socketScopeHarness.tsx';
  await page.goto('/');
  await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).mount(); }, harness);
  try {
    await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).emitCurrent('first'); }, harness);
    await expect(page.getByTestId('fixture-socket-messages')).toHaveText('A:1:first');
    expect(await page.evaluate(async path => (await import(/* @vite-ignore */ path)).commitB(), harness)).toBe(true);
    await expect(page.getByTestId('fixture-socket-messages')).toHaveText('A:1:first');
    await expect.poll(() => page.evaluate(async path => (await import(/* @vite-ignore */ path)).snapshot().length, harness)).toBe(2);
    await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).emitCurrent('second'); }, harness);
    await expect(page.getByTestId('fixture-socket-messages')).toHaveText('A:1:first,B:2:second');
    await expect(page.getByTestId('fixture-socket-status')).toHaveText('connected');
    const sockets = await page.evaluate(async path => (await import(/* @vite-ignore */ path)).snapshot(), harness);
    expect(sockets.map((socket: { url: string }) => new URL(socket.url).searchParams.get('network'))).toEqual(['socket-fixture-A', 'socket-fixture-B']);
    expect(sockets[0].readyState).toBe(3);
  } finally {
    if (!page.isClosed()) await page.evaluate(async path => { (await import(/* @vite-ignore */ path)).unmount(); }, harness).catch(() => {});
  }
});
