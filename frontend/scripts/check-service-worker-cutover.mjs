/** Native Chromium cutover rehearsal on a private loopback origin and profile. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { chromium } from '@playwright/test';
import { SW_CACHE_POLICY as policy } from '../public/sw-runtime.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const args = process.argv.slice(2);
const option = name => { const index = args.indexOf(name); return index < 0 ? undefined : args[index + 1]; };
const previousVersion = policy.previousAppCache.match(/-(v\d+)$/)?.[1];
assert.ok(previousVersion);
const currentSource = await readFile(path.join(root, 'public/sw-runtime.js'), 'utf8');
const wrapper = await readFile(path.join(root, 'public/sw.js'), 'utf8');
const previousPath = option('--previous-runtime');
// CI exercises the protocol with the previous namespace. An archived runtime
// can be supplied for a rehearsal against its exact previous implementation.
const previousPolicy = { ...policy, version: previousVersion };
for (const [current, previous] of [['tileCache','previousTileCache'], ['appCache','previousAppCache'], ['metadataCache','previousMetadataCache'], ['basemapMetadataCache','previousBasemapMetadataCache']]) {
  previousPolicy[current] = policy[previous];
  previousPolicy[previous] = policy[previous].replace(/v\d+$/, `v${Number(previousVersion.slice(1)) - 1}`);
}
const previousSource = previousPath ? await readFile(previousPath, 'utf8')
  : currentSource.replace(/Object\.freeze\(\{[\s\S]*?\n\}\);/, `Object.freeze(${JSON.stringify(previousPolicy)});`);
assert.ok(previousSource.includes(`'${previousVersion}'`) || previousSource.includes(`"${previousVersion}"`));
let phase = previousVersion, removeOldAssets = false, disconnectNetwork = false;
const currentVersion = policy.version;
const asset = (kind, version) => `/assets/${kind}-${version}-${kind === 'entry' ? 'a1b2c3' : 'd4e5f6'}.js`;
const requests = [];
const server = http.createServer((request, response) => {
  const pathname = new URL(request.url, 'http://fixture.invalid').pathname;
  requests.push({ path: pathname, phase, disconnected: disconnectNetwork });
  if (disconnectNetwork) { request.socket.destroy(); return; }
  response.setHeader('Cache-Control', pathname.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache, no-store, must-revalidate');
  response.setHeader('Content-Type', 'application/javascript');
  if (pathname === '/sw.js') return response.end(wrapper);
  if (pathname === '/sw-runtime.js') return response.end(phase === currentVersion ? currentSource : previousSource);
  for (const version of [previousVersion, currentVersion]) {
    if (removeOldAssets && version === previousVersion && pathname.startsWith('/assets/') && pathname.includes(`-${version}-`)) {
      response.statusCode = 404; return response.end('expired fixture asset');
    }
    if (pathname === asset('entry', version)) return response.end(`window.entryVersion=${JSON.stringify(version)};window.loadLazy=()=>import(${JSON.stringify(asset('lazy', version))});`);
    if (pathname === asset('lazy', version)) return response.end(`export const version=${JSON.stringify(version)};`);
  }
  response.setHeader('Content-Type', 'text/html');
  response.end(`<!doctype html><html><head><title>Cutover fixture</title></head><body><h1>Cutover fixture</h1><script type="module" src="${asset('entry', phase)}"></script></body></html>`);
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
const checks = [];
const status = (page, target = 'controller') => page.evaluate(async target => {
 const worker = target === 'waiting' ? (await navigator.serviceWorker.getRegistration()).waiting : navigator.serviceWorker.controller;
 return new Promise((resolve, reject) => {
  const timer = setTimeout(() => { navigator.serviceWorker.removeEventListener('message', listener); reject(new Error('worker status deadline')); }, 5000);
  const listener = event => { if (event.data.type !== 'SW_STATUS' || event.source !== worker) return; clearTimeout(timer); navigator.serviceWorker.removeEventListener('message', listener); resolve(event.data); };
  navigator.serviceWorker.addEventListener('message', listener);
  worker.postMessage({ type: 'GET_SW_STATUS' });
}); }, target);
const setRollback = (page, enabled) => page.evaluate(enabled => new Promise((resolve, reject) => {
  const timer = setTimeout(() => { navigator.serviceWorker.removeEventListener('message', listener); reject(new Error('rollback acknowledgement deadline')); }, 5000);
  const listener = event => {
    if (event.data.type !== 'CACHE_ROLLBACK_MODE' || event.data.enabled !== enabled) return;
    clearTimeout(timer); navigator.serviceWorker.removeEventListener('message', listener); resolve(true);
  };
  navigator.serviceWorker.addEventListener('message', listener);
  navigator.serviceWorker.controller.postMessage({ type: 'SET_ROLLBACK_MODE', enabled });
}), enabled);
const waitForStatus = async (page, predicate) => {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const value = await status(page);
    if (predicate(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error('worker state deadline');
};
try {
  browser = await chromium.launch({ headless: true, executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH });
  const context = await browser.newContext({ serviceWorkers: 'allow' });
  context.setDefaultTimeout(15_000);
  const oldTab = await context.newPage();
  await oldTab.goto(base);
  await oldTab.evaluate(async () => { await navigator.serviceWorker.register('/sw.js?build=previous', { type: 'module', updateViaCache: 'none' }); await navigator.serviceWorker.ready; });
  await oldTab.waitForFunction(() => !!navigator.serviceWorker.controller);
  await oldTab.reload();
  await oldTab.waitForFunction(version => window.entryVersion === version, previousVersion);
  await oldTab.evaluate(url => fetch(url).then(response => { if (!response.ok) throw new Error('old lazy warm-up failed'); return response.text(); }), asset('lazy', previousVersion));
  assert.equal((await status(oldTab)).version, previousVersion);
  // Exercise CacheStorage fallback even when the browser's separate HTTP
  // cache has no old asset. This CDP session belongs only to this test browser.
  const cacheControl = await context.newCDPSession(oldTab);
  await cacheControl.send('Network.clearBrowserCache'); await cacheControl.detach();
  const updater = await context.newPage(); await updater.goto(base);
  phase = currentVersion;
  await updater.evaluate(async () => { const registration = await navigator.serviceWorker.getRegistration(); await registration.update(); });
  await updater.waitForFunction(async () => !!(await navigator.serviceWorker.getRegistration())?.waiting);
  assert.equal((await status(oldTab)).version, previousVersion);
  assert.equal(await oldTab.evaluate(() => window.entryVersion), previousVersion);
  assert.equal((await status(updater, 'waiting')).version, currentVersion);
  checks.push('new worker waits while old clients remain active');
  for (const page of [oldTab, updater]) await page.evaluate(() => {
    window.activationBarrier = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('controller change deadline')), 10_000);
      navigator.serviceWorker.addEventListener('controllerchange', () => { clearTimeout(timer); resolve(true); }, { once: true });
    });
  });
  await updater.evaluate(async () => { const registration = await navigator.serviceWorker.getRegistration(); registration.waiting.postMessage({ type: 'ACTIVATE_UPDATE' }); });
  await Promise.all([oldTab, updater].map(page => page.evaluate(() => window.activationBarrier)));
  await waitForStatus(updater, value => value.version === currentVersion);
  await waitForStatus(oldTab, value => value.version === currentVersion);
  assert.equal((await status(oldTab)).version, currentVersion);
  assert.equal(await oldTab.evaluate(() => window.entryVersion), previousVersion);
  await updater.reload();
  await updater.waitForFunction(version => window.entryVersion === version, currentVersion);
  await updater.evaluate(url => fetch(url).then(response => response.text()), asset('lazy', currentVersion));
  const names = await updater.evaluate(() => caches.keys());
  assert.ok(names.includes(policy.appCache) && names.includes(policy.previousAppCache));
  checks.push('activation preserves old tab and both asset generations');
  disconnectNetwork = true;
  await context.setOffline(true);
  assert.equal(await oldTab.evaluate(() => window.loadLazy().then(module => module.version)), previousVersion);
  assert.equal(await updater.evaluate(() => window.loadLazy().then(module => module.version)), currentVersion);
  await updater.reload();
  await updater.waitForFunction(version => window.entryVersion === version, currentVersion);
  checks.push('old lazy chunk and new shell load offline after activation');
  await context.setOffline(false);
  disconnectNetwork = false;
  phase = previousVersion;
  await setRollback(updater, true);
  await waitForStatus(updater, value => value.rollbackMode);
  await updater.reload();
  await updater.waitForFunction(version => window.entryVersion === version, previousVersion);
  disconnectNetwork = true;
  await context.setOffline(true);
  const rollbackTab = await context.newPage(); await rollbackTab.goto(`${base}/offline-rollback`);
  await rollbackTab.waitForFunction(version => window.entryVersion === version, previousVersion);
  checks.push('rollback serves previous HTML and assets online and offline');
  await context.setOffline(false);
  disconnectNetwork = false;
  await setRollback(updater, false);
  await waitForStatus(updater, value => !value.rollbackMode);
  assert.equal(await updater.evaluate(async ({ cache, url }) => !!(await (await caches.open(cache)).match(url)), { cache: policy.appCache, url: asset('lazy', previousVersion) }), false, 'old offline fallback must remain in the previous cache');
  await updater.evaluate(async ({ cache, version, windowMs }) => {
    const metadata = await caches.open(cache), url = `/__meshcore_sw_metadata_${version}__`;
    const response = await metadata.match(url); if (!response) throw new Error('versioned metadata missing');
    const state = await response.json(); state.activatedAt = Date.now() - windowMs - 1000;
    await metadata.put(url, new Response(JSON.stringify(state), { headers: { 'content-type': 'application/json' } }));
  }, { cache: policy.metadataCache, version: currentVersion, windowMs: policy.rollbackWindowMs });
  // Restart the owned worker to verify it reads the persisted expiry timestamp.
  const cdp = await context.newCDPSession(updater);
  const versions = new Map();
  cdp.on('ServiceWorker.workerVersionUpdated', event => { for (const version of event.versions) versions.set(version.versionId, version); });
  await cdp.send('ServiceWorker.enable');
  const waitLifecycle = async predicate => {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const version = [...versions.values()].find(predicate);
      if (version) return version;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    throw new Error('worker lifecycle deadline');
  };
  const active = await waitLifecycle(version => version.status === 'activated');
  await cdp.send('ServiceWorker.stopAllWorkers');
  await waitLifecycle(version => version.versionId === active.versionId && version.runningStatus === 'stopped');
  removeOldAssets = true; phase = currentVersion;
  assert.equal(await updater.evaluate(url => fetch(url, { cache: 'no-store' }).then(response => response.status), asset('lazy', previousVersion)), 404);
  await updater.waitForFunction(async previous => !(await caches.keys()).includes(previous), policy.previousAppCache);
  assert.ok((await updater.evaluate(() => caches.keys())).includes(policy.appCache));
  checks.push('persisted seven-day expiry removes previous caches and refuses stale fallback');
  const evidence = { checkedAt: new Date().toISOString(), browser: browser.version(), currentVersion, previousVersion, previousSource: previousPath ? 'archived implementation' : 'previous namespace fixture', currentRuntimeSha256: createHash('sha256').update(currentSource).digest('hex'), previousRuntimeSha256: createHash('sha256').update(previousSource).digest('hex'), checks, requests: requests.length };
  if (option('--output')) { const directory = path.resolve(option('--output')); await mkdir(directory, { recursive: true }); await writeFile(path.join(directory, 'cutover.json'), JSON.stringify(evidence, null, 2) + '\n', { flag: 'wx' }); }
  console.log(JSON.stringify(evidence, null, 2));
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
}
