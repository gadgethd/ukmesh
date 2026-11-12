import lighthouse from 'lighthouse';
import desktopConfig from 'lighthouse/core/config/desktop-config.js';
import { launch } from 'chrome-launcher';
import puppeteer from 'puppeteer-core';
import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { WebSocketServer } from 'ws';
import { basemapStyle, publicFixture } from './lighthouse-fixtures.mjs';

const config = JSON.parse(await readFile(new URL('./lighthouse-ci.config.json', import.meta.url)));
const args = process.argv.slice(2);
const option = (key, fallback) => args.includes(`--${key}`) ? args[args.indexOf(`--${key}`) + 1] : fallback;
const runs = Number(option('runs', config.runs));
if (!Number.isSafeInteger(runs) || runs < (process.env.CI ? 3 : 1) || runs > 5) throw new Error('invalid Lighthouse run count');
const selected = option('route', null);
if (selected && !config.routes[selected]) throw new Error('unknown route');
const output = path.resolve(option('output', 'test-results/lighthouse-ci'));
if (JSON.parse(await readFile('dist/build-target.json')).target !== 'public-website') throw new Error('build the public website first');
await mkdir(output, { recursive: true });
const metadata = {
  revision: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
  dirty: execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim().length > 0,
  manifestSha256: createHash('sha256').update(await readFile('dist/.vite/manifest.json')).digest('hex'),
  node: process.version,
  lighthouse: JSON.parse(await readFile('node_modules/lighthouse/package.json')).version,
  cpus: os.cpus().length, cpu: os.cpus()[0]?.model, memory: os.totalmem(),
};
const fixture = createServer((req, res) => {
  req.resume();
  const url = new URL(req.url, 'http://fixture.invalid');
  const payload = publicFixture(url.pathname, url.searchParams);
  res.writeHead(payload === undefined ? 501 : 200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(payload ?? { error: 'Unhandled CI fixture' }));
});
const sockets = new WebSocketServer({ server: fixture, path: '/ws', maxPayload: 16 * 1024 });
sockets.on('connection', socket => socket.send(JSON.stringify({ type: 'initial_state', ts: Date.now(), data: {
  nodes: publicFixture('/api/nodes/map').nodes, packets: publicFixture('/api/packets/recent'),
  messages: publicFixture('/api/messages/recent'), viable_links: [],
} })));
fixture.listen(0, '127.0.0.1');
await once(fixture, 'listening');
const preview = spawn(process.execPath, ['node_modules/vite/bin/vite.js', 'preview', '--host', '127.0.0.1', '--port', '4186', '--strictPort'], {
  env: { ...process.env, UKMESH_DEV_API_TARGET: `http://127.0.0.1:${fixture.address().port}` }, stdio: ['ignore', 'pipe', 'pipe'],
});
let previewErrors = '';
let previewOutput = '';
let ownedPreviewReady = false;
preview.stdout.on('data', chunk => {
  previewOutput = (previewOutput + chunk).slice(-4000);
  if (previewOutput.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '').includes('http://127.0.0.1:4186')) ownedPreviewReady = true;
});
preview.stderr.on('data', chunk => { previewErrors = (previewErrors + chunk).slice(-4000); });
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const measurements = [];
const failures = [];
try {
  let ready = false;
  for (let attempt = 0; attempt < 100; attempt++) {
    if (preview.exitCode !== null) throw new Error(`preview exited: ${previewErrors}`);
    try { if (ownedPreviewReady && (await fetch('http://127.0.0.1:4186', { signal: AbortSignal.timeout(500) })).ok) { ready = true; break; } } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  if (!ready) throw new Error(`preview startup deadline exceeded: ${previewOutput} ${previewErrors}`);
  for (const [route, budget] of Object.entries(config.routes).filter(([route]) => !selected || route === selected)) {
    for (const mode of ['mobile', 'desktop']) {
      const samples = [];
      for (let run = 1; run <= runs; run++) {
        const chrome = await launch({ chromePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || chromium.executablePath(), chromeFlags: [
          '--headless', '--no-sandbox', '--disable-dev-shm-usage', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader',
        ] });
        let browser;
        try {
          browser = await puppeteer.connect({ browserURL: `http://127.0.0.1:${chrome.port}` });
          const page = await browser.newPage();
          await page.setBypassServiceWorker(true);
          await page.setRequestInterception(true);
          let populated = false;
          await page.exposeFunction('__lighthouseFixturePopulated', () => { populated = true; });
          await page.evaluateOnNewDocument(route => {
            let reported = false;
            const check = () => {
              if (reported) return;
              const ready = route === '/stats' ? document.querySelector('.stats-page__stat-value')?.textContent === '120'
                : route === '/regions' ? document.querySelector('.regions-page__stats strong')?.textContent === '1'
                : route === '/feed' || route === '/' ? document.querySelector('main')?.textContent?.includes('Synthetic CI message')
                : document.querySelector('main h1')?.textContent === 'UK Mesh documentation';
              if (!ready) return;
              reported = true;
              observer.disconnect();
              void globalThis.__lighthouseFixturePopulated().catch(() => {});
            };
            const observer = new MutationObserver(check);
            observer.observe(document, { childList: true, subtree: true, characterData: true });
            document.addEventListener('DOMContentLoaded', check, { once: true });
          }, route);
          const missing = new Set();
          const seen = new Set();
          page.on('request', request => {
            const url = new URL(request.url());
            if (url.pathname.startsWith('/api/')) {
              const payload = publicFixture(url.pathname, url.searchParams);
              seen.add(url.pathname);
              if (payload === undefined) missing.add(url.pathname);
              void request.continue();
            } else if (url.hostname === 'tiles.openfreemap.org') {
              void request.respond(url.pathname.startsWith('/styles/')
                ? { status: 200, contentType: 'application/json', body: JSON.stringify(basemapStyle) }
                : { status: 200, contentType: 'application/x-protobuf', body: Buffer.alloc(0) });
            } else if (url.hostname === '127.0.0.1' || !['http:', 'https:'].includes(url.protocol)) {
              void request.continue();
            } else {
              missing.add(url.origin);
              void request.abort();
            }
          });
          const result = await lighthouse(`http://127.0.0.1:4186${route}`, { port: chrome.port, logLevel: 'error', output: 'json',
            onlyCategories: ['performance', 'accessibility', 'seo'] }, mode === 'desktop' ? desktopConfig : undefined, page);
          if (!result || result.lhr.runtimeError) throw new Error(result?.lhr.runtimeError?.message || 'missing Lighthouse result');
          if (missing.size) throw new Error(`unhandled CI fixtures: ${[...missing].join(', ')}`);
          const required = route === '/stats' ? '/api/stats/charts' : route === '/regions' ? '/api/regions/summary' : null;
          if (required && !seen.has(required)) throw new Error(`expected populated view did not request ${required}`);
          if (!populated) {
            await writeFile(path.join(output, 'fixture-failure.html'), await page.content());
            throw new Error(`${route} did not render the populated CI fixture (page ${page.url()}, API requests ${[...seen].join(', ')})`);
          }
          const { lhr } = result;
          const metric = key => lhr.audits[key]?.numericValue;
          const sample = { lcpMs: metric('largest-contentful-paint'), cls: metric('cumulative-layout-shift'),
            tbtMs: metric('total-blocking-time'), accessibility: lhr.categories.accessibility.score, seo: lhr.categories.seo.score };
          if (Object.values(sample).some(value => !Number.isFinite(value))) throw new Error('missing navigation metric');
          samples.push(sample);
          await writeFile(path.join(output, `${route.slice(1) || 'home'}-${mode}-${run}.json`), result.report);
        } finally { await browser?.disconnect(); await chrome.kill(); }
      }
      const medians = Object.fromEntries(Object.keys(samples[0]).map(key => [key, median(samples.map(sample => sample[key]))]));
      const limits = { lcpMs: budget[`${mode}LcpMs`], cls: budget.cls, tbtMs: budget.tbtMs };
      for (const [metric, limit] of Object.entries(limits)) if (medians[metric] > limit) failures.push(`${route} ${mode} ${metric}: ${medians[metric]} > ${limit}`);
      for (const [metric, limit] of [['accessibility', config.minimumAccessibility], ['seo', config.minimumSeo]]) {
        if (medians[metric] < limit) failures.push(`${route} ${mode} ${metric}: ${medians[metric]} < ${limit}`);
      }
      measurements.push({ route, mode, samples, medians });
      console.log(`${route} ${mode}: LCP ${medians.lcpMs.toFixed(0)}ms, CLS ${medians.cls.toFixed(3)}, TBT ${medians.tbtMs.toFixed(0)}ms`);
    }
  }
  await writeFile(path.join(output, 'summary.json'), JSON.stringify({ version: 1, fixture: 'invented-public-v1', metadata, runs, measurements, failures }, null, 2));
  if (failures.length) throw new Error(failures.join('\n'));
} finally {
  preview.kill('SIGTERM');
  for (const socket of sockets.clients) socket.terminate();
  sockets.close();
  fixture.closeAllConnections();
  await new Promise(resolve => fixture.close(resolve));
}
