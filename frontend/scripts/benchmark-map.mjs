/** Owned production preview, invented WS data and real store/renderer updates. */
import { chromium } from 'playwright';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, open, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import os from 'node:os';
import { mapHandle, nodeMapHandle } from '../test/browserMapHandle.ts';
import { publicFixture } from './lighthouse-fixtures.mjs';

const frontend = fileURLToPath(new URL('..', import.meta.url));
const args = process.argv.slice(2);
const option = (name, fallback) => args.includes(`--${name}`) ? args[args.indexOf(`--${name}`) + 1] : fallback;
const label = option('label', String(Date.now()));
if (!/^[a-z0-9][a-z0-9-]*$/.test(label)) throw new Error('use a lowercase measurement label');
const counts = option('counts', '1000,3000,6000').split(',').map(Number);
const runs = Number(option('runs', '3'));
const durationMs = Number(option('duration-ms', '6000'));
const selectedMode = option('mode', null);
if (counts.length > 3 || !counts.every(n => [1000, 3000, 6000].includes(n))) throw new Error('counts must select 1000,3000,6000');
if (!Number.isInteger(runs) || runs < 1 || runs > 5 || !Number.isInteger(durationMs) || durationMs < 3000 || durationMs > 30000) throw new Error('invalid run count or duration');
if (selectedMode && !['desktop', 'mobile'].includes(selectedMode)) throw new Error('invalid mode');
const output = path.join(frontend, '..', 'plans', 'measurements', `map-${label}`);
await mkdir(path.dirname(output), { recursive: true });
await mkdir(output); // Never discard an earlier measurement or build.
const artifact = path.join(output, 'app');
const metadata = { revision: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: frontend, encoding: 'utf8' }).trim(),
  toolSha256: createHash('sha256').update(await readFile(fileURLToPath(import.meta.url))).digest('hex'),
  diffSha256: createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'], { cwd: frontend })).digest('hex'),
  at: new Date().toISOString(), node: process.version, cpus: os.cpus().length,
  cpu: os.cpus()[0]?.model, memory: os.totalmem(), fixture: 'invented-grid-v1', links: 0,
  updateBatch: 50, updateIntervalMs: 100, durationMs, runs, warmupMs: 3000, drainMs: 600 };
const buildLog = await open(path.join(output, 'build.log'), 'wx');
try {
  const build = spawn('npm', ['run', 'build', '--', '--outDir', artifact], { cwd: frontend,
    env: { ...process.env, VITE_BUILD_TARGET: 'app', VITE_SITE: 'ukmesh', VITE_NETWORK: 'ukmesh',
      VITE_APP_HOSTNAME: '127.0.0.1', VITE_RF_COVERAGE_ENABLED: 'false' }, stdio: ['ignore', buildLog.fd, buildLog.fd] });
  const [code] = await once(build, 'exit');
  if (code !== 0) throw new Error(`app build failed; inspect ${output}/build.log`);
} finally { await buildLog.close(); }
metadata.manifestSha256 = createHash('sha256').update(await readFile(path.join(artifact, '.vite/manifest.json'))).digest('hex');

let nodes = [];
let sent = 0;
let pump;
const unexpected = new Set();
const fixture = createServer((req, res) => {
  req.resume();
  const url = new URL(req.url, 'http://fixture.invalid');
  let data;
  if (url.pathname === '/api/runtime-config') data = { version: 1, packetArcs: false, heatmap: false, regionScoping: false,
    privacyGeneration: 1, refreshAfterSeconds: 30 };
  else if (url.pathname === '/api/nodes/map') {
    const start = Number(url.searchParams.get('cursor')) || 0;
    const limit = Math.min(2000, Number(url.searchParams.get('limit')) || 2000);
    data = { nodes: nodes.slice(start, start + limit), page: { complete: start + limit >= nodes.length,
      nextCursor: start + limit >= nodes.length ? null : String(start + limit), snapshot: metadata.at, rowLimit: limit } };
  } else if (url.pathname === '/api/inferred-nodes') data = { inferredNodes: [], inferredActiveNodeIds: [] };
  else if (url.pathname === '/api/node-status/latest' || url.pathname === '/api/activity/timeline') data = [];
  else data = publicFixture(url.pathname, url.searchParams);
  if (data === undefined) unexpected.add(url.pathname);
  res.writeHead(data === undefined ? 501 : 200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(data ?? { error: 'Unhandled benchmark fixture' }));
});
const sockets = new WebSocketServer({ server: fixture, path: '/ws', maxPayload: 16 * 1024 });
sockets.on('connection', socket => socket.send(JSON.stringify({ type: 'initial_state', ts: Date.now(),
  data: { nodes, packets: [], messages: [], viable_links: [] } })));
fixture.listen(0, '127.0.0.1');
await once(fixture, 'listening');
const preview = spawn(process.execPath, ['node_modules/vite/bin/vite.js', 'preview', '--outDir', artifact,
  '--host', '127.0.0.1', '--port', '4187', '--strictPort'], { cwd: frontend,
  env: { ...process.env, UKMESH_DEV_API_TARGET: `http://127.0.0.1:${fixture.address().port}` }, stdio: ['ignore', 'pipe', 'pipe'] });
let ready = false;
let previewOutput = '';
let previewError = '';
preview.stdout.on('data', chunk => {
  previewOutput = (previewOutput + chunk).slice(-4000);
  if (previewOutput.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '').includes('http://127.0.0.1:4187')) ready = true;
});
preview.stderr.on('data', chunk => { previewError = (previewError + chunk).slice(-4000); });
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const percentile = (values, ratio) => [...values].sort((a, b) => a - b)[Math.ceil(values.length * ratio) - 1];
const samples = [];
let browser;
try {
  for (let i = 0; i < 100 && !ready; i++) {
    if (preview.exitCode !== null) throw new Error(`preview failed: ${previewError}`);
    await pause(100);
  }
  if (!ready) throw new Error(`preview startup deadline: ${previewOutput} ${previewError}`);
  browser = await chromium.launch({ executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    args: ['--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
  metadata.browser = browser.version();
  const flatTerrain = await readFile(new URL('../test/e2e/fixtures/flat-terrain.png', import.meta.url));
  for (const mode of selectedMode ? [selectedMode] : ['desktop', 'mobile']) {
    for (const count of counts) {
      for (let run = 1; run <= runs; run++) {
        nodes = Array.from({ length: count }, (_, index) => {
          const id = createHash('sha256').update(`ukmesh invented map fixture ${index}`).digest('hex').toUpperCase();
          return { node_id: id, name: `Invented map node ${index + 1}`, role: 2, network: 'ukmesh',
            lat: 50.3 + parseInt(id.slice(0, 8), 16) / 0xffffffff * 7.8,
            lon: -6.6 + parseInt(id.slice(8, 16), 16) / 0xffffffff * 7.2,
            last_seen: new Date().toISOString(), is_online: true, advert_count: 10 };
        });
        const context = await browser.newContext({ serviceWorkers: 'block', deviceScaleFactor: 1,
          viewport: mode === 'mobile' ? { width: 390, height: 844 } : { width: 1280, height: 800 },
          isMobile: mode === 'mobile', hasTouch: mode === 'mobile' });
        try {
          const page = await context.newPage();
          page.setDefaultTimeout(45_000);
          const errors = [];
          page.on('pageerror', error => errors.push(error.message));
          await page.addInitScript(() => {
            localStorage.setItem('meshcore-disclaimer-dismissed', '1');
            localStorage.setItem('meshcore-cookie-consent-v1', '1');
            globalThis.__mapBenchReceived = 0;
            const Native = WebSocket;
            globalThis.WebSocket = class extends Native {
              constructor(...args) {
                super(...args);
                this.addEventListener('message', event => {
                  if (typeof event.data === 'string' && event.data.includes('"type":"node_update"')) globalThis.__mapBenchReceived++;
                });
              }
            };
          });
          await page.route('**/*', route => {
            const url = new URL(route.request().url());
            if (url.pathname.startsWith('/terrain-tiles/')) return route.fulfill({ contentType: 'image/png', body: flatTerrain });
            if (url.hostname === 'tiles.openfreemap.org') return route.fulfill(url.pathname.startsWith('/styles/')
              ? { json: { version: 8, glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
                sources: { openmaptiles: { type: 'vector', tiles: ['https://tiles.openfreemap.org/benchmark/{z}/{x}/{y}.pbf'] } },
                layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#080d14' } },
                  { id: 'water', type: 'fill', source: 'openmaptiles', 'source-layer': 'water' }] } }
              : { contentType: 'application/x-protobuf', body: Buffer.alloc(0) });
            if (url.hostname === '127.0.0.1' || !['http:', 'https:'].includes(url.protocol)) return route.continue();
            unexpected.add(url.origin);
            return route.abort();
          });
          await page.goto('http://127.0.0.1:4187/?map=54.5,-2.5,6');
          await page.bringToFront();
          await page.locator('canvas.maplibregl-canvas').waitFor();
          const map = await mapHandle(page);
          const deadline = Date.now() + 45_000;
          let features = 0;
          while (features !== count && Date.now() < deadline) {
            features = await map.evaluate(async map => {
              const source = map.getSource('nodes');
              const data = source && await source.getData();
              return data?.features?.length ?? 0;
            });
            if (features !== count) await pause(100);
          }
          if (features !== count) throw new Error(`expected ${count} node features, received ${features}`);
          await pause(metadata.warmupMs);
          const expectedSeen = new Map();
          const cdp = await context.newCDPSession(page);
          await cdp.send('Performance.enable');
          if (mode === 'mobile') await cdp.send('Emulation.setCPUThrottlingRate', { rate: 4 });
          const metrics = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(value => [value.name, value.value]));
          const before = await metrics();
          sent = 0;
          pump = setInterval(() => {
            for (let i = 0; i < metadata.updateBatch; i++) {
              const node = nodes[sent % count];
              const timestamp = Date.now();
              expectedSeen.set(node.node_id, timestamp);
              for (const socket of sockets.clients) if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({
                type: 'node_update', ts: timestamp, data: { nodeId: node.node_id, ts: timestamp },
              }));
              sent++;
            }
          }, metadata.updateIntervalMs);
          const frames = await page.evaluate(duration => new Promise(resolve => {
            const values = [];
            const start = performance.now();
            let last = start;
            const frame = at => {
              values.push(at - last); last = at;
              if (at - start < duration) requestAnimationFrame(frame);
              else resolve(values.slice(1));
            };
            requestAnimationFrame(frame);
          }), durationMs);
          clearInterval(pump); pump = undefined;
          await page.waitForFunction(expected => globalThis.__mapBenchReceived >= expected, sent);
          await pause(metadata.drainMs);
          const after = await metrics();
          const store = await nodeMapHandle(page);
          const applied = await store.evaluate((nodes, expected) => expected.filter(([id, timestamp]) =>
            nodes.get(id)?.last_seen === new Date(timestamp).toISOString()).length, [...expectedSeen]);
          await store.dispose();
          if (applied !== expectedSeen.size) throw new Error(`node store did not drain latest presence: ${applied}/${expectedSeen.size}`);
          const received = await page.evaluate(() => globalThis.__mapBenchReceived);
          if (errors.length || unexpected.size) throw new Error(`invalid map fixture: ${errors.join('; ')} ${[...unexpected].join(', ')}`);
          const sample = { mode, count, run, viewport: page.viewportSize(), cpuThrottle: mode === 'mobile' ? 4 : 1,
            frames: frames.length, p50Ms: percentile(frames, 0.5), p95Ms: percentile(frames, 0.95),
            over50Ms: frames.filter(value => value > 50).length, sent, received, appliedLatest: applied,
            taskSeconds: after.TaskDuration - before.TaskDuration, scriptSeconds: after.ScriptDuration - before.ScriptDuration,
            heapBefore: before.JSHeapUsedSize, heapAfter: after.JSHeapUsedSize, layers: await map.evaluate(map => map.getStyle().layers.length) };
          samples.push(sample);
          await writeFile(path.join(output, 'map.json'), JSON.stringify({ version: 1, metadata, samples }, null, 2));
          console.log(`${mode} ${count} #${run}: p95 ${sample.p95Ms.toFixed(1)}ms, task ${sample.taskSeconds.toFixed(3)}s, ${received}/${sent} updates`);
        } finally { clearInterval(pump); pump = undefined; await context.close(); }
      }
    }
  }
} finally {
  clearInterval(pump);
  await browser?.close();
  preview.kill('SIGTERM');
  for (const socket of sockets.clients) socket.terminate();
  sockets.close(); fixture.closeAllConnections();
  await new Promise(resolve => fixture.close(resolve));
}
