/** Sequential Lighthouse route measurements; results stay in ignored plans/. */
import lighthouse from 'lighthouse';
import desktopConfig from 'lighthouse/core/config/desktop-config.js';
import { launch } from 'chrome-launcher';
import { chromium } from 'playwright';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import path from 'node:path';

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const index = args.indexOf(`--${name}`);
  return index < 0 ? fallback : args[index + 1];
};
const base = new URL(option('base-url', 'http://127.0.0.1:4173'));
if (!['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)) throw new Error('use a local production preview');
const output = path.resolve(option('output', `../plans/measurements/website-${Date.now()}`));
const runs = Number(option('runs', '3'));
if (!Number.isSafeInteger(runs) || runs < 1 || runs > 10) throw new Error('--runs must be 1..10');
const routes = ['/', '/docs', '/stats', '/feed', '/regions'];
const selected = option('route', undefined);
if (selected && ![...routes, '/topology', '/spam', '/login', '/owner'].includes(selected)) throw new Error('unknown website route');
const selectedMode = option('mode', undefined);
if (selectedMode && !['mobile', 'desktop'].includes(selectedMode)) throw new Error('unknown mode');
await mkdir(path.dirname(output), { recursive: true });
await mkdir(output);
const measurements = [];
for (const mode of selectedMode ? [selectedMode] : ['mobile', 'desktop']) {
  for (const route of selected ? [selected] : routes) {
    let rateRetries = 0;
    for (let run = 0; run < runs; run += 1) {
      let rateWaitMs = 0;
      const chrome = await launch({ chromePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || chromium.executablePath(),
        chromeFlags: ['--headless', '--no-sandbox', '--disable-dev-shm-usage', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
      try {
        execFileSync(process.execPath, ['--import', path.resolve('../backend/node_modules/tsx/dist/loader.mjs'),
          path.resolve('../backend/src/tools/refreshSampleRecent.ts')], { stdio: 'pipe' });
        const result = await lighthouse(new URL(route, base).href,
          { port: chrome.port, output: 'json', logLevel: 'error', onlyCategories: ['performance', 'accessibility', 'seo'] },
          mode === 'desktop' ? desktopConfig : undefined);
        if (!result || result.lhr.runtimeError) throw new Error(result?.lhr.runtimeError?.message || 'missing Lighthouse result');
        const { lhr } = result;
        const resources = lhr.audits['resource-summary']?.details?.items ?? [];
        const metric = (name) => lhr.audits[name]?.numericValue ?? null;
        const name = `${route === '/' ? 'home' : route.slice(1)}-${mode}-${run + 1}`;
        const badApi = (lhr.audits['network-requests']?.details?.items ?? []).filter(item => {
          const url = new URL(item.url, base);
          return url.origin === base.origin && url.pathname.startsWith('/api/')
            && (item.statusCode >= 400 || item.statusCode === 0)
            && !(item.statusCode === 401 && url.pathname.startsWith('/api/owner/'));
        });
        if (badApi.length) {
          const throttled = badApi.every(item => item.statusCode === 429);
          await writeFile(path.join(output, `${name}-invalid-${++rateRetries}.json`), result.report);
          if (!throttled || rateRetries > 3) throw new Error(`invalid website measurement: ${badApi.map(item => `${new URL(item.url).pathname} HTTP ${item.statusCode}`).join(', ')}`);
          console.log(`${name}: rate limited; close Chrome and wait for the shared limiter before repeating`);
          rateWaitMs = 60_000;
          run -= 1;
          continue;
        }
        rateRetries = 0;
        await writeFile(path.join(output, `${name}.json`), result.report);
        const entry = { route, mode, run: run + 1, lcpMs: metric('largest-contentful-paint'), cls: metric('cumulative-layout-shift'),
          tbtMs: metric('total-blocking-time'), scores: Object.fromEntries(Object.entries(lhr.categories).map(([key, value]) => [key, value.score])),
          resources, environment: lhr.environment, configSettings: lhr.configSettings,
          warnings: lhr.runWarnings, failedRequests: lhr.audits['network-requests']?.details?.items?.filter((item) => item.statusCode >= 400).map(({ url, statusCode }) => ({ url, statusCode })) ?? [] };
        if (![entry.lcpMs, entry.cls, entry.tbtMs].every(Number.isFinite)) throw new Error('missing navigation metric');
        measurements.push(entry);
        console.log(`${route} ${mode} #${run + 1}: LCP ${entry.lcpMs?.toFixed(0)}ms, CLS ${entry.cls}, TBT ${entry.tbtMs?.toFixed(0)}ms`);
      } finally {
        await chrome.kill();
        if (rateWaitMs) await new Promise(resolve => setTimeout(resolve, rateWaitMs));
      }
    }
  }
}
await writeFile(path.join(output, 'website.json'), JSON.stringify({
  version: 1, at: new Date().toISOString(), commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
  node: process.version, lighthouse: JSON.parse(await readFile(new URL('../node_modules/lighthouse/package.json', import.meta.url))).version,
  browser: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || chromium.executablePath(), runs,
  cache: 'fresh Chrome profile per navigation; origin caches warmed separately',
  inp: 'Navigation does not measure INP; interaction timings and authorized field RUM are separate gates.', measurements,
}, null, 2) + '\n');
