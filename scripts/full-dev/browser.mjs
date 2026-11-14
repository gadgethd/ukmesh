import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import {browserLaunchOptions} from './browser-runtime.mjs';
const { chromium } = createRequire(new URL('../../frontend/package.json', import.meta.url))('playwright');
const output = path.resolve(process.argv[2] ?? 'plans/lean-runtime/browser');
assert.ok(output.includes('/plans/lean-runtime/'), 'browser evidence must stay in the owned evidence directory');
await mkdir(output, {recursive: true});
const browser = await chromium.launch(browserLaunchOptions());
const cases = [];
try {
  for (const width of [390, 1440]) {
    const context = await browser.newContext({viewport: {width, height: 900},
      userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'});
    for (const [name, url, api] of [
      ['public', 'http://127.0.0.1:5173/feed', '/api/runtime-config'],
      ['dashboard', 'http://127.0.0.1:3003/', '/api/runtime-config'],
      ['corescope', 'http://127.0.0.1:3102/', '/api/stats'],
      ['beacon', 'http://127.0.0.1:5175/', '/api/v1/info'],
      ['health', 'http://127.0.0.1:3090/', '/api/bootstrap'],
      ['anubis', 'http://127.0.0.1:8923/', '/build-target.json'],
    ]) {
      const page = await context.newPage();
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      try {
        const response = await page.goto(url, {waitUntil: 'domcontentloaded', timeout: 30000});
        assert.ok(response?.ok(), `${name} navigation failed`);
        await page.waitForFunction(() => !document.title.includes('Making sure'), {timeout: 30000});
        await page.waitForTimeout(1500);
        const result = await page.evaluate(async resource => {
          const response = await fetch(resource);
          const body = await response.json();
          return {status: response.status, body: typeof body, overflow: document.documentElement.scrollWidth > innerWidth + 1};
        }, api);
        assert.equal(result.status, 200);
        assert.equal(result.body, 'object');
        assert.equal(result.overflow, false, `${name} overflows at ${width}px`);
        assert.deepEqual(errors, [], `${name} has uncaught browser errors`);
        await page.screenshot({path: path.join(output, `${name}-${width}.png`), fullPage: false});
        cases.push({name, width, passed: true});
      } catch (error) {
        cases.push({name, width, passed: false, error: error.message, browserErrors: errors});
      } finally { await page.close(); }
    }
    await context.close();
  }
} finally { await browser.close(); }
await writeFile(path.join(output, 'results.json'), JSON.stringify({passed: cases.every(row => row.passed), cases}, null, 2) + '\n');
console.log(JSON.stringify(cases));
if (cases.some(row => !row.passed)) process.exitCode = 1;
