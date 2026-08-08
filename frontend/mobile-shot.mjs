// Mobile layout verification captures — run against the local app container (skips anubis).
// Usage: node /tmp/mobile-shot.mjs [outdir]
import { chromium } from 'playwright';
import { mkdirSync } from 'fs';

const outdir = process.argv[2] ?? '/tmp/mobile-shots';
mkdirSync(outdir, { recursive: true });

const UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1 Googlebot/2.1';

const browser = await chromium.launch({ args: ['--no-sandbox'] });
const context = await browser.newContext({
  viewport: { width: 390, height: 844 },
  deviceScaleFactor: 2,
  isMobile: true,
  hasTouch: true,
  userAgent: UA,
});
const page = await context.newPage();

const shots = [
  { name: 'map-top', url: 'http://127.0.0.1:3003/', full: false },
  { name: 'map-full', url: 'http://127.0.0.1:3003/', full: true },
  { name: 'login', url: 'http://127.0.0.1:3003/login', full: true },
];

for (const s of shots) {
  try {
    await page.goto(s.url, { waitUntil: 'load', timeout: 30000 });
    await page.waitForTimeout(4000);
    await page.screenshot({ path: `${outdir}/${s.name}.png`, fullPage: s.full });
    console.log(`OK ${s.name}`);
  } catch (e) {
    console.log(`FAIL ${s.name}: ${e.message.slice(0, 120)}`);
  }
}

await browser.close();
console.log('done ->', outdir);
