import { chromium, type FullConfig } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fixtures, vectorRoutes } from './basemapFixtures.js';
import warmDevServer from './warmDevServer.js';

/** Fetching top-level modules leaves their imports cold. Load the complete
 * development graph once so dependency discovery/reloads finish before the
 * interop test's readiness deadline. The real worker still decodes real MVT. */
export default async function warmDevelopmentRenderer(config: FullConfig): Promise<void> {
  await warmDevServer(config);
  const project = config.projects[0];
  const origin = project?.use.baseURL;
  if (typeof origin !== 'string') throw new Error('Development warm-up requires its owned server origin');
  const browser = await chromium.launch({ ...project.use.launchOptions, headless: true });
  const page = await browser.newPage({ baseURL: origin, serviceWorkers: 'block' });
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error' && /SyntaxError|TypeError|ReferenceError/.test(message.text())) {
      if (errors.length < 50) errors.push(message.text());
    }
  });
  const started = Date.now();
  try {
    await fixtures(page); await vectorRoutes(page, () => false);
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    await page.locator('.map-area[data-basemap-state="healthy"]').waitFor({ timeout: 180_000 });
    await page.waitForTimeout(2000);
    await page.locator('.map-area[data-basemap-state="healthy"]').waitFor({ timeout: 30_000 });
    if (errors.length) throw new Error(`Development warm-up encountered renderer errors: ${errors.join('; ')}`);
    console.log(`[e2e] complete development graph warm-up: ${Date.now() - started}ms`);
  } catch (error) {
    const directory = path.resolve('test-results/development-warmup');
    await mkdir(directory, { recursive: true });
    await writeFile(path.join(directory, 'failure.json'), JSON.stringify({
      elapsedMs: Date.now() - started, error: String(error), errors,
    }, null, 2));
    await page.screenshot({ path: path.join(directory, 'failure.png'), timeout: 5000 }).catch(() => undefined);
    throw error;
  } finally { await browser.close(); }
}
