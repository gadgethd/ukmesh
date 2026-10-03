import { defineConfig } from '@playwright/test';
import config from './playwright.config.js';
import { publicOrigin } from './test/e2e/localServers.js';

// Optional feed verification. The default Chromium matrix remains unchanged.
export default defineConfig({
  ...config,
  workers: 1,
  retries: 0,
  projects: (['firefox', 'webkit'] as const).map(browserName => ({
    name: `feed-${browserName}`,
    testMatch: /feed-virtualizer\.spec\.ts/,
    use: { browserName, baseURL: publicOrigin },
  })),
});
