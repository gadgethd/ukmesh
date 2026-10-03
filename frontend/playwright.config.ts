import { defineConfig, devices } from '@playwright/test';
import { portBase, publicOrigin, dashboardOrigin } from './test/e2e/localServers.js';

// Parallel worktrees can select isolated ports instead of reusing a stale server.
const reuseExistingServer = !process.env['CI'] && !process.env['PLAYWRIGHT_PORT_BASE'];

export default defineConfig({
  testDir: './test/e2e',
  fullyParallel: true,
  forbidOnly: Boolean(process.env['CI']),
  retries: process.env['CI'] ? 1 : 0,
  reporter: process.env['CI'] ? 'github' : 'list',
  use: {
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'public-desktop',
      testMatch: /(?:accessibility|owner|public|feed-virtualizer)\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], baseURL: publicOrigin },
    },
    {
      name: 'dashboard-desktop',
      testMatch: /dashboard\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], baseURL: dashboardOrigin },
    },
    {
      name: 'dashboard-mobile',
      testMatch: /dashboard\.spec\.ts/,
      use: { ...devices['Pixel 7'], baseURL: dashboardOrigin },
    },
    {
      name: 'mobile-regression',
      testMatch: /mobile\.spec\.ts/,
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: [
    {
      command: `VITE_APP_HOSTNAME=app.invalid VITE_SITE=ukmesh npm run dev -- --config playwright.vite.config.ts --host 127.0.0.1 --port ${portBase} --strictPort`,
      port: portBase,
      reuseExistingServer,
    },
    {
      command: `VITE_APP_HOSTNAME=127.0.0.1 VITE_SITE=ukmesh VITE_NETWORK=ukmesh VITE_RF_COVERAGE_ENABLED=true npm run dev -- --config playwright.vite.config.ts --host 127.0.0.1 --port ${portBase + 1} --strictPort`,
      port: portBase + 1,
      reuseExistingServer,
    },
    {
      command: `VITE_APP_HOSTNAME=app.invalid VITE_SITE=dev npm run dev -- --config playwright.vite.config.ts --host 127.0.0.1 --port ${portBase + 2} --strictPort`,
      port: portBase + 2,
      reuseExistingServer,
    },
  ],
});
