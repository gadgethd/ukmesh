import { defineConfig, devices } from '@playwright/test';

// Parallel worktrees can select isolated ports instead of reusing a stale server.
const portBase = Number(process.env['PLAYWRIGHT_PORT_BASE'] ?? 4173);

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
      use: { ...devices['Desktop Chrome'], baseURL: `http://127.0.0.1:${portBase}` },
    },
    {
      name: 'dashboard-desktop',
      testMatch: /dashboard\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], baseURL: `http://127.0.0.1:${portBase + 1}` },
    },
    {
      name: 'dashboard-mobile',
      testMatch: /dashboard\.spec\.ts/,
      use: { ...devices['Pixel 7'], baseURL: `http://127.0.0.1:${portBase + 1}` },
    },
    {
      name: 'mobile-regression',
      testMatch: /mobile\.spec\.ts/,
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: [
    {
      command: `VITE_APP_HOSTNAME=app.invalid VITE_SITE=ukmesh npm run dev -- --host 127.0.0.1 --port ${portBase} --strictPort`,
      port: portBase,
      reuseExistingServer: !process.env['CI'],
    },
    {
      command: `VITE_APP_HOSTNAME=127.0.0.1 VITE_SITE=ukmesh VITE_NETWORK=ukmesh VITE_RF_COVERAGE_ENABLED=true npm run dev -- --host 127.0.0.1 --port ${portBase + 1} --strictPort`,
      port: portBase + 1,
      reuseExistingServer: !process.env['CI'],
    },
    {
      command: `VITE_APP_HOSTNAME=app.invalid VITE_SITE=dev npm run dev -- --host 127.0.0.1 --port ${portBase + 2} --strictPort`,
      port: portBase + 2,
      reuseExistingServer: !process.env['CI'],
    },
  ],
});
