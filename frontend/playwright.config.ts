import { defineConfig } from '@playwright/test';

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
  webServer: [
    {
      command: 'VITE_APP_HOSTNAME=app.invalid VITE_SITE=ukmesh npm run dev -- --host 127.0.0.1 --port 4173',
      port: 4173,
      reuseExistingServer: !process.env['CI'],
    },
    {
      command: 'VITE_APP_HOSTNAME=127.0.0.1 VITE_SITE=ukmesh VITE_NETWORK=ukmesh npm run dev -- --host 127.0.0.1 --port 4174',
      port: 4174,
      reuseExistingServer: !process.env['CI'],
    },
    {
      command: 'VITE_APP_HOSTNAME=app.invalid VITE_SITE=dev npm run dev -- --host 127.0.0.1 --port 4175',
      port: 4175,
      reuseExistingServer: !process.env['CI'],
    },
    {
      command: 'VITE_APP_HOSTNAME=app.invalid VITE_SITE=teesside npm run dev -- --host 127.0.0.1 --port 4176',
      port: 4176,
      reuseExistingServer: !process.env['CI'],
    },
  ],
});
