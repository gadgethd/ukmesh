import { defineConfig, devices } from '@playwright/test';

// Exercise the emitted production worker, which the Vite dev server cannot test.
export default defineConfig({
  testDir: './test/e2e',
  testMatch: /basemap\.spec\.ts/,
  timeout: 60_000,
  workers: 1,
  use: {
    baseURL: 'http://127.0.0.1:4180',
    serviceWorkers: 'block',
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
    launchOptions: {
      executablePath: process.env['PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'],
      args: ['--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'],
    },
  },
  projects: [
    { name: 'basemap-desktop', use: { ...devices['Desktop Chrome'], viewport: { width: 900, height: 650 } } },
    { name: 'basemap-mobile', use: { ...devices['Pixel 7'], deviceScaleFactor: 1 } },
  ],
  webServer: {
    command: 'npm run preview -- --host 127.0.0.1 --port 4180',
    port: 4180,
    reuseExistingServer: !process.env['CI'],
  },
});
