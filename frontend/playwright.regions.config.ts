import { defineConfig } from '@playwright/test';
import base from './playwright.config.js';

// These checks inspect source-store state and require a development server.
// Only the dashboard server is used; avoid compiling two idle website servers.
export default defineConfig({
  ...base,
  fullyParallel: false,
  workers: 1,
  testMatch: /regions\.spec\.ts/,
  projects: base.projects?.filter(project => project.name.startsWith('dashboard-'))
    .map(project => ({ ...project, testMatch: /regions\.spec\.ts/ })),
  webServer: Array.isArray(base.webServer) ? [base.webServer[1]] : base.webServer,
});
