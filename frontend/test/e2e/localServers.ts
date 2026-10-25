// Keep cross-site navigations on the same isolated servers as Playwright.
export const portBase = Number(process.env['PLAYWRIGHT_PORT_BASE'] ?? 4173);
export const publicOrigin = `http://127.0.0.1:${portBase}`;
export const dashboardOrigin = `http://127.0.0.1:${portBase + 1}`;
export const devOrigin = `http://127.0.0.1:${portBase + 2}`;
