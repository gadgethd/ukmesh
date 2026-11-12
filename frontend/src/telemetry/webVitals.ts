import { onCLS, onINP, onLCP } from 'web-vitals';
import { isTelemetryDisabled } from './clientErrors.js';

const routes = new Set(['/', '/docs', '/stats', '/feed', '/regions', '/topology']);
export function publicVitalRoute(pathname: string): string | null {
  const route = pathname.length > 1 ? pathname.replace(/\/$/, '') : pathname;
  return routes.has(route) ? route : null;
}

/** One reported sample per metric/document. No identifiers, URLs, attribution or retries. */
export function createWebVitalsReporter(route: string, fetcher: typeof fetch = fetch) {
  const sent = new Set<string>();
  return (metric: { name: string; value: number }) => {
    if (isTelemetryDisabled() || !routes.has(route) || sent.has(metric.name)) return;
    if (!['CLS', 'LCP', 'INP'].includes(metric.name) || !Number.isFinite(metric.value)
      || metric.value < 0 || metric.value > (metric.name === 'CLS' ? 10 : 600_000)) return;
    sent.add(metric.name);
    try {
      void fetcher('/api/telemetry/web-vitals', {
        method: 'POST', credentials: 'omit', cache: 'no-store', keepalive: true,
        referrerPolicy: 'no-referrer',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ route, metric: metric.name, value: metric.value }),
        signal: AbortSignal.timeout(5_000),
      }).then(response => response.body?.cancel()).catch(() => undefined);
    } catch { /* Reporting must not break a browser lifecycle handler. */ }
  };
}

let installed = false;
export function installPublicWebVitals(pathname: string): void {
  const route = publicVitalRoute(pathname);
  if (installed || !route || isTelemetryDisabled()) return;
  installed = true;
  const report = createWebVitalsReporter(route);
  // Document metrics remain attributed to their initial route. Soft-navigation
  // support differs between browsers; do not mix it into the same distribution.
  onCLS(report, { reportSoftNavs: false });
  onLCP(report, { reportSoftNavs: false });
  onINP(report, { reportSoftNavs: false });
}
