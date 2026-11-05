import type { FullConfig } from '@playwright/test';

/**
 * Warm the dev server's module graph before any test asserts.
 *
 * `maplibre-gl` is deliberately excluded from `optimizeDeps` (see vite.config.ts:
 * pre-bundling rewrites the worker URL and kills the worker). The cost lands on
 * the first cold request instead — on the CI runner the MapLibre worker module
 * takes 41–47 s to transform, measured from the Playwright traces retained on
 * run 41887037964 — which is most of the 60 s map-health budget those specs
 * allow. The map therefore starts ~57 s after the document load and the
 * assertion expires just before the basemap becomes healthy.
 *
 * Playwright starts `webServer` before `globalSetup` (createGlobalSetupTasks
 * runs createPluginSetupTasks first), so paying the transform cost here moves
 * it out of every test's timeout while keeping the assertions tight.
 *
 * This is an optimisation, never a gate: every failure is swallowed and logged.
 */

const WARM_PATHS = [
  '/node_modules/maplibre-gl/dist/maplibre-gl-worker.mjs?worker_file&type=module',
  '/src/components/Map/MapLibreMap.tsx',
];

async function warmOne(origin: string, path: string): Promise<string> {
  const url = new URL(path, origin).toString();
  const started = Date.now();
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(180_000) });
    // Drain the body so Vite finishes the transform rather than aborting it.
    await response.arrayBuffer();
    return `${response.status} ${Date.now() - started}ms bytes=${response.headers.get('content-length') ?? 'unknown'} type=${response.headers.get('content-type') ?? 'unknown'} ${path}`;
  } catch (error) {
    return `skip ${Date.now() - started}ms ${path} (${(error as Error).message})`;
  }
}

function projectOrigins(config: FullConfig): string[] {
  const origins = new Set<string>();
  for (const project of config.projects) {
    const baseURL = (project.use as { baseURL?: unknown } | undefined)?.baseURL;
    if (typeof baseURL === 'string' && baseURL.length > 0) origins.add(baseURL);
  }
  return [...origins];
}

export default async function globalSetup(config: FullConfig): Promise<void> {
  const origins = projectOrigins(config);
  if (origins.length === 0) return;
  const results = await Promise.all(
    origins.flatMap((origin) => WARM_PATHS.map((path) => warmOne(origin, path))),
  );
  console.log(`[e2e] module warm-up — ${results.join(' | ')}`);
}
