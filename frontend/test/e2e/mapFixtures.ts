import { readFileSync } from 'node:fs';
import type { Page } from '@playwright/test';

const TEST_MAP_STYLE = {
  version: 8,
  sources: {
    openmaptiles: {
      type: 'vector',
      url: 'https://tiles.openfreemap.org/planet',
    },
  },
  glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  layers: [
    { id: 'bg', type: 'background', paint: { 'background-color': '#080d14' } },
  ],
};

// 256px RGB Terrarium tile: (128, 0, 0) decodes to zero metres everywhere.
const FLAT_TERRAIN_TILE = readFileSync(new URL('./fixtures/flat-terrain.png', import.meta.url));
export const RF_COVERAGE_TILE = readFileSync(new URL('./fixtures/rf-coverage.png', import.meta.url));

export async function installMapRoutes(page: Page, delayPlanetMetadata = false) {
  await page.route('https://tiles.openfreemap.org/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname === '/styles/dark' || pathname === '/styles/positron') {
      return route.fulfill({ json: TEST_MAP_STYLE });
    }
    if (pathname === '/planet') {
      if (delayPlanetMetadata) {
        await new Promise((resolve) => setTimeout(resolve, 1_500));
      }
      // Keep metadata load/race behavior without depending on public tile servers.
      return route.fulfill({ json: {
        tilejson: '3.0.0',
        tiles: ['https://tiles.openfreemap.org/fixture/{z}/{x}/{y}.pbf'],
        minzoom: 0,
        maxzoom: 14,
        vector_layers: [],
      } });
    }
    if (pathname.startsWith('/fixture/') || pathname.startsWith('/fonts/')) {
      return route.fulfill({ contentType: 'application/x-protobuf', body: Buffer.alloc(0) });
    }
    return route.abort();
  });
}

export async function installTerrainRoutes(page: Page) {
  await page.route('**/terrain-tiles/**/*.png*', (route) => route.fulfill({
    contentType: 'image/png',
    body: FLAT_TERRAIN_TILE,
  }));
}
