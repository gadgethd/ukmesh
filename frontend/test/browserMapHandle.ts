import type { JSHandle, Page } from '@playwright/test';
import type { Map as MapLibreMap } from 'maplibre-gl';
import type { MeshNode } from '../src/types/domain.js';

async function reactMapRef<T>(page: Page, kind: 'renderer' | 'nodes'): Promise<JSHandle<T>> {
  // Inspect existing refs in the actual component, with no production globals.
  const handle = await page.evaluateHandle(kind => {
    type Hook = { memoizedState?: { current?: unknown }; next?: Hook };
    type Fiber = { memoizedState?: Hook; return?: Fiber };
    const element = document.querySelector('.maplibregl-map') as HTMLElement & Record<string, unknown>;
    let fiber = element[Object.keys(element).find(key => key.startsWith('__reactFiber'))!] as Fiber | undefined;
    while (fiber) {
      for (let hook = fiber.memoizedState; hook; hook = hook.next) {
        const value = hook.memoizedState?.current;
        if (kind === 'renderer' && value && typeof (value as { getStyle?: unknown }).getStyle === 'function') return value;
        if (kind === 'nodes' && value instanceof globalThis.Map && value.size > 0
          && typeof value.values().next().value?.node_id === 'string') return value;
      }
      fiber = fiber.return;
    }
    throw new Error(`Map ${kind} ref not found`);
  }, kind);
  return handle as JSHandle<T>;
}

export function mapHandle(page: Page): Promise<JSHandle<MapLibreMap>> {
  return reactMapRef<MapLibreMap>(page, 'renderer');
}

export function nodeMapHandle(page: Page): Promise<JSHandle<Map<string, MeshNode>>> {
  return reactMapRef<Map<string, MeshNode>>(page, 'nodes');
}
