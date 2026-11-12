import type { Page } from '@playwright/test';
import type { Map } from 'maplibre-gl';

export async function mapHandle(page: Page) {
  // Read the existing ref so the test exercises the real component and MapLibre
  // instance, without adding a production window global or mocking its events.
  return page.evaluateHandle(() => {
    type Hook = { memoizedState?: { current?: Map }; next?: Hook };
    type Fiber = { memoizedState?: Hook; return?: Fiber };
    const element = document.querySelector('.maplibregl-map') as HTMLElement & Record<string, unknown>;
    let fiber = element[Object.keys(element).find(key => key.startsWith('__reactFiber'))!] as Fiber | undefined;
    while (fiber) {
      for (let hook = fiber.memoizedState; hook; hook = hook.next) {
        const value = hook.memoizedState?.current;
        if (typeof value?.getStyle === 'function') return value;
      }
      fiber = fiber.return;
    }
    throw new Error('MapLibre map ref not found');
  });
}
