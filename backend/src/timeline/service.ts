import { createActivityTimelineCache } from './snapshotCache.js';
import type { TimelineScope, createTimelineRepository } from './repository.js';

export function createTimelineService(repository: ReturnType<typeof createTimelineRepository>, readReadyGeneration: () => Promise<number>) {
  const cache = createActivityTimelineCache();
  return {
    get(scope: TimelineScope, generation: number) {
      const key = JSON.stringify([scope.network, scope.minutes, scope.bucketMinutes, generation]);
      return cache.get(key, generation, readReadyGeneration, () => repository.load(scope));
    },
    close: cache.shutdown,
  };
}
