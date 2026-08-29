export class PublicVisibilityChangedError extends Error {
  constructor() {
    super('PUBLIC_VISIBILITY_CHANGED_DURING_CACHE_LOAD');
    this.name = 'PublicVisibilityChangedError';
  }
}

/**
 * Publish or serve public cache data only when the privacy generation is
 * unchanged across the whole lookup/load. A single transition is retried;
 * repeated churn fails closed instead of returning a stale projection.
 */
export async function withStablePublicVisibility<T>(
  getGeneration: () => Promise<number>,
  load: (generation: number) => Promise<T>,
  maxAttempts = 2,
): Promise<T> {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const generation = await getGeneration();
    const result = await load(generation);
    if (await getGeneration() === generation) return result;
  }
  throw new PublicVisibilityChangedError();
}
