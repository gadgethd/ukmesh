import type { Response } from 'express';

export const PUBLIC_REVALIDATE = 'private, no-cache, must-revalidate';

/** Fence delivery as well as cache lookup. Express computes/compares the body ETag
 * only after both fresh authority reads succeed, including on conditional GETs. */
export async function publicJson(
  res: Response,
  readGeneration: () => Promise<number>,
  load: (generation: number) => Promise<unknown>,
  cacheControl: typeof PUBLIC_REVALIDATE | 'no-store' = PUBLIC_REVALIDATE,
): Promise<void> {
  res.setHeader('Cache-Control', 'no-store');
  const generation = await readGeneration();
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
  }
  const payload = await load(generation);
  if (await readGeneration() !== generation) throw new Error('PUBLIC_VISIBILITY_CHANGED');
  res.setHeader('Cache-Control', cacheControl);
  res.json(payload);
}
