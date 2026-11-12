import { query } from '../db/runtime.js';
import { readPublicResponseGeneration } from './responseGeneration.js';

export async function readReadyVisibilityGeneration(): Promise<number> {
  return readPublicResponseGeneration(query);
}

export async function readCurrentVisibilityGeneration(signal?: AbortSignal): Promise<number> {
  const result = await query<{ generation: string }>(
    `SELECT generation::text AS generation
       FROM ops.public_visibility_state
      WHERE singleton = TRUE`,
    undefined,
    signal,
  );
  const generation = Number(result.rows[0]?.generation);
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
  }
  return generation;
}
