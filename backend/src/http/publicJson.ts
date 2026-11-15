import type { Response } from 'express';

export const PUBLIC_REVALIDATE = 'private, no-cache, must-revalidate';

class PreparedJsonValue<T> {
  readonly #value: T;
  readonly #encodedBody: string;
  readonly #cloneForDelivery?: (value: T) => T;

  constructor(value: T, encodedBody: string, cloneForDelivery?: (value: T) => T) {
    this.#value = value;
    this.#encodedBody = encodedBody;
    this.#cloneForDelivery = cloneForDelivery;
  }

  send(res: Response): void {
    // Settings belong to this delivery. The original value never escapes to
    // a custom serializer when its representation owner supplied a clone.
    const defaultJson = res.app && ['json replacer', 'json spaces', 'json escape']
      .every(setting => res.app.get(setting) === undefined);
    if (defaultJson) {
      if (!res.get('Content-Type')) res.set('Content-Type', 'application/json');
      res.send(this.#encodedBody);
    } else {
      res.json(this.#cloneForDelivery ? this.#cloneForDelivery(this.#value) : this.#value);
    }
  }
}

/** The representation owner supplies both forms; ordinary payloads cannot
 * accidentally opt in by containing similarly named JSON properties. */
export function PreparedJson<T>(value: T, encodedBody: string, cloneForDelivery?: (value: T) => T): unknown {
  return Object.freeze(new PreparedJsonValue(value, encodedBody, cloneForDelivery));
}

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
  if (payload instanceof PreparedJsonValue) {
    // Express still owns UTF-8, length, ETag and HEAD/304 after both fences.
    payload.send(res);
  } else {
    res.json(payload);
  }
}
