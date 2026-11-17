import express, { type Response } from 'express';

type EtagGenerator = (body: Buffer, encoding?: string) => string;
const ordinarySend = express.response.send;
const ordinarySet = express.response.set;
const ordinarySetHeader = express.response.setHeader;
const ordinaryGet = express.response.get;
const ordinaryGetHeader = express.response.getHeader;
const ordinaryHasHeader = express.response.hasHeader;
const ordinaryApplicationGet = express.application.get;
const builtinEtagGenerators = new Set<EtagGenerator>(['weak', 'strong'].map(mode => {
  const app = express();
  app.set('etag', mode);
  return app.get('etag fn') as EtagGenerator;
}));

export const PUBLIC_REVALIDATE = 'private, no-cache, must-revalidate';

class PreparedJsonValue<T> {
  readonly #value: T;
  readonly #encodedBody: string;
  #lastEtag?: Readonly<{ generator: EtagGenerator; value: string }>;
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
    const generator = res.app?.get === ordinaryApplicationGet
      ? res.app.get('etag fn') as EtagGenerator | undefined : undefined;
    if (defaultJson && res.send === ordinarySend && res.set === ordinarySet && res.setHeader === ordinarySetHeader
      && res.get === ordinaryGet && res.getHeader === ordinaryGetHeader && res.hasHeader === ordinaryHasHeader
      && !res.hasHeader('Content-Type') && !res.hasHeader('ETag')
      && typeof generator === 'function' && builtinEtagGenerators.has(generator)) {
      // Express hashes these same UTF-8 bytes on every ordinary string send.
      // Keep only its last builtin validator; every delivery owns fresh bytes.
      const body = Buffer.from(this.#encodedBody, 'utf8');
      if (!this.#lastEtag || this.#lastEtag.generator !== generator) {
        this.#lastEtag = Object.freeze({ generator, value: generator(body) });
      }
      res.set('Content-Type', 'application/json; charset=utf-8');
      res.set('ETag', this.#lastEtag.value);
      res.send(body);
    } else if (defaultJson) {
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

/** Fence delivery as well as cache lookup. Validator selection and Express's
 * freshness comparison follow both authority reads, including conditional GETs. */
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
    // Express still owns length, freshness and HEAD/304 after both fences.
    payload.send(res);
  } else {
    res.json(payload);
  }
}
