import { createHash } from 'node:crypto';
import { LOOKUP_RESPONSE_MAX_BYTES, validateLookupResponse } from './lookupResponseBody.js';
import type { LookupResponseEvidence, LookupResponseSpec } from './lookupResponseBody.js';

export const LOOKUP_RESPONSE_MEMO_MAX_ENTRIES = 8;
export const LOOKUP_RESPONSE_MEMO_COUNTER_NAMES = Object.freeze([
  'calls', 'freshHashes', 'hits', 'misses', 'evictions', 'fullValidations', 'errors', 'entries', 'maxEntries',
] as const);
export type LookupResponseMemoStatistics = Record<typeof LOOKUP_RESPONSE_MEMO_COUNTER_NAMES[number], number>;

const primitiveKey = (value: number | boolean) => [typeof value,
  typeof value === 'number' && Object.is(value, -0) ? '-0' : String(value)];

/** Reuse successful pure evidence, while hashing and checking every response.
 * Keep body bytes, decoded strings and parsed JSON outside the retained memo. */
export function createLookupResponseMemo() {
  const results = new Map<string, Readonly<LookupResponseEvidence>>();
  const counters: LookupResponseMemoStatistics = { calls: 0, freshHashes: 0, hits: 0, misses: 0,
    evictions: 0, fullValidations: 0, errors: 0, entries: 0, maxEntries: 0 };
  return {
    statistics: (): LookupResponseMemoStatistics => ({ ...counters }),
    validate(body: ArrayBuffer, spec: LookupResponseSpec): LookupResponseEvidence {
      counters.calls++;
      try {
        if (body.byteLength > LOOKUP_RESPONSE_MAX_BYTES) throw new Error('response byte budget exceeded');
        const digest = createHash('sha256').update(Buffer.from(body)).digest('hex');
        counters.freshHashes++;
        if (spec.status !== spec.expectedStatus) throw new Error(`lookup response HTTP ${spec.status}`);
        const key = JSON.stringify([digest, body.byteLength, primitiveKey(spec.status),
          primitiveKey(spec.expectedStatus), primitiveKey(spec.populated)]);
        const previous = results.get(key);
        if (previous) { counters.hits++; return { ...previous }; }
        counters.misses++; counters.fullValidations++;
        // The original validator owns parsing, population and canonical hashes.
        // Its additional raw hash on a miss also verifies the memo's fresh key.
        const evidence = validateLookupResponse(body, spec);
        if (evidence.digest !== digest) throw new Error('lookup response digest disagreement');
        if (results.size === LOOKUP_RESPONSE_MEMO_MAX_ENTRIES) {
          results.delete(results.keys().next().value!); counters.evictions++;
        }
        results.set(key, Object.freeze({ ...evidence }));
        counters.entries = results.size; counters.maxEntries = Math.max(counters.maxEntries, results.size);
        return { ...evidence };
      } catch (error) { counters.errors++; throw error; }
    },
  };
}
