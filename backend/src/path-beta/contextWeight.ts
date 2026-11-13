import type { BetaResolveContext } from './types.js';
import { LinkMetricsIndex } from './linkMetricsIndex.js';

export const PATH_CONTEXT_MAX_WEIGHT = 128 * 1024 * 1024;

/** Estimated retained bytes, including Map contents and shared object references.
 * This is a cache budget, not an exact V8 heap measurement. Stop once rejected.
 */
export function pathContextWeight(network: string, context: BetaResolveContext): number {
  const seen = new WeakSet<object>();
  let bytes = 0;
  const add = (value: number) => { bytes += value; };
  const visit = (value: unknown): void => {
    if (bytes > PATH_CONTEXT_MAX_WEIGHT) return;
    if (typeof value === 'string') { add(24 + value.length * 2); return; }
    if (value === null || typeof value !== 'object') { add(8); return; }
    if (seen.has(value)) return;
    seen.add(value);
    if (value instanceof LinkMetricsIndex) {
      add(512);
      for (const reference of value.retainedReferences()) visit(reference);
    } else if (ArrayBuffer.isView(value)) {
      add(64); visit(value.buffer);
    } else if (value instanceof ArrayBuffer || value instanceof SharedArrayBuffer) {
      add(32 + value.byteLength);
    } else if (value instanceof Map) {
      add(64 + value.size * 48);
      for (const [key, entry] of value) {
        visit(key); visit(entry);
        if (bytes > PATH_CONTEXT_MAX_WEIGHT) break;
      }
    } else if (Array.isArray(value)) {
      add(32 + value.length * 8);
      for (const entry of value) {
        visit(entry);
        if (bytes > PATH_CONTEXT_MAX_WEIGHT) break;
      }
    } else {
      add(64);
      for (const key in value) {
        if (!Object.hasOwn(value, key)) continue;
        add(24 + key.length * 2);
        visit((value as Record<string, unknown>)[key]);
        if (bytes > PATH_CONTEXT_MAX_WEIGHT) break;
      }
    }
  };
  visit(network);
  visit(context);
  return bytes;
}
