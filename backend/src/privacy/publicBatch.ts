import type { PrivatePrefixCache } from './privatePrefixCache.js';

/** A bounded FIFO with one current-authority query per delivery batch. */
export function createPublicBatch<T>(options: {
  loadPolicy: () => Promise<PrivatePrefixCache | null>;
  deliver: (items: T[], policy: PrivatePrefixCache) => void;
  refused: (reason: 'overflow' | 'unavailable', count: number) => void;
  maxItems: number;
  maxBytes: number;
  delayMs?: number;
}) {
  let pending: Array<{ item: T; bytes: number }> = [];
  let bytes = 0, count = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let active: Promise<void> | null = null;
  let closed = false;
  const schedule = () => {
    if (!closed && !timer && !active && pending.length) {
      timer = setTimeout(() => { timer = null; void flush(); }, options.delayMs ?? 16);
    }
  };
  const flush = (): Promise<void> => {
    if (active) return active;
    if (!pending.length || closed) return Promise.resolve();
    if (timer) { clearTimeout(timer); timer = null; }
    const batch = pending; pending = [];
    const batchBytes = batch.reduce((sum, value) => sum + value.bytes, 0);
    const work = (async () => {
      try {
        const policy = await options.loadPolicy();
        if (!closed) {
          if (policy) options.deliver(batch.map(({ item }) => item), policy);
          else options.refused('unavailable', batch.length);
        }
      } catch {
        if (!closed) options.refused('unavailable', batch.length);
      } finally {
        bytes -= batchBytes; count -= batch.length;
      }
    })().finally(() => { active = null; schedule(); });
    active = work;
    return work;
  };
  return {
    enqueue(item: T, itemBytes: number): boolean {
      if (closed || !Number.isSafeInteger(itemBytes) || itemBytes < 0
        || count >= options.maxItems || bytes + itemBytes > options.maxBytes) {
        options.refused('overflow', 1); return false;
      }
      pending.push({ item, bytes: itemBytes }); bytes += itemBytes; count++;
      schedule(); return true;
    },
    flush,
    close(): Promise<void> {
      closed = true;
      if (timer) { clearTimeout(timer); timer = null; }
      bytes -= pending.reduce((sum, value) => sum + value.bytes, 0);
      count -= pending.length; pending = [];
      return active ?? Promise.resolve();
    },
    size: () => ({ count, bytes }),
  };
}
