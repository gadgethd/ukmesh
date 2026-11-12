type Timer = ReturnType<typeof setTimeout>;

/** Coalesce connection-status renders; packet handling never goes through this
 * timer. The trailing publication retains the exact final reception timestamp. */
export function createMessageTimestampPublisher(publish: (timestamp: number) => void, options: {
  intervalMs: number;
  now?: () => number;
  setTimer?: (callback: () => void, delay: number) => Timer;
  clearTimer?: (timer: Timer) => void;
}) {
  if (!Number.isFinite(options.intervalMs) || options.intervalMs < 1) throw new Error('Invalid status interval');
  const now = options.now ?? (() => performance.now());
  const setTimer = options.setTimer ?? ((callback, delay) => setTimeout(callback, delay));
  const clearTimer = options.clearTimer ?? (timer => clearTimeout(timer));
  let timer: Timer | null = null;
  let latest: number | null = null;
  let published: number | null = null;
  let publishedAt = Number.NEGATIVE_INFINITY;
  let stopped = false;
  const flush = () => {
    if (timer !== null) clearTimer(timer);
    timer = null;
    if (stopped || latest === null || latest === published) return;
    publishedAt = now(); published = latest;
    publish(latest);
  };
  return {
    record: (timestamp: number) => {
      if (stopped || !Number.isFinite(timestamp)) return;
      latest = timestamp;
      const elapsed = now() - publishedAt;
      if (elapsed >= options.intervalMs) flush();
      else if (timer === null && latest !== published) timer = setTimer(flush, options.intervalMs - elapsed);
    },
    flush,
    stop: () => {
      stopped = true;
      if (timer !== null) clearTimer(timer);
      timer = null;
    },
  };
}
