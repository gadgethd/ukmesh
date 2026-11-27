const RECHECK_MS = 2_000;
const RESUME_MARGIN_BYTES = 512 * 1024 ** 2;

export function replayDiskReserve(minimumBytes, availableBytes, now = Date.now) {
  if (!Number.isSafeInteger(minimumBytes) || minimumBytes <= 0) {
    throw new Error('Invalid DEV archive replay disk reserve');
  }
  let lastCheck = -Infinity;
  let paused = false;
  return async () => {
    const time = now();
    if (time - lastCheck >= RECHECK_MS) {
      lastCheck = time;
      try {
        const free = await availableBytes();
        const required = minimumBytes + (paused ? RESUME_MARGIN_BYTES : 0);
        paused = !Number.isSafeInteger(free) || free < required;
      } catch {
        paused = true;
      }
    }
    return !paused;
  };
}
