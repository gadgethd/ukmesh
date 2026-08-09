export type DeferredOnce = {
  register(start: () => void): void;
  start(): boolean;
};

/**
 * Registers a lifecycle callback during module wiring, then starts it once the
 * application has completed the prerequisite initialization phase.
 */
export function createDeferredOnce(name: string): DeferredOnce {
  let callback: (() => void) | null = null;
  let started = false;

  return {
    register(start) {
      if (started || callback) {
        throw new Error(`${name} is already registered`);
      }
      callback = start;
    },
    start() {
      if (started) return false;
      if (!callback) {
        throw new Error(`${name} is not registered`);
      }
      const start = callback;
      callback = null;
      started = true;
      start();
      return true;
    },
  };
}
