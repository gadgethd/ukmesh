const INITIAL_RETRY_MS = 10_000;

export function createOwnerAclRuntime(input: {
  initialize: () => Promise<void>;
  reconcile: () => Promise<void>;
  refreshIntervalMs: () => number;
  reportError: (error: unknown) => void;
}) {
  let state: 'idle' | 'active' | 'stopped' = 'idle';
  let initialized = false;
  let timer: NodeJS.Timeout | undefined;
  let running: Promise<void> | undefined;
  let onFirstReconcile: (() => void) | undefined;

  function schedule() {
    if (state !== 'active') return;
    const delay = initialized ? input.refreshIntervalMs() : INITIAL_RETRY_MS;
    timer = setTimeout(refresh, delay);
    timer.unref();
  }

  function refresh() {
    if (state !== 'active') return;
    const work = async () => {
      if (!initialized) await input.initialize();
      await input.reconcile();
    };
    running = work().then(() => {
      if (initialized || state !== 'active') return;
      initialized = true;
      onFirstReconcile?.();
    }).catch(input.reportError).finally(schedule);
  }

  return {
    start(onReady?: () => void) {
      if (state !== 'idle') return;
      state = 'active';
      onFirstReconcile = onReady;
      refresh();
    },
    isAvailable: () => state === 'active' && initialized,
    async stop() {
      state = 'stopped';
      clearTimeout(timer);
      await running;
    },
  };
}
