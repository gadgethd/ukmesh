import { useEffect, useRef } from 'react';

type TimerHandle = ReturnType<typeof setTimeout>;

export type VisibilityPollerOptions = {
  poll: (signal: AbortSignal) => Promise<void>;
  intervalMs: number;
  timeoutMs: number;
  maxBackoffMs?: number;
  jitterRatio?: number;
  immediate?: boolean;
  isVisible: () => boolean;
  subscribeVisibility: (listener: () => void) => () => void;
  random?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => TimerHandle;
  clearTimer?: (handle: TimerHandle) => void;
  onError?: (error: unknown) => void;
};

export function createVisibilityPoller(options: VisibilityPollerOptions) {
  if (!Number.isFinite(options.intervalMs) || options.intervalMs <= 0) {
    throw new Error('intervalMs must be positive');
  }
  if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new Error('timeoutMs must be positive');
  }
  const setTimer = options.setTimer ?? ((callback, delay) => setTimeout(callback, delay));
  const clearTimer = options.clearTimer ?? ((handle) => clearTimeout(handle));
  const random = options.random ?? Math.random;
  const jitterRatio = Math.max(0, Math.min(0.5, options.jitterRatio ?? 0.1));
  const maxBackoffMs = Math.max(options.intervalMs, options.maxBackoffMs ?? options.intervalMs * 8);
  let stopped = false;
  let running = false;
  let pendingImmediate = false;
  let failures = 0;
  let timer: TimerHandle | null = null;
  let timeoutTimer: TimerHandle | null = null;
  let controller: AbortController | null = null;

  const clearScheduled = () => {
    if (timer !== null) clearTimer(timer);
    timer = null;
  };

  const schedule = (failed: boolean) => {
    clearScheduled();
    if (stopped || !options.isVisible()) return;
    failures = failed ? failures + 1 : 0;
    const base = Math.min(maxBackoffMs, options.intervalMs * (2 ** failures));
    const jitter = base * jitterRatio * ((random() * 2) - 1);
    const delay = Math.max(1, Math.round(base + jitter));
    timer = setTimer(() => {
      timer = null;
      void trigger();
    }, delay);
  };

  const trigger = async (): Promise<void> => {
    if (stopped) return;
    if (!options.isVisible()) {
      pendingImmediate = true;
      return;
    }
    if (running) {
      pendingImmediate = true;
      return;
    }
    clearScheduled();
    running = true;
    controller = new AbortController();
    const current = controller;
    timeoutTimer = setTimer(() => current.abort(new DOMException('Poll timed out', 'TimeoutError')), options.timeoutMs);
    let failed = false;
    try {
      await options.poll(current.signal);
      failed = current.signal.aborted;
    } catch (error) {
      failed = true;
      if (!current.signal.aborted) options.onError?.(error);
    } finally {
      if (timeoutTimer !== null) clearTimer(timeoutTimer);
      timeoutTimer = null;
      if (controller === current) controller = null;
      running = false;
    }
    if (stopped) return;
    if (pendingImmediate && options.isVisible()) {
      pendingImmediate = false;
      await trigger();
      return;
    }
    schedule(failed);
  };

  const onVisibilityChange = () => {
    if (!options.isVisible()) {
      clearScheduled();
      controller?.abort(new DOMException('Page hidden', 'AbortError'));
      pendingImmediate = true;
      return;
    }
    if (pendingImmediate || timer === null) {
      pendingImmediate = false;
      void trigger();
    }
  };
  const unsubscribe = options.subscribeVisibility(onVisibilityChange);

  if (options.immediate ?? true) void trigger();
  else schedule(false);

  return {
    trigger,
    stop: () => {
      if (stopped) return;
      stopped = true;
      clearScheduled();
      if (timeoutTimer !== null) clearTimer(timeoutTimer);
      timeoutTimer = null;
      controller?.abort(new DOMException('Poller stopped', 'AbortError'));
      controller = null;
      unsubscribe();
    },
    snapshot: () => ({
      running,
      scheduled: timer !== null,
      pendingImmediate,
      failures,
    }),
  };
}

export type UseVisibilityPollOptions = {
  enabled?: boolean;
  scopeKey: string;
  intervalMs: number;
  timeoutMs: number;
  maxBackoffMs?: number;
  jitterRatio?: number;
  immediate?: boolean;
  onError?: (error: unknown) => void;
};

export function useVisibilityPoll(
  poll: (signal: AbortSignal) => Promise<void>,
  options: UseVisibilityPollOptions,
): void {
  const pollRef = useRef(poll);
  const onErrorRef = useRef(options.onError);
  pollRef.current = poll;
  onErrorRef.current = options.onError;

  useEffect(() => {
    if (options.enabled === false) return undefined;
    const poller = createVisibilityPoller({
      poll: (signal) => pollRef.current(signal),
      intervalMs: options.intervalMs,
      timeoutMs: options.timeoutMs,
      maxBackoffMs: options.maxBackoffMs,
      jitterRatio: options.jitterRatio,
      immediate: options.immediate,
      isVisible: () => document.visibilityState === 'visible',
      subscribeVisibility: (listener) => {
        document.addEventListener('visibilitychange', listener);
        return () => document.removeEventListener('visibilitychange', listener);
      },
      onError: (error) => onErrorRef.current?.(error),
    });
    return () => poller.stop();
  }, [
    options.enabled,
    options.immediate,
    options.intervalMs,
    options.jitterRatio,
    options.maxBackoffMs,
    options.scopeKey,
    options.timeoutMs,
  ]);
}
