import { randomUUID } from 'node:crypto';

export type BoundedRunStatus = 'complete' | 'partial' | 'failed' | 'timed_out' | 'stale';

export type BoundedRunResult<T> = {
  runId: string;
  status: BoundedRunStatus;
  windowStart: Date;
  windowEnd: Date;
  startedAt: Date;
  completedAt: Date;
  checkpoint: number;
  totalItems: number;
  results: T[];
  errors: Array<{ index: number; message: string }>;
};

export async function runBoundedItems<TInput, TOutput>(
  items: readonly TInput[],
  work: (item: TInput, index: number) => Promise<TOutput>,
  options: {
    windowStart: Date;
    windowEnd: Date;
    deadlineMs: number;
    concurrency?: number;
    collectResults?: boolean;
    maxErrors?: number;
    now?: () => number;
    runId?: string;
  },
): Promise<BoundedRunResult<TOutput>> {
  const now = options.now ?? Date.now;
  const startedAtMs = now();
  const deadline = startedAtMs + Math.max(1, options.deadlineMs);
  const maxErrors = Math.max(1, options.maxErrors ?? 100);
  const indexedResults: Array<{ index: number; value: TOutput }> = [];
  const errors: Array<{ index: number; message: string }> = [];
  const requestedConcurrency = Number.isFinite(options.concurrency)
    ? Math.trunc(options.concurrency ?? 1)
    : 1;
  const concurrency = Math.max(1, Math.min(items.length || 1, requestedConcurrency));
  let nextIndex = 0;
  let checkpoint = 0;
  let successfulItems = 0;
  let timedOut = false;

  const worker = async () => {
    while (nextIndex < items.length && errors.length < maxErrors) {
      if (now() >= deadline) {
        timedOut = true;
        return;
      }
      const index = nextIndex;
      nextIndex += 1;
      try {
        const value = await work(items[index]!, index);
        successfulItems += 1;
        if (options.collectResults !== false) indexedResults.push({ index, value });
      } catch (error) {
        errors.push({
          index,
          message: error instanceof Error ? error.message : String(error),
        });
      }
      checkpoint += 1;
    }
  };
  await Promise.all(Array.from({ length: concurrency }, () => worker()));

  const results = indexedResults
    .sort((left, right) => left.index - right.index)
    .map((entry) => entry.value);
  const status: BoundedRunStatus = timedOut
    ? 'timed_out'
    : errors.length > 0
      ? (successfulItems > 0 ? 'partial' : 'failed')
      : checkpoint === items.length
        ? 'complete'
        : 'partial';
  return {
    runId: options.runId ?? randomUUID(),
    status,
    windowStart: options.windowStart,
    windowEnd: options.windowEnd,
    startedAt: new Date(startedAtMs),
    completedAt: new Date(now()),
    checkpoint,
    totalItems: items.length,
    results,
    errors,
  };
}
