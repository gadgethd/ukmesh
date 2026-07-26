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
    maxErrors?: number;
    now?: () => number;
    runId?: string;
  },
): Promise<BoundedRunResult<TOutput>> {
  const now = options.now ?? Date.now;
  const startedAtMs = now();
  const deadline = startedAtMs + Math.max(1, options.deadlineMs);
  const maxErrors = Math.max(1, options.maxErrors ?? 100);
  const results: TOutput[] = [];
  const errors: Array<{ index: number; message: string }> = [];
  let checkpoint = 0;
  let timedOut = false;

  for (let index = 0; index < items.length; index += 1) {
    if (now() >= deadline) {
      timedOut = true;
      break;
    }
    try {
      results.push(await work(items[index]!, index));
    } catch (error) {
      errors.push({
        index,
        message: error instanceof Error ? error.message : String(error),
      });
      if (errors.length >= maxErrors) {
        checkpoint = index + 1;
        break;
      }
    }
    checkpoint = index + 1;
  }

  const status: BoundedRunStatus = timedOut
    ? 'timed_out'
    : errors.length > 0
      ? (results.length > 0 ? 'partial' : 'failed')
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
