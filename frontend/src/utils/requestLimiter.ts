export type RequestLimiterSnapshot = {
  active: number;
  queued: number;
  maxConcurrent: number;
  maxQueued: number;
  rejected: number;
};

type QueueJob = {
  signal: AbortSignal;
  started: boolean;
  task: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  cleanup: () => void;
};

export class RequestLimiter {
  readonly #maxConcurrent: number;
  readonly #maxQueued: number;
  readonly #queue: QueueJob[] = [];
  #active = 0;
  #rejected = 0;

  constructor(maxConcurrent: number, maxQueued: number) {
    if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent <= 0) {
      throw new Error('maxConcurrent must be a positive integer');
    }
    if (!Number.isSafeInteger(maxQueued) || maxQueued < 0) {
      throw new Error('maxQueued must be a non-negative integer');
    }
    this.#maxConcurrent = maxConcurrent;
    this.#maxQueued = maxQueued;
  }

  run<T>(signal: AbortSignal, task: () => Promise<T>): Promise<T> {
    if (signal.aborted) return Promise.reject(signal.reason);
    if (this.#active >= this.#maxConcurrent && this.#queue.length >= this.#maxQueued) {
      this.#rejected += 1;
      return Promise.reject(new Error('Request queue limit exceeded'));
    }

    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const settleResolve = (value: unknown) => {
        if (settled) return;
        settled = true;
        resolve(value as T);
      };
      const settleReject = (reason: unknown) => {
        if (settled) return;
        settled = true;
        reject(reason);
      };
      const job: QueueJob = {
        signal,
        started: false,
        task,
        resolve: settleResolve,
        reject: settleReject,
        cleanup: () => signal.removeEventListener('abort', onAbort),
      };
      const onAbort = () => {
        if (!job.started) {
          const index = this.#queue.indexOf(job);
          if (index >= 0) this.#queue.splice(index, 1);
        }
        job.cleanup();
        settleReject(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
      this.#queue.push(job);
      this.#drain();
    });
  }

  #drain(): void {
    while (this.#active < this.#maxConcurrent && this.#queue.length > 0) {
      const job = this.#queue.shift()!;
      if (job.signal.aborted) {
        job.cleanup();
        job.reject(job.signal.reason);
        continue;
      }
      job.started = true;
      this.#active += 1;
      void Promise.resolve()
        .then(job.task)
        .then(job.resolve, job.reject)
        .finally(() => {
          job.cleanup();
          this.#active -= 1;
          this.#drain();
        });
    }
  }

  snapshot(): RequestLimiterSnapshot {
    return {
      active: this.#active,
      queued: this.#queue.length,
      maxConcurrent: this.#maxConcurrent,
      maxQueued: this.#maxQueued,
      rejected: this.#rejected,
    };
  }
}
