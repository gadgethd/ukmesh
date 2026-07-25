import { Worker } from 'worker_threads';

type JobCallback = {
  resolve: (result: unknown) => void;
  reject: (err: Error) => void;
  timeout?: NodeJS.Timeout;
};

type QueuedJob = {
  id: number;
  msg: object;
  cb: JobCallback;
};

export class WorkerPoolOverloadedError extends Error {
  constructor() {
    super('PATH_RESOLVE_OVERLOADED');
    this.name = 'WorkerPoolOverloadedError';
  }
}

export class WorkerPoolTimeoutError extends Error {
  constructor() {
    super('PATH_RESOLVE_TIMEOUT');
    this.name = 'WorkerPoolTimeoutError';
  }
}

/**
 * Small worker pool shared by HTTP path queries and best-effort MQTT warmups.
 * Interactive work is always dispatched before queued background work.
 */
export class WorkerPool {
  private idleWorkers: Worker[] = [];
  private activeWorkers = new Set<Worker>();
  private workerJobs = new Map<Worker, number>();
  private pendingJobs = new Map<number, JobCallback>();
  private interactiveQueue: QueuedJob[] = [];
  private backgroundQueue: QueuedJob[] = [];
  private jobId = 0;

  constructor(
    private readonly scriptUrl: URL,
    size = 2,
    private readonly maxBackgroundQueue = 128,
    private readonly maxInteractiveQueue = 32,
    private readonly jobTimeoutMs = 15_000,
  ) {
    for (let i = 0; i < size; i++) this.spawnWorker();
  }

  private spawnWorker(): void {
    const worker = new Worker(this.scriptUrl);
    this.activeWorkers.add(worker);

    worker.on('message', (msg: { id: number; ok: boolean; result?: unknown; error?: string }) => {
      if (!this.activeWorkers.has(worker)) return;
      const jobId = this.workerJobs.get(worker);
      if (jobId !== msg.id) return;

      this.workerJobs.delete(worker);
      const cb = this.pendingJobs.get(msg.id);
      this.pendingJobs.delete(msg.id);
      if (cb) {
        if (cb.timeout) clearTimeout(cb.timeout);
        if (msg.ok) cb.resolve(msg.result ?? null);
        else cb.reject(new Error(msg.error ?? 'Worker error'));
      }
      this.dispatchNext(worker);
    });

    worker.on('error', (err) => {
      console.error('[worker-pool] worker crashed:', err.message);
      this.retireWorker(worker, err);
    });

    worker.on('exit', (code) => {
      if (!this.activeWorkers.has(worker)) return;
      const error = new Error(`Worker exited with code ${code}`);
      if (code !== 0) console.error('[worker-pool]', error.message);
      this.retireWorker(worker, error);
    });

    this.dispatchNext(worker);
  }

  private retireWorker(worker: Worker, error: Error): void {
    if (!this.activeWorkers.delete(worker)) return;
    this.idleWorkers = this.idleWorkers.filter((idleWorker) => idleWorker !== worker);

    const jobId = this.workerJobs.get(worker);
    this.workerJobs.delete(worker);
    if (jobId != null) {
      const cb = this.pendingJobs.get(jobId);
      this.pendingJobs.delete(jobId);
      if (cb?.timeout) clearTimeout(cb.timeout);
      cb?.reject(error);
    }

    this.spawnWorker();
  }

  private dispatchNext(worker: Worker): void {
    if (!this.activeWorkers.has(worker)) return;
    const next = this.interactiveQueue.shift() ?? this.backgroundQueue.shift();
    if (!next) {
      this.idleWorkers.push(worker);
      return;
    }
    this.startJob(worker, next);
  }

  private startJob(worker: Worker, job: QueuedJob): void {
    this.workerJobs.set(worker, job.id);
    this.pendingJobs.set(job.id, job.cb);
    job.cb.timeout = setTimeout(() => {
      if (this.workerJobs.get(worker) !== job.id) return;
      this.retireWorker(worker, new WorkerPoolTimeoutError());
      void worker.terminate();
    }, this.jobTimeoutMs);
    job.cb.timeout.unref();
    try {
      worker.postMessage(job.msg);
    } catch (err) {
      this.workerJobs.delete(worker);
      this.pendingJobs.delete(job.id);
      if (job.cb.timeout) clearTimeout(job.cb.timeout);
      job.cb.reject(err instanceof Error ? err : new Error(String(err)));
      this.dispatchNext(worker);
    }
  }

  private enqueue<T>(msg: object, background: boolean): Promise<T> {
    if (
      (background && this.backgroundQueue.length >= this.maxBackgroundQueue)
      || (!background && this.interactiveQueue.length >= this.maxInteractiveQueue)
    ) {
      return Promise.reject(new WorkerPoolOverloadedError());
    }
    return new Promise<T>((resolve, reject) => {
      const id = ++this.jobId;
      const job: QueuedJob = {
        id,
        msg: { ...msg, id },
        cb: { resolve: resolve as (result: unknown) => void, reject },
      };
      const worker = this.idleWorkers.pop();
      if (worker) this.startJob(worker, job);
      else if (background) this.backgroundQueue.push(job);
      else this.interactiveQueue.push(job);
    });
  }

  run<T>(msg: object): Promise<T> {
    return this.enqueue<T>(msg, false);
  }

  /**
   * Queue best-effort work without allowing it to grow without bound or delay
   * queued interactive requests. A null result means the queue was full.
   */
  runBackground<T>(msg: object): Promise<T | null> {
    if (this.backgroundQueue.length >= this.maxBackgroundQueue) return Promise.resolve(null);
    return this.enqueue<T | null>(msg, true).catch((err: unknown) => {
      if (err instanceof WorkerPoolOverloadedError) return null;
      throw err;
    });
  }

  snapshot(): { active: number; interactiveQueued: number; backgroundQueued: number } {
    return {
      active: this.workerJobs.size,
      interactiveQueued: this.interactiveQueue.length,
      backgroundQueued: this.backgroundQueue.length,
    };
  }
}
