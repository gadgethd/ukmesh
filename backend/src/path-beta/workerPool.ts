import { Worker } from 'worker_threads';

type JobCallback = {
  resolve: (result: unknown) => void;
  reject: (err: Error) => void;
};

type QueuedJob = {
  id: number;
  msg: object;
  cb: JobCallback;
};

export class WorkerPool {
  private idleWorkers: Worker[] = [];
  private pendingJobs = new Map<number, JobCallback>();
  private queue: QueuedJob[] = [];
  private jobId = 0;

  constructor(private readonly scriptUrl: URL, size = 2) {
    for (let i = 0; i < size; i++) this.spawnWorker();
  }

  private spawnWorker(): void {
    const worker = new Worker(this.scriptUrl);

    worker.on('message', (msg: { id: number; ok: boolean; result?: unknown; error?: string }) => {
      const cb = this.pendingJobs.get(msg.id);
      this.pendingJobs.delete(msg.id);
      if (cb) {
        if (msg.ok) cb.resolve(msg.result ?? null);
        else cb.reject(new Error(msg.error ?? 'Worker error'));
      }
      const next = this.queue.shift();
      if (next) {
        this.pendingJobs.set(next.id, next.cb);
        worker.postMessage(next.msg);
      } else {
        this.idleWorkers.push(worker);
      }
    });

    worker.on('error', (err) => {
      console.error('[worker-pool] worker crashed:', err.message);
      // Reject jobs that were in-flight on this worker is not possible without per-worker tracking.
      // Instead, just spawn a replacement — queue will drain normally.
      this.spawnWorker();
    });

    this.idleWorkers.push(worker);
  }

  run<T>(msg: object): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const id = ++this.jobId;
      const cb: JobCallback = { resolve: resolve as (r: unknown) => void, reject };
      const worker = this.idleWorkers.pop();
      if (worker) {
        this.pendingJobs.set(id, cb);
        worker.postMessage({ ...msg, id });
      } else {
        this.queue.push({ id, msg: { ...msg, id }, cb });
      }
    });
  }
}
