export type WebSocketAdmissionLimits = {
  maxConnections: number;
  maxPendingHandshakes: number;
};

export function websocketAdmissionDecision(
  state: { activeConnections: number; pendingHandshakes: number },
  limits: WebSocketAdmissionLimits,
): { allowed: true } | { allowed: false; statusCode: 503; reason: string } {
  if (state.pendingHandshakes >= limits.maxPendingHandshakes) {
    return { allowed: false, statusCode: 503, reason: 'too many pending handshakes' };
  }
  if (state.activeConnections + state.pendingHandshakes >= limits.maxConnections) {
    return { allowed: false, statusCode: 503, reason: 'connection capacity reached' };
  }
  return { allowed: true };
}

export class BoundedTaskQueueFullError extends Error {
  constructor() {
    super('BOUNDED_TASK_QUEUE_FULL');
    this.name = 'BoundedTaskQueueFullError';
  }
}

export class BoundedAsyncGate {
  private active = 0;
  private readonly queue: Array<() => void> = [];

  constructor(
    private readonly concurrency: number,
    private readonly maxQueued: number,
  ) {
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error('INVALID_ASYNC_GATE_CONCURRENCY');
    }
    if (!Number.isInteger(maxQueued) || maxQueued < 0) {
      throw new Error('INVALID_ASYNC_GATE_QUEUE');
    }
  }

  run<T>(task: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const start = () => {
        this.active += 1;
        const finish = () => {
          this.active = Math.max(0, this.active - 1);
          this.queue.shift()?.();
        };
        task().then(
          (value) => {
            finish();
            resolve(value);
          },
          (err: unknown) => {
            finish();
            reject(err);
          },
        );
      };
      if (this.active < this.concurrency) {
        start();
      } else if (this.queue.length < this.maxQueued) {
        this.queue.push(start);
      } else {
        reject(new BoundedTaskQueueFullError());
      }
    });
  }

  stats(): { active: number; queued: number } {
    return { active: this.active, queued: this.queue.length };
  }
}
