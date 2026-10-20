import { randomUUID } from 'node:crypto';
import { open, mkdir, readdir, readFile, rename, unlink } from 'node:fs/promises';
import path from 'node:path';

export type AlertReceipt = {
  received_at: string;
  source: 'alertmanager' | 'synthetic' | 'unknown';
  status: 'firing' | 'resolved' | 'recovery' | 'unknown';
  alert_names: string[];
  firing: number;
  resolved: number;
};

type DeliveryRecord = {
  id: string;
  receipt: AlertReceipt;
  createdAt: string;
  attempts: number;
  lastAttemptAt: string | null;
  nextAttemptAt: string;
  lastError: string | null;
};

type QueueMetadata = { lastSuccessAt: string | null };

export type AlertDeliveryQueueMetrics = {
  pendingCount: number;
  oldestQueueAgeSeconds: number;
  deadLetterCount: number;
  lastSuccessAt: string | null;
  lastError: string | null;
};

export class AlertQueueFullError extends Error {
  constructor() {
    super('alert forwarding queue is full');
    this.name = 'AlertQueueFullError';
  }
}

export class DurableAlertQueue {
  private readonly records = new Map<string, DeliveryRecord>();
  private initialized = false;
  private running = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private lastSuccessAt: string | null = null;
  private deadLetterCount = 0;
  private lastError: string | null = null;
  private enqueueTail: Promise<void> = Promise.resolve();
  private processing = false;

  constructor(private readonly options: {
    directory: string;
    maxItems: number;
    maxAttempts: number;
    backoffBaseMs: number;
    backoffCapMs: number;
    maxBatchSize?: number;
    now?: () => number;
    send: (receipt: AlertReceipt) => Promise<void>;
  }) {}

  async initialize(): Promise<void> {
    if (this.initialized) return;
    await mkdir(this.options.directory, { recursive: true, mode: 0o700 });
    const entries = await readdir(this.options.directory, { withFileTypes: true });
    const deadIds = new Set(
      entries
        .filter((entry) => entry.isFile() && entry.name.endsWith('.dead.json'))
        .map((entry) => entry.name.slice(0, -'.dead.json'.length)),
    );
    this.deadLetterCount = deadIds.size;

    try {
      const metadata = JSON.parse(
        await readFile(path.join(this.options.directory, 'queue-state.json'), 'utf8'),
      ) as QueueMetadata;
      if (typeof metadata.lastSuccessAt === 'string' || metadata.lastSuccessAt === null) {
        this.lastSuccessAt = metadata.lastSuccessAt;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        this.lastError = `could not read queue state: ${(error as Error).message}`;
      }
    }

    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith('.pending.json')) continue;
      const id = entry.name.slice(0, -'.pending.json'.length);
      const file = path.join(this.options.directory, entry.name);
      if (deadIds.has(id)) {
        await this.removeFile(file);
        continue;
      }
      try {
        const record = JSON.parse(await readFile(file, 'utf8')) as DeliveryRecord;
        if (
          record.id !== id
          || !record.receipt
          || !Array.isArray(record.receipt.alert_names)
          || !Number.isSafeInteger(record.attempts)
          || typeof record.createdAt !== 'string'
          || !Number.isFinite(Date.parse(record.createdAt))
          || typeof record.nextAttemptAt !== 'string'
          || !Number.isFinite(Date.parse(record.nextAttemptAt))
        ) {
          throw new Error('invalid queue record');
        }
        this.records.set(id, record);
      } catch (error) {
        const deadPath = this.deadPath(id);
        await rename(file, deadPath);
        this.deadLetterCount += 1;
        this.lastError = `quarantined invalid queue record ${id}: ${(error as Error).message}`;
      }
    }
    this.initialized = true;
  }

  start(): void {
    if (!this.initialized) throw new Error('alert queue has not been initialized');
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  stop(): void {
    this.running = false;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }

  metrics(now = this.now()): AlertDeliveryQueueMetrics {
    const records = [...this.records.values()];
    const oldest = records
      .reduce<number | null>((result, record) => {
        const createdAt = Date.parse(record.createdAt);
        return result === null || createdAt < result ? createdAt : result;
      }, null);
    const pendingError = records.find((record) => record.lastError !== null)?.lastError ?? null;
    return {
      pendingCount: records.length,
      oldestQueueAgeSeconds: oldest === null ? 0 : Math.max(0, Math.floor((now - oldest) / 1_000)),
      deadLetterCount: this.deadLetterCount,
      lastSuccessAt: this.lastSuccessAt,
      lastError: pendingError ?? this.lastError,
    };
  }

  async enqueue(receipt: AlertReceipt): Promise<void> {
    const previousEnqueue = this.enqueueTail;
    let release!: () => void;
    this.enqueueTail = new Promise<void>((resolve) => { release = resolve; });
    await previousEnqueue;
    try {
      if (!this.initialized) throw new Error('alert queue has not been initialized');
      if (this.records.size >= this.options.maxItems) throw new AlertQueueFullError();
      const now = this.now();
      const record: DeliveryRecord = {
        id: randomUUID(),
        receipt,
        createdAt: new Date(now).toISOString(),
        attempts: 0,
        lastAttemptAt: null,
        nextAttemptAt: new Date(now).toISOString(),
        lastError: null,
      };
      await this.writeAtomic(this.pendingPath(record.id), record);
      this.records.set(record.id, record);
      this.schedule(0);
    } finally {
      release();
    }
  }

  /** Process due alerts sequentially. The attempt is fsynced before sending. */
  async processDue(): Promise<number> {
    if (!this.initialized) throw new Error('alert queue has not been initialized');
    if (this.processing) return 0;
    this.processing = true;
    try {
      const currentTime = this.now();
      const batchSize = Math.max(1, this.options.maxBatchSize ?? 20);
      const due = [...this.records.values()]
        .filter((record) => Date.parse(record.nextAttemptAt) <= currentTime)
        .sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt))
        .slice(0, batchSize);

      for (const record of due) {
        const attemptAt = this.now();
        const attempts = record.attempts + 1;
        const backoff = Math.min(
          this.options.backoffCapMs,
          this.options.backoffBaseMs * (2 ** Math.max(0, attempts - 1)),
        );
        const attempted: DeliveryRecord = {
          ...record,
          attempts,
          lastAttemptAt: new Date(attemptAt).toISOString(),
          nextAttemptAt: new Date(attemptAt + backoff).toISOString(),
        };
        const pendingPath = this.pendingPath(record.id);
        await this.writeAtomic(pendingPath, attempted);
        this.records.set(record.id, attempted);

        try {
          await this.options.send(record.receipt);
        } catch (error) {
          const message = (error instanceof Error ? error.message : String(error)).slice(0, 500);
          const failed = { ...attempted, lastError: message };
          this.lastError = message;
          console.error(`[alert-receiver] forward attempt ${attempts}/${this.options.maxAttempts} failed: ${message}`);
          if (attempts >= this.options.maxAttempts) {
            await this.writeAtomic(this.deadPath(record.id), failed);
            this.records.delete(record.id);
            this.deadLetterCount += 1;
            console.error(`[alert-receiver] dead-lettered alert ${record.id} after ${attempts} attempts`);
            try {
              await this.removeFile(pendingPath);
            } catch (removeError) {
              this.lastError = `dead letter ${record.id} retained a pending file: ${(removeError as Error).message}`;
            }
          } else {
            await this.writeAtomic(pendingPath, failed);
            this.records.set(record.id, failed);
          }
          continue;
        }

        const deliveredAt = new Date(this.now()).toISOString();
        try {
          await this.writeAtomic(path.join(this.options.directory, 'queue-state.json'), {
            lastSuccessAt: deliveredAt,
          } satisfies QueueMetadata);
          await this.removeFile(pendingPath);
          this.records.delete(record.id);
          this.lastSuccessAt = deliveredAt;
          this.lastError = null;
        } catch (error) {
          // Keep the persisted pending record so a process restart or the next
          // retry delivers at least once if it is unclear whether the send landed.
          this.lastError = `could not finalize delivered alert ${record.id}: ${(error as Error).message}`;
        }
      }
      return due.length;
    } finally {
      this.processing = false;
      if (this.running) this.scheduleNext();
    }
  }

  private now(): number {
    return (this.options.now ?? Date.now)();
  }

  private pendingPath(id: string): string {
    return path.join(this.options.directory, `${id}.pending.json`);
  }

  private deadPath(id: string): string {
    return path.join(this.options.directory, `${id}.dead.json`);
  }

  private async writeAtomic(file: string, value: unknown): Promise<void> {
    const temporary = `${file}.${randomUUID()}.tmp`;
    try {
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(value)}\n`, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(temporary, file);
      await this.syncDirectory();
    } catch (error) {
      try {
        await unlink(temporary);
      } catch (cleanupError) {
        if ((cleanupError as NodeJS.ErrnoException).code !== 'ENOENT') {
          this.lastError = `could not clean temporary queue file: ${(cleanupError as Error).message}`;
        }
      }
      throw error;
    }
  }

  private async removeFile(file: string): Promise<void> {
    try {
      await unlink(file);
      await this.syncDirectory();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }

  private async syncDirectory(): Promise<void> {
    const handle = await open(this.options.directory, 'r');
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.processDue().catch((error: unknown) => {
        this.lastError = `queue worker failed: ${error instanceof Error ? error.message : String(error)}`;
        console.error('[alert-receiver] durable queue worker failed:', this.lastError);
        this.schedule(1_000);
      });
    }, Math.max(0, delayMs));
    this.timer.unref();
  }

  private scheduleNext(): void {
    const now = this.now();
    const nextAt = [...this.records.values()]
      .reduce<number | null>((result, record) => {
        const at = Date.parse(record.nextAttemptAt);
        return result === null || at < result ? at : result;
      }, null);
    if (nextAt === null) return;
    this.schedule(Math.max(0, nextAt - now));
  }
}
