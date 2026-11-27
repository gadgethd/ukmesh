import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { open, mkdir, readdir, link, rename, unlink } from 'node:fs/promises';
import path from 'node:path';

export type AlertReceipt = {
  received_at: string;
  source: 'alertmanager' | 'synthetic' | 'unknown';
  status: 'firing' | 'resolved' | 'recovery' | 'unknown';
  alert_names: string[];
  firing: number;
  resolved: number;
};

type RedriveRequest = {
  requestId: string;
  reason: string;
  requestedAt: string;
  originalSha256: string;
};

type Resolution = RedriveRequest & {
  id: string;
  deliveredAt: string;
  attempts: number;
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const isDate = (value: unknown): value is string => typeof value === 'string' && Number.isFinite(Date.parse(value));

export class AlertRedriveError extends Error {
  constructor(public readonly statusCode: 400 | 404 | 409 | 503, message: string) {
    super(message);
    this.name = 'AlertRedriveError';
  }
}

type DeliveryRecord = {
  id: string;
  receipt: AlertReceipt;
  createdAt: string;
  attempts: number;
  lastAttemptAt: string | null;
  nextAttemptAt: string;
  lastError: string | null;
  redrive?: RedriveRequest;
};

type QueueMetadata = { lastSuccessAt: string | null };

export type AlertDeliveryQueueMetrics = {
  pendingCount: number;
  oldestQueueAgeSeconds: number;
  deadLetterCount: number;
  unresolvedDeadLetterCount: number;
  resolvedDeadLetterCount: number;
  lastSuccessAt: string | null;
  lastError: string | null;
  enqueuedTotal: number;
  forwardedTotal: number;
  failedTotal: number;
  deadLetteredTotal: number;
};

export class AlertQueueFullError extends Error {
  constructor() {
    super('alert forwarding queue is full');
    this.name = 'AlertQueueFullError';
  }
}

export class DurableAlertQueue {
  private readonly records = new Map<string, DeliveryRecord>();
  private readonly resolutions = new Map<string, Resolution>();
  private initialized = false;
  private running = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private lastSuccessAt: string | null = null;
  private readonly deadIds = new Set<string>();
  private lastError: string | null = null;
  private enqueueTail: Promise<void> = Promise.resolve();
  private processing = false;
  private closing = false;
  private admissionFault: string | null = null;
  private enqueuedTotal = 0;
  private forwardedTotal = 0;
  private failedTotal = 0;
  private deadLetteredTotal = 0;

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
    const deadIds = new Set(entries.filter((entry) => entry.name.endsWith('.dead.json'))
      .map((entry) => entry.name.slice(0, -'.dead.json'.length)));
    for (const id of deadIds) this.deadIds.add(id);

    try {
      const metadata = JSON.parse((await this.readRegular(path.join(this.options.directory, 'queue-state.json'))).toString()) as QueueMetadata;
      if (metadata.lastSuccessAt === null || isDate(metadata.lastSuccessAt)) this.lastSuccessAt = metadata.lastSuccessAt;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        this.lastError = `could not read queue state: ${(error as Error).message}`;
      }
    }

    for (const entry of entries.filter((item) => item.name.endsWith('.resolved.json'))) {
      const id = entry.name.slice(0, -'.resolved.json'.length);
      if (!UUID.test(id) || !deadIds.has(id)) throw new Error('invalid dead-letter resolution identity');
      const resolution = await this.readResolution(id);
      if (!resolution) throw new Error('missing dead-letter resolution');
      await this.syncDirectory();
      this.acceptResolution(resolution);
    }
    for (const entry of entries) {
      if (!entry.name.endsWith('.pending.json')) continue;
      const replay = entry.name.endsWith('.redrive.pending.json');
      const id = entry.name.slice(0, -(replay ? '.redrive.pending.json' : '.pending.json').length);
      if (!UUID.test(id) || !entry.isFile()) throw new Error('invalid pending queue file');
      const file = path.join(this.options.directory, entry.name);
      if (!replay && deadIds.has(id)) {
        await this.removeFile(file);
        continue;
      }
      let record: DeliveryRecord;
      try {
        record = JSON.parse((await this.readRegular(file)).toString()) as DeliveryRecord;
        this.validateRecord(record, id);
      } catch (error) {
        // Redrive audit corruption must never replace the immutable original.
        if (replay) throw error;
        await rename(file, this.deadPath(id));
        await this.syncDirectory();
        this.deadIds.add(id);
        this.deadLetteredTotal += 1;
        this.lastError = `quarantined invalid queue record ${id}: ${(error as Error).message}`;
        continue;
      }
      if (replay) {
        if (!record.redrive || !deadIds.has(id)) throw new Error('redrive is missing its original dead letter');
        await this.validateOriginal(id, record.redrive.originalSha256);
        const resolution = this.resolutions.get(id);
        if (resolution) {
          if (!this.sameRequest(resolution, record.redrive) || resolution.attempts !== record.attempts) throw new Error('resolution does not match pending redrive');
          await this.removeFile(file);
          continue;
        }
        if (await this.readOptional(this.failedPath(id, record.redrive.requestId))) {
          await this.validateFailed(record);
          await this.removeFile(file);
          continue;
        }
      } else if (record.redrive) throw new Error('redrive has an ordinary pending filename');
      this.records.set(id, record);
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

  async close(timeoutMs = 12_000): Promise<void> {
    this.stop();
    this.closing = true;
    await this.enqueueTail;
    const deadline = Date.now() + timeoutMs;
    while (this.processing && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    // Unfinished attempts remain fsynced and will replay after restart.
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
      deadLetterCount: this.deadIds.size,
      unresolvedDeadLetterCount: this.deadIds.size - this.resolutions.size,
      resolvedDeadLetterCount: this.resolutions.size,
      lastSuccessAt: this.lastSuccessAt,
      lastError: this.admissionFault ?? pendingError ?? this.lastError,
      enqueuedTotal: this.enqueuedTotal,
      forwardedTotal: this.forwardedTotal,
      failedTotal: this.failedTotal,
      deadLetteredTotal: this.deadLetteredTotal,
    };
  }

  async enqueue(receipt: AlertReceipt): Promise<void> {
    await this.admit(async () => {
      this.assertOpen();
      if (this.records.size >= this.options.maxItems) throw new AlertQueueFullError();
      const record = this.newRecord(randomUUID(), receipt);
      await this.persistAdmission(record);
    });
  }

  async redrive(id: string, requestId: string, reason: string): Promise<{ state: 'queued' | 'resolved' }> {
    if (!UUID.test(id) || !UUID.test(requestId) || typeof reason !== 'string'
      || !reason.trim() || reason.length > 200 || /[\x00-\x1f\x7f]/.test(reason)) {
      throw new AlertRedriveError(400, 'provide canonical UUIDs and a reason of 1–200 printable characters');
    }
    return this.admit(async () => {
      this.assertOpen();
      const original = await this.readOptional(this.deadPath(id));
      if (!original) throw new AlertRedriveError(404, 'dead letter not found');
      const digest = createHash('sha256').update(original).digest('hex');
      const resolution = this.resolutions.get(id);
      if (resolution) {
        if (resolution.originalSha256 !== digest) throw new Error('original dead letter does not match its audit digest');
        if (resolution.requestId === requestId) return { state: 'resolved' };
        throw new AlertRedriveError(409, 'dead letter has already been resolved');
      }
      const active = this.records.get(id);
      if (active) {
        if (!active.redrive) throw new AlertRedriveError(409, 'original delivery is still being finalized');
        if (active.redrive.originalSha256 !== digest) throw new Error('original dead letter does not match its audit digest');
        if (active.redrive.requestId === requestId) {
          await this.syncDirectory();
          return { state: 'queued' };
        }
        throw new AlertRedriveError(409, 'another redrive is pending');
      }
      if (await this.readOptional(this.failedPath(id, requestId))) {
        throw new AlertRedriveError(409, 'this redrive request has exhausted its retry budget');
      }
      const retained = await this.readOptional(path.join(this.options.directory, `${id}.redrive.pending.json`));
      if (retained) {
        const pending = JSON.parse(retained.toString()) as DeliveryRecord;
        this.validateRecord(pending, id);
        if (pending.redrive?.originalSha256 !== digest) throw new Error('original dead letter does not match pending redrive');
        if (pending.redrive.requestId !== requestId) throw new AlertRedriveError(409, 'another redrive is pending');
        if (this.records.size >= this.options.maxItems) throw new AlertQueueFullError();
        await this.syncDirectory();
        this.records.set(id, pending);
        this.schedule(0);
        return { state: 'queued' };
      }
      if (this.records.size >= this.options.maxItems) throw new AlertQueueFullError();
      const source = JSON.parse(original.toString()) as DeliveryRecord;
      this.validateRecord(source, id);
      if (source.redrive) throw new Error('original dead letter contains a redrive');
      const record = this.newRecord(id, source.receipt);
      record.redrive = { requestId, reason, requestedAt: record.createdAt, originalSha256: digest };
      // Never replace an unaccounted pending intent after a storage failure.
      await this.persistAdmission(record, true);
      return { state: 'queued' };
    });
  }

  private async admit<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.enqueueTail;
    let release!: () => void;
    this.enqueueTail = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try { return await operation(); } finally { release(); }
  }

  private assertOpen(): void {
    if (!this.initialized) throw new Error('alert queue has not been initialized');
    if (this.closing) throw new AlertRedriveError(503, 'alert forwarding queue is closing');
    if (this.admissionFault) throw new AlertRedriveError(503, 'queue storage requires recovery before accepting work');
  }

  private async persistAdmission(record: DeliveryRecord, immutable = false): Promise<void> {
    try {
      if (immutable) await this.writeImmutable(this.pendingPath(record), record);
      else await this.writeAtomic(this.pendingPath(record), record);
    } catch (error) {
      // Rename/link may have succeeded before directory fsync failed. Keep that
      // intent inside the same capacity/ID bound; sending still fsyncs its attempt.
      try {
        const retained = await this.readOptional(this.pendingPath(record));
        if (retained) {
          if (!retained.equals(Buffer.from(`${JSON.stringify(record)}\n`))) throw new Error('unexpected pending intent');
          this.records.set(record.id, record);
          this.schedule(1_000);
        }
      } catch {
        this.admissionFault = 'could not inspect an uncertain queue admission; restart after storage recovery';
      }
      this.lastError = 'could not confirm durable queue admission';
      throw error;
    }
    this.records.set(record.id, record);
    this.enqueuedTotal += 1;
    this.schedule(0);
  }

  private newRecord(id: string, receipt: AlertReceipt): DeliveryRecord {
    const at = new Date(this.now()).toISOString();
    return { id, receipt, createdAt: at, attempts: 0, lastAttemptAt: null, nextAttemptAt: at, lastError: null };
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
        if (this.closing) break;
        if (record.redrive) {
          const resolution = await this.readResolution(record.id);
          if (resolution) {
            if (!this.sameRequest(resolution, record.redrive) || resolution.attempts !== record.attempts) throw new Error('resolution does not match pending redrive');
            await this.syncDirectory();
            this.acceptResolution(resolution);
            await this.finalizeDelivery(record, resolution.deliveredAt);
            continue;
          }
        }
        // A crashed reserved attempt is ambiguous, but cannot grant extra tries.
        if (record.attempts >= this.options.maxAttempts) {
          await this.exhaust(record, record.lastError ?? 'retry budget exhausted before delivery could be confirmed');
          continue;
        }
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
        const pendingPath = this.pendingPath(record);
        await this.writeAtomic(pendingPath, attempted);
        this.records.set(record.id, attempted);

        try {
          await this.options.send(record.receipt);
        } catch (error) {
          const message = (error instanceof Error ? error.message : String(error)).slice(0, 500);
          const failed = { ...attempted, lastError: message };
          this.lastError = message;
          this.failedTotal += 1;
          console.error(`[alert-receiver] forward attempt ${attempts}/${this.options.maxAttempts} failed: ${message}`);
          if (attempts >= this.options.maxAttempts) {
            await this.exhaust(failed, message);
          } else {
            await this.writeAtomic(pendingPath, failed);
            this.records.set(record.id, failed);
          }
          continue;
        }

        this.forwardedTotal += 1;
        const deliveredAt = new Date(this.now()).toISOString();
        try {
          if (attempted.redrive) {
            const resolution: Resolution = { ...attempted.redrive, id: record.id, deliveredAt, attempts };
            // The original remains immutable. Resolution must survive restart
            // before pending cleanup can erase evidence of the confirmed send.
            await this.writeImmutable(this.resolutionPath(record.id), resolution);
            this.acceptResolution(resolution);
          }
          await this.finalizeDelivery(attempted, deliveredAt);
        } catch (error) {
          // Without durable resolution an ambiguous external success is retried
          // at least once, within the original reserved attempt budget.
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

  private pendingPath(record: DeliveryRecord): string {
    return path.join(this.options.directory, `${record.id}${record.redrive ? '.redrive' : ''}.pending.json`);
  }

  private deadPath(id: string): string {
    return path.join(this.options.directory, `${id}.dead.json`);
  }

  private resolutionPath(id: string): string {
    return path.join(this.options.directory, `${id}.resolved.json`);
  }

  private failedPath(id: string, requestId: string): string {
    return path.join(this.options.directory, `${id}.redrive.${requestId}.failed.json`);
  }

  private validateRecord(record: DeliveryRecord, id: string): void {
    if (!record || record.id !== id || !record.receipt || !Array.isArray(record.receipt.alert_names)
      || !isDate(record.receipt.received_at)
      || !['alertmanager', 'synthetic', 'unknown'].includes(record.receipt.source)
      || !['firing', 'resolved', 'recovery', 'unknown'].includes(record.receipt.status)
      || record.receipt.alert_names.length > 50
      || record.receipt.alert_names.some((name) => typeof name !== 'string' || name.length > 120)
      || !Number.isSafeInteger(record.receipt.firing) || record.receipt.firing < 0
      || !Number.isSafeInteger(record.receipt.resolved) || record.receipt.resolved < 0
      || !Number.isSafeInteger(record.attempts) || record.attempts < 0
      || !isDate(record.createdAt) || !isDate(record.nextAttemptAt)) throw new Error('invalid queue record');
    if (record.redrive) this.validateRequest(record.redrive);
  }

  private sameRequest(left: RedriveRequest, right: RedriveRequest): boolean {
    return left.requestId === right.requestId && left.reason === right.reason
      && left.requestedAt === right.requestedAt && left.originalSha256 === right.originalSha256;
  }

  private validateRequest(request: RedriveRequest): void {
    if (!UUID.test(request.requestId) || !isDate(request.requestedAt) || !/^[0-9a-f]{64}$/.test(request.originalSha256)
      || typeof request.reason !== 'string' || !request.reason.trim() || request.reason.length > 200
      || /[\x00-\x1f\x7f]/.test(request.reason)) throw new Error('invalid redrive audit');
  }

  private async validateOriginal(id: string, digest: string): Promise<void> {
    if (createHash('sha256').update(await this.readRegular(this.deadPath(id))).digest('hex') !== digest) {
      throw new Error('original dead letter does not match its audit digest');
    }
  }

  private async readResolution(id: string): Promise<Resolution | null> {
    const bytes = await this.readOptional(this.resolutionPath(id));
    if (!bytes) return null;
    const resolution = JSON.parse(bytes.toString()) as Resolution;
    this.validateRequest(resolution);
    if (resolution.id !== id || !isDate(resolution.deliveredAt)
      || !Number.isSafeInteger(resolution.attempts) || resolution.attempts < 1) throw new Error('invalid delivery resolution');
    await this.validateOriginal(id, resolution.originalSha256);
    return resolution;
  }

  private acceptResolution(resolution: Resolution): void {
    this.resolutions.set(resolution.id, resolution);
    if (!this.lastSuccessAt || resolution.deliveredAt > this.lastSuccessAt) this.lastSuccessAt = resolution.deliveredAt;
  }

  private async validateFailed(record: DeliveryRecord): Promise<void> {
    const failed = JSON.parse((await this.readRegular(this.failedPath(record.id, record.redrive!.requestId))).toString()) as DeliveryRecord;
    this.validateRecord(failed, record.id);
    if (!failed.redrive || !this.sameRequest(failed.redrive, record.redrive!)
      || failed.attempts !== record.attempts) throw new Error('failed redrive audit does not match pending request');
  }

  private async exhaust(record: DeliveryRecord, message: string): Promise<void> {
    const failed = { ...record, lastError: message };
    this.records.set(record.id, failed);
    await this.writeImmutable(record.redrive ? this.failedPath(record.id, record.redrive.requestId) : this.deadPath(record.id), failed);
    if (!record.redrive && !this.deadIds.has(record.id)) {
      this.deadIds.add(record.id);
      this.deadLetteredTotal += 1;
    }
    await this.removeFile(this.pendingPath(record));
    this.records.delete(record.id);
  }

  private async finalizeDelivery(record: DeliveryRecord, deliveredAt: string): Promise<void> {
    await this.writeAtomic(path.join(this.options.directory, 'queue-state.json'), { lastSuccessAt: deliveredAt } satisfies QueueMetadata);
    await this.removeFile(this.pendingPath(record));
    this.records.delete(record.id);
    this.lastSuccessAt = deliveredAt;
    this.lastError = null;
  }

  private async readRegular(file: string): Promise<Buffer> {
    const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
      if (!(await handle.stat()).isFile()) throw new Error('queue state must be a regular file');
      return await handle.readFile();
    } finally { await handle.close(); }
  }

  private async readOptional(file: string): Promise<Buffer | null> {
    try { return await this.readRegular(file); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw error;
    }
  }

  private async writeImmutable(file: string, value: unknown): Promise<void> {
    const bytes = Buffer.from(`${JSON.stringify(value)}\n`);
    const existing = await this.readOptional(file);
    if (existing) {
      if (!existing.equals(bytes)) throw new Error('immutable queue audit already exists with different content');
      await this.syncDirectory();
      return;
    }
    await this.writeAtomic(file, value, true);
  }

  private async writeAtomic(file: string, value: unknown, exclusive = false): Promise<void> {
    const temporary = `${file}.${randomUUID()}.tmp`;
    try {
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(value)}\n`, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      if (exclusive) {
        await link(temporary, file);
        await unlink(temporary);
      } else await rename(temporary, file);
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
