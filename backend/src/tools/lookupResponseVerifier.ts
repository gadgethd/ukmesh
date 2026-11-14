import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import type { LookupResponseEvidence, LookupResponseSpec } from './lookupResponseBody.js';
import { LOOKUP_RESPONSE_MEMO_COUNTER_NAMES, LOOKUP_RESPONSE_MEMO_MAX_ENTRIES } from './lookupResponseMemo.js';
import type { LookupResponseMemoStatistics } from './lookupResponseMemo.js';

type Pending = { resolve: (value: LookupResponseEvidence) => void; reject: (error: Error) => void;
  deadline: ReturnType<typeof setTimeout> };

export const LOOKUP_RESPONSE_VALIDATION_LIMITS = Object.freeze({
  workerCount: 1, maxPending: 8, maxBytes: LOOKUP_RESPONSE_MAX_BYTES, timeoutMs: 30_000,
  memoMaxEntries: LOOKUP_RESPONSE_MEMO_MAX_ENTRIES,
  resourceLimits: Object.freeze({ maxOldGenerationSizeMb: 128, maxYoungGenerationSizeMb: 16, stackSizeMb: 4 }),
});

/** One bounded observer owns validation; its CPU cannot stall the route event loop. */
export class LookupResponseVerifier {
  private readonly worker: Worker;
  private readonly pending = new Map<number, Pending>();
  private sequence = 0;
  private failure: Error | undefined;
  private termination: Promise<number> | undefined;
  private statisticsPending: { id: number; resolve: (value: LookupResponseMemoStatistics) => void;
    reject: (error: Error) => void; deadline: ReturnType<typeof setTimeout> } | undefined;

  constructor(worker?: Worker) {
    const source = fileURLToPath(import.meta.url).endsWith('.ts');
    this.worker = worker ?? new Worker(new URL(source ? './lookupResponseWorker.ts' : './lookupResponseWorker.js', import.meta.url), {
      execArgv: source ? ['--import', import.meta.resolve('tsx')] : [],
      env: {},
      resourceLimits: LOOKUP_RESPONSE_VALIDATION_LIMITS.resourceLimits,
    });
    this.worker.on('message', (message: unknown) => {
      if (!message || typeof message !== 'object' || !('id' in message)
        || !Number.isSafeInteger(message.id) || Number(message.id) < 1
        || ('error' in message && message.error !== undefined && typeof message.error !== 'string')) {
        this.fail(new Error('lookup response worker returned a malformed reply')); return;
      }
      const reply = message as { id: number; error?: string; result?: LookupResponseEvidence;
        statistics?: LookupResponseMemoStatistics };
      if (reply.id === this.statisticsPending?.id) {
        const job = this.statisticsPending; this.statisticsPending = undefined; clearTimeout(job.deadline);
        if (reply.error) { job.reject(new Error(reply.error)); return; }
        const stats = reply.statistics;
        if (!stats || LOOKUP_RESPONSE_MEMO_COUNTER_NAMES.some(name => !Number.isSafeInteger(stats[name]) || stats[name] < 0)
          || stats.entries > LOOKUP_RESPONSE_MEMO_MAX_ENTRIES || stats.maxEntries > LOOKUP_RESPONSE_MEMO_MAX_ENTRIES
          || stats.entries > stats.maxEntries || stats.freshHashes > stats.calls
          || stats.hits + stats.misses > stats.calls || stats.fullValidations !== stats.misses
          || stats.errors > stats.calls || stats.evictions > stats.misses) {
          const error = new Error('lookup response worker returned invalid statistics');
          job.reject(error); this.fail(error); return;
        }
        job.resolve({ ...stats }); return;
      }
      const job = this.pending.get(reply.id);
      if (!job) { this.fail(new Error('lookup response worker returned an unknown job')); return; }
      this.pending.delete(reply.id); clearTimeout(job.deadline);
      if (reply.error) { job.reject(new Error(reply.error)); return; }
      const result = reply.result;
      if (!result || !/^[a-f0-9]{64}$/.test(result.digest) || !/^[a-f0-9]{64}$/.test(result.dataDigest)
        || (result.entries !== null && (!Number.isSafeInteger(result.entries) || result.entries < 0))) {
        job.reject(new Error('lookup response worker returned invalid evidence'));
        this.fail(new Error('lookup response worker protocol failed')); return;
      }
      job.resolve(result);
    });
    this.worker.on('error', error => this.fail(new Error('lookup response worker failed', { cause: error })));
    this.worker.on('exit', code => this.fail(new Error(`lookup response worker exited (${code})`)));
  }

  verify(body: Buffer, spec: LookupResponseSpec): Promise<LookupResponseEvidence> {
    if (this.failure) return Promise.reject(this.failure);
    if (this.statisticsPending) return Promise.reject(new Error('lookup response statistics are in progress'));
    if (body.byteLength > LOOKUP_RESPONSE_MAX_BYTES) return Promise.reject(new Error('response byte budget exceeded'));
    if (this.pending.size >= LOOKUP_RESPONSE_VALIDATION_LIMITS.maxPending) return Promise.reject(new Error('lookup response validation queue is full'));
    const id = ++this.sequence;
    // A pooled Buffer may share a slab with unrelated bytes. Transfer only the
    // exact response slice; large standalone response buffers need no copy.
    const raw = body.buffer instanceof ArrayBuffer && body.byteOffset === 0 && body.byteLength === body.buffer.byteLength
      ? body.buffer : Uint8Array.from(body).buffer;
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(() => this.fail(new Error('lookup response validation deadline exceeded')), LOOKUP_RESPONSE_VALIDATION_LIMITS.timeoutMs);
      this.pending.set(id, { resolve, reject, deadline });
      try { this.worker.postMessage({ id, body: raw, spec }, [raw]); }
      catch (error) { this.fail(error instanceof Error ? error : new Error('lookup response worker submission failed')); }
    });
  }

  /** Collect counters after all timed requests, without extra per-response IPC. */
  readStatistics(): Promise<LookupResponseMemoStatistics> {
    if (this.failure) return Promise.reject(this.failure);
    if (this.pending.size || this.statisticsPending) return Promise.reject(new Error('lookup response statistics require an idle verifier'));
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(() => this.fail(new Error('lookup response statistics deadline exceeded')),
        LOOKUP_RESPONSE_VALIDATION_LIMITS.timeoutMs);
      this.statisticsPending = { id, resolve, reject, deadline };
      try { this.worker.postMessage({ id, command: 'statistics' }); }
      catch (error) { this.fail(error instanceof Error ? error : new Error('lookup response statistics submission failed')); }
    });
  }

  private fail(error: Error): void {
    if (this.failure) return;
    this.failure = error;
    for (const job of this.pending.values()) { clearTimeout(job.deadline); job.reject(error); }
    this.pending.clear();
    if (this.statisticsPending) {
      clearTimeout(this.statisticsPending.deadline); this.statisticsPending.reject(error); this.statisticsPending = undefined;
    }
    void this.terminate().catch(() => undefined);
  }

  private terminate(): Promise<number> { return this.termination ??= this.worker.terminate(); }

  async close(): Promise<void> {
    this.fail(new Error('lookup response verifier closed'));
    await this.terminate();
  }
}
