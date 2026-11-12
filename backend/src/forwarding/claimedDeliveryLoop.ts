import { randomUUID } from 'node:crypto';
import { withDeadline } from '../platform/deadline.js';

/** Polling and claims are single-flight; authorization and repository transitions stay in each policy. */
export function createClaimedDeliveryLoop<Row>(deps: {
  pollIntervalMs: number;
  deliveryTimeoutMs: number;
  claim: (claimToken: string) => Promise<Row[]>;
  deliver: (row: Row, claimToken: string, signal: AbortSignal) => Promise<void>;
  deliveryLabel: (row: Row) => string;
  aborted: (row: Row, claimToken: string, error: unknown) => Promise<void>;
  failed: (error: unknown) => void;
  settled: () => void | Promise<void>;
}) {
  let timer: NodeJS.Timeout | null = null;
  let processing: Promise<void> | null = null;
  let stopping = false;

  async function processDue(): Promise<void> {
    if (stopping) return;
    if (processing) return processing;
    processing = (async () => {
      try {
        const claimToken = randomUUID();
        const rows = await deps.claim(claimToken);
        await Promise.all(rows.map(async row => {
          const cancellation = new AbortController();
          try {
            await withDeadline(deps.deliver(row, claimToken, cancellation.signal),
              deps.deliveryTimeoutMs, deps.deliveryLabel(row));
          } catch (error) {
            cancellation.abort();
            await deps.aborted(row, claimToken, error);
          }
        }));
      } catch (error) { deps.failed(error); }
      finally {
        try { await deps.settled(); }
        finally { processing = null; }
      }
    })();
    await processing;
  }

  return {
    processDue,
    start(): boolean {
      if (timer || stopping) return false;
      timer = setInterval(() => void processDue(), deps.pollIntervalMs);
      timer.unref();
      void processDue();
      return true;
    },
    async stop() {
      stopping = true;
      if (timer) clearInterval(timer);
      timer = null;
      if (processing) await processing.catch(() => undefined);
    },
  };
}
