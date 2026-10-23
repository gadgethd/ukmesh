// Background cache warmer for the owner dashboard "RX Strength by Last Hop" panel.
//
// Every pass it refreshes the last-hop series cached for each node of every
// active MQTT owner, for both the 7d and 30d windows, so any owner opening the
// dashboard — on either time window — hits a warm cache instead of paying for a
// cold scan on their own request. The pass is sequential with a small delay
// between nodes to keep database load gentle, and individual failures are
// logged and skipped so one bad node can never stall the whole pass.
export type OwnerLastHopPrewarmDeps = {
  listOwnerNodes: () => Promise<Array<{ mqttUsername: string; isActive: boolean; nodeIds: string[] }>>;
  refreshLastHop: (ownedNodeIds: string[], nodeId: string, windowDays: number, minAgeMs: number) => Promise<boolean>;
  intervalMs: number;
  nodeDelayMs: number;
  windows?: number[];
};

const DEFAULT_WINDOWS = [7, 30];
const INITIAL_DELAY_MS = 20_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}

export function createOwnerLastHopPrewarm(deps: OwnerLastHopPrewarmDeps): () => void {
  if (!Number.isFinite(deps.intervalMs) || deps.intervalMs <= 0) {
    console.warn('[owner-last-hop-prewarm] disabled (invalid interval)');
    return () => undefined;
  }

  const windows = deps.windows && deps.windows.length > 0 ? deps.windows : DEFAULT_WINDOWS;
  // Entries refreshed within half a pass interval are left alone; anything older
  // is rebuilt (incrementally when a previous series exists, fully when cold).
  const minAgeMs = Math.max(30_000, Math.floor(deps.intervalMs / 2));
  let stopped = false;
  let running = false;
  let initialTimer: NodeJS.Timeout | null = null;
  let intervalTimer: NodeJS.Timeout | null = null;

  async function runPass(): Promise<void> {
    if (stopped || running) return;
    running = true;
    const startedAt = Date.now();
    let owners = 0;
    let nodes = 0;
    let nodesTotal = 0;
    let refreshed = 0;
    let skipped = 0;
    let failed = 0;
    const logProgress = (stage: string) => console.log(`[owner-last-hop-prewarm] ${stage}`, {
      owners,
      nodes,
      nodesTotal,
      refreshed,
      skipped,
      failed,
      durationMs: Date.now() - startedAt,
    });
    try {
      const active = (await deps.listOwnerNodes()).filter((account) => account.isActive && account.nodeIds.length > 0);
      owners = active.length;
      nodesTotal = active.reduce((total, account) => total + account.nodeIds.length, 0);
      logProgress('pass started');
      for (const account of active) {
        if (stopped) break;
        for (const nodeId of account.nodeIds) {
          if (stopped) break;
          nodes += 1;
          for (const windowDays of windows) {
            try {
              const didRefresh = await deps.refreshLastHop(account.nodeIds, nodeId, windowDays, minAgeMs);
              if (didRefresh) refreshed += 1;
              else skipped += 1;
            } catch (error) {
              failed += 1;
              console.warn('[owner-last-hop-prewarm] refresh failed', {
                mqttUsername: account.mqttUsername,
                nodeId,
                windowDays,
                error: error instanceof Error ? error.message : String(error),
              });
            }
          }
          if (nodes % 10 === 0) logProgress('pass progress');
          if (deps.nodeDelayMs > 0) await sleep(deps.nodeDelayMs);
        }
      }
      logProgress('pass complete');
    } catch (error) {
      console.warn('[owner-last-hop-prewarm] pass failed', error instanceof Error ? error.message : error);
    } finally {
      running = false;
    }
  }

  console.log('[owner-last-hop-prewarm] enabled', {
    intervalMs: deps.intervalMs,
    nodeDelayMs: deps.nodeDelayMs,
    windows,
    minAgeMs,
  });
  initialTimer = setTimeout(() => { void runPass(); }, INITIAL_DELAY_MS);
  initialTimer.unref();
  intervalTimer = setInterval(() => { void runPass(); }, deps.intervalMs);
  intervalTimer.unref();

  return () => {
    stopped = true;
    if (initialTimer) clearTimeout(initialTimer);
    if (intervalTimer) clearInterval(intervalTimer);
    initialTimer = null;
    intervalTimer = null;
  };
}
