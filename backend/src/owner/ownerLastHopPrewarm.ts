// Background cache warmer for the owner dashboard "RX Strength by Last Hop" panel.
//
// Every pass it refreshes the last-hop series cached for each node of every
// active MQTT owner, for both the 7d and 30d windows, so any owner opening the
// dashboard — on either time window — hits a warm cache instead of paying for a
// cold scan on their own request. The pass defaults to sequential work with a
// small delay between nodes; optional concurrency is capped at two nodes.
// Windows for each node remain sequential, and individual failures are
// logged and skipped so one bad node can never stall the whole pass.
export type OwnerLastHopPrewarmDeps = {
  listOwnerNodes: () => Promise<Array<{ mqttUsername: string; isActive: boolean; nodeIds: string[] }>>;
  refreshLastHop: (ownedNodeIds: string[], nodeId: string, windowDays: number, minAgeMs: number) => Promise<boolean>;
  intervalMs: number;
  nodeDelayMs: number;
  concurrency?: number;
  windows?: number[];
};

const DEFAULT_WINDOWS = [7, 30];
const INITIAL_DELAY_MS = 20_000;
const PROGRESS_INTERVAL_MS = 30_000;
const MAX_CONCURRENCY = 2;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}

export function createOwnerLastHopPrewarm(deps: OwnerLastHopPrewarmDeps): () => void {
  if (!Number.isFinite(deps.intervalMs) || deps.intervalMs <= 0) {
    console.warn('[owner-last-hop-prewarm] disabled (invalid interval)');
    return () => undefined;
  }

  const windows = deps.windows && deps.windows.length > 0 ? deps.windows : DEFAULT_WINDOWS;
  const requestedConcurrency = deps.concurrency ?? 1;
  const concurrency = Number.isFinite(requestedConcurrency) && requestedConcurrency >= 1
    ? Math.min(MAX_CONCURRENCY, Math.floor(requestedConcurrency))
    : 1;
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
    let refreshed = 0;
    let skipped = 0;
    let failed = 0;
    let ownersCompleted = 0;
    let nodesCompleted = 0;
    let totalNodes = 0;
    let progressTimer: NodeJS.Timeout | null = null;
    function progress() {
      const durationMs = Math.max(0, Date.now() - startedAt);
      const totalRefreshes = totalNodes * windows.length;
      const completedRefreshes = refreshed + skipped + failed;
      const remainingRefreshes = totalRefreshes - completedRefreshes;
      const ratePerSecond = durationMs > 0 ? completedRefreshes * 1_000 / durationMs : 0;
      return {
        owners, ownersCompleted, nodes, nodesCompleted, totalNodes,
        totalRefreshes, completedRefreshes, remainingRefreshes,
        refreshed, skipped, failed, durationMs, ratePerSecond,
        etaSeconds: remainingRefreshes === 0 ? 0 : ratePerSecond > 0 ? remainingRefreshes / ratePerSecond : null,
        concurrency,
      };
    }
    try {
      const active = (await deps.listOwnerNodes()).filter((account) => account.isActive && account.nodeIds.length > 0);
      owners = active.length;
      const tasks = active.flatMap((account) => {
        const owner = { account, remainingNodes: account.nodeIds.length };
        return account.nodeIds.map((nodeId) => ({ owner, nodeId }));
      });
      totalNodes = tasks.length;
      console.log('[owner-last-hop-prewarm] pass started', progress());
      progressTimer = setInterval(() => {
        console.log('[owner-last-hop-prewarm] progress', progress());
      }, PROGRESS_INTERVAL_MS);
      progressTimer.unref();
      let nextNode = 0;
      async function runWorker(): Promise<void> {
        while (!stopped && nextNode < tasks.length) {
          const { owner, nodeId } = tasks[nextNode++]!;
          const { account } = owner;
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
          nodesCompleted += 1;
          owner.remainingNodes -= 1;
          if (owner.remainingNodes === 0) ownersCompleted += 1;
          if (deps.nodeDelayMs > 0) await sleep(deps.nodeDelayMs);
        }
      }
      await Promise.all(Array.from({ length: Math.min(concurrency, totalNodes) }, () => runWorker()));
      console.log(`[owner-last-hop-prewarm] pass ${stopped ? 'stopped' : 'complete'}`, progress());
    } catch (error) {
      console.warn('[owner-last-hop-prewarm] pass failed', {
        ...progress(), error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      if (progressTimer) clearInterval(progressTimer);
      running = false;
    }
  }

  console.log('[owner-last-hop-prewarm] enabled', {
    intervalMs: deps.intervalMs,
    nodeDelayMs: deps.nodeDelayMs,
    windows,
    minAgeMs,
    concurrency,
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
