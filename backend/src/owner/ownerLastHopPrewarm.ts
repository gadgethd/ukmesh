export type OwnerLastHopPrewarmTarget = {
  mqttUsername: string;
  nodeIds: string[];
};

export type OwnerLastHopPrewarmProgress = {
  owners: number;
  ownersTotal: number;
  nodes: number;
  nodesTotal: number;
  refreshed: number;
  failed: number;
};

export function ownerLastHopPrewarmConcurrency(value: unknown): number {
  if (value == null || value === '') return 2;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(1, Math.min(2, Math.trunc(parsed))) : 2;
}

type PrewarmDeps = {
  loadOwners: () => Promise<OwnerLastHopPrewarmTarget[]>;
  refresh: (owner: OwnerLastHopPrewarmTarget, nodeId: string) => Promise<unknown>;
  concurrency?: unknown;
  now?: () => number;
  log?: Pick<Console, 'log' | 'warn'>;
};

/** One pass at a time, at most two node refreshes, and no overlapping schedules. */
export function createOwnerLastHopPrewarm(deps: PrewarmDeps) {
  const concurrency = ownerLastHopPrewarmConcurrency(deps.concurrency);
  const now = deps.now ?? Date.now;
  const log = deps.log ?? console;
  let inFlight: Promise<OwnerLastHopPrewarmProgress> | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let started = false;
  let stopped = false;

  async function pass(): Promise<OwnerLastHopPrewarmProgress> {
    const owners = (await deps.loadOwners())
      .map((owner) => ({ ...owner, nodeIds: [...new Set(owner.nodeIds)] }))
      .filter((owner) => owner.nodeIds.length > 0);
    const jobs = owners.flatMap((owner) => owner.nodeIds.map((nodeId) => ({ owner, nodeId })));
    const remaining = new Map(owners.map((owner) => [owner, owner.nodeIds.length]));
    const progress: OwnerLastHopPrewarmProgress = {
      owners: 0, ownersTotal: owners.length, nodes: 0, nodesTotal: jobs.length,
      refreshed: 0, failed: 0,
    };
    log.log('[owner-last-hop-prewarm] started', { ...progress, concurrency });
    let next = 0;
    async function work() {
      while (!stopped && next < jobs.length) {
        const { owner, nodeId } = jobs[next++]!;
        const startedAt = now();
        let warned = false;
        const warnSlow = () => {
          warned = true;
          log.warn('[owner-last-hop-prewarm] refresh exceeds 20s', {
            owner: owner.mqttUsername, nodeId, elapsedMs: now() - startedAt,
          });
        };
        const slowTimer = setTimeout(warnSlow, 20_000);
        slowTimer.unref();
        try {
          await deps.refresh(owner, nodeId);
          progress.refreshed++;
        } catch (error) {
          progress.failed++;
          log.warn('[owner-last-hop-prewarm] refresh failed', {
            owner: owner.mqttUsername, nodeId,
            error: error instanceof Error ? error.message : String(error),
          });
        } finally {
          clearTimeout(slowTimer);
          if (!warned && now() - startedAt > 20_000) warnSlow();
          progress.nodes++;
          const left = remaining.get(owner)! - 1;
          remaining.set(owner, left);
          if (left === 0) progress.owners++;
          if (progress.nodes % 10 === 0 && progress.nodes < progress.nodesTotal) {
            log.log('[owner-last-hop-prewarm] progress', { ...progress });
          }
        }
      }
    }
    await Promise.all(Array.from({ length: concurrency }, () => work()));
    log.log(stopped ? '[owner-last-hop-prewarm] stopped' : '[owner-last-hop-prewarm] complete', { ...progress });
    return progress;
  }

  function run(): Promise<OwnerLastHopPrewarmProgress> {
    if (inFlight) return inFlight;
    if (stopped) return Promise.reject(new Error('OWNER_LAST_HOP_PREWARM_STOPPED'));
    inFlight = pass().finally(() => { inFlight = null; });
    return inFlight;
  }

  function start(intervalMs = 30 * 60_000): void {
    if (started || stopped) return;
    started = true;
    const scheduled = () => {
      void run().catch((error: unknown) => {
        log.warn('[owner-last-hop-prewarm] pass failed', error instanceof Error ? error.message : String(error));
      }).finally(() => {
        if (!stopped) {
          timer = setTimeout(scheduled, intervalMs);
          timer.unref();
        }
      });
    };
    scheduled();
  }

  async function stop(): Promise<void> {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
    await inFlight?.catch(() => undefined);
  }

  return { run, start, stop };
}
