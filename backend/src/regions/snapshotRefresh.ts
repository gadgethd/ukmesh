import type { DatabaseQueryFn } from '../db/contracts.js';
import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';
import { observeWorkerOutcome, regionSnapshotHealthy, regionSnapshotPublishedSeconds, regionSnapshotRunning } from '../metrics.js';
import type { regionRebuildSettings } from './rebuildSettings.js';

type Settings = ReturnType<typeof regionRebuildSettings>;
export type RegionRefreshOutcome = 'fresh' | 'rebuilt' | 'unavailable' | 'backoff' | 'failed';

/** One background operation; SQL remains authoritative for publication/serving. */
export class RegionSnapshotRefresher {
  private active: Promise<RegionRefreshOutcome> | undefined;
  private failures = 0;
  private retryAt = 0;
  private attemptedGeneration = 0;

  constructor(private readonly options: {
    query: DatabaseQueryFn;
    run: (signal: AbortSignal) => Promise<void>;
    settings: Settings;
    now?: () => number;
  }) {}

  runOnce(signal: AbortSignal): Promise<RegionRefreshOutcome> {
    if (this.active) return this.active;
    this.active = this.iterate(signal).finally(() => { this.active = undefined; });
    return this.active;
  }

  private async state() {
    const generation = await readPublicResponseGeneration(sql => this.options.query(sql));
    const result = await this.options.query<{
      visibility_generation: string; window_days: number; finished_at: Date; fresh: boolean;
    }>(`SELECT visibility_generation::text, window_days, finished_at,
        (finished_at BETWEEN NOW() - ($1::bigint * INTERVAL '1 millisecond')
          AND NOW() + INTERVAL '1 minute') AS fresh
      FROM core.region_scope_run WHERE id = TRUE AND network = 'ukmesh'`, [this.options.settings.maxAgeMs]);
    const row = result.rows[0];
    return {
      generation,
      fresh: row?.fresh === true && Number(row.visibility_generation) === generation
        && row.window_days === this.options.settings.windowDays,
      publishedAt: row ? new Date(row.finished_at).getTime() / 1000 : 0,
    };
  }

  private async iterate(signal: AbortSignal): Promise<RegionRefreshOutcome> {
    signal.throwIfAborted();
    let state;
    try { state = await this.state(); }
    catch {
      // Pending/failed privacy work and authority outages must not start a build.
      regionSnapshotHealthy.set(0);
      observeWorkerOutcome('region_scope', 'poll', 'skipped');
      return 'unavailable';
    }
    if (Number.isFinite(state.publishedAt)) regionSnapshotPublishedSeconds.set(state.publishedAt);
    if (state.fresh) {
      regionSnapshotHealthy.set(1);
      this.failures = 0; this.retryAt = 0;
      return 'fresh';
    }
    regionSnapshotHealthy.set(0);
    const now = this.options.now ?? (() => performance.now());
    if (state.generation !== this.attemptedGeneration) {
      this.failures = 0; this.retryAt = 0;
    }
    if (now() < this.retryAt) return 'backoff';
    this.attemptedGeneration = state.generation;
    regionSnapshotRunning.set(1);
    try {
      await this.options.run(signal);
      // The child may have observed a newer ready generation than this poll.
      const published = await this.state();
      if (!published.fresh) throw new Error('REGION_SNAPSHOT_NOT_CERTIFIED');
      regionSnapshotHealthy.set(1);
      regionSnapshotPublishedSeconds.set(published.publishedAt);
      this.failures = 0; this.retryAt = 0;
      observeWorkerOutcome('region_scope', 'rebuild', 'success');
      return 'rebuilt';
    } catch (error) {
      if (signal.aborted) throw error;
      this.failures = Math.min(10, this.failures + 1);
      this.retryAt = now() + Math.min(300_000, this.options.settings.pollMs * 2 ** (this.failures - 1));
      observeWorkerOutcome('region_scope', 'rebuild', 'failure');
      console.error('[region-snapshot] rebuild failed:', error instanceof Error ? error.message : String(error));
      return 'failed';
    } finally { regionSnapshotRunning.set(0); }
  }
}
