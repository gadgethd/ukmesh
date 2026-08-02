import { getPublicVisibilityGeneration, pool, query } from '../db/index.js';
import {
  analysisGeneration,
  beginAnalysisRun,
  finishAnalysisRun,
  startAnalysisRunHeartbeat,
  updateAnalysisRunTotalItems,
} from '../analysis/runState.js';
import { loadSpamMessageConfig } from '../spam/config.js';
import {
  countLogicalMessages,
  loadRecentMessages,
  persistIncidents,
  withSpamAnalyzerLease,
} from '../spam/repository.js';
import { buildIncidentsWithPaths } from '../spam/analyzer.js';

// ---------------------------------------------------------------------------
// One-off / backfill recompute of message-spam incidents.
//
//   node dist/tools/recomputeSpamMessages.js              # dry run, 24h
//   node dist/tools/recomputeSpamMessages.js --hours 720  # dry run, 30 days
//   node dist/tools/recomputeSpamMessages.js --hours 720 --apply
// ---------------------------------------------------------------------------

function argValue(flag: string): string | undefined {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 ? process.argv[idx + 1] : undefined;
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const hoursArg = argValue('--hours');
  const base = loadSpamMessageConfig();
  const cfg = { ...base, analysisWindowHours: hoursArg ? Number(hoursArg) : base.analysisWindowHours };

  console.log(`[spam-msg-recompute] window=${cfg.analysisWindowHours}h apply=${apply}`);
  const windowEnd = new Date();
  const windowStart = new Date(windowEnd.getTime() - cfg.analysisWindowHours * 60 * 60 * 1000);
  const candidateCount = await countLogicalMessages({ start: windowStart, end: windowEnd });
  if (candidateCount > cfg.maxMessagesPerRun) {
    throw new Error(
      `SPAM_RECOMPUTE_WINDOW_TOO_LARGE:${candidateCount}>${cfg.maxMessagesPerRun}; reduce --hours or raise the reviewed bounded limit`,
    );
  }

  const result = await withSpamAnalyzerLease(async () => {
    const visibilityGeneration = apply
      ? await getPublicVisibilityGeneration()
      : undefined;
    const run = visibilityGeneration === undefined
      ? null
      : await beginAnalysisRun({
        workload: 'spam-analysis',
        scope: 'public',
        windowStart,
        windowEnd,
        totalItems: 0,
        deadlineMs: cfg.analysisBudgetMs,
        privacyGeneration: visibilityGeneration,
        modelGeneration: 'spam-analysis-v2',
      });
    const heartbeat = run ? startAnalysisRunHeartbeat(run) : null;
    try {
      const records = await loadRecentMessages(
        cfg,
        { start: windowStart, end: windowEnd },
        heartbeat?.signal,
      );
      console.log(`[spam-msg-recompute] loaded ${records.length}/${candidateCount} decoded message transmissions`);
      if (run && heartbeat) {
        await updateAnalysisRunTotalItems(run, records.length, heartbeat.signal);
      }

      const items = await buildIncidentsWithPaths(
        records,
        Date.now(),
        heartbeat
          ? (text, params) => query(text, params, heartbeat.signal)
          : query,
        cfg,
        run?.deadlineAt.getTime(),
        heartbeat?.signal,
      );
      console.log(`[spam-msg-recompute] built ${items.length} incident(s):`);
      for (const it of items.slice(0, 25)) {
        const p = it.publicJson;
        console.log(
          `  - [${p.status}] msgs=${p.messageCount} obs=${p.observerCount} ` +
            `conf=${p.confidence} origin="${p.origin.region}" (${p.origin.level}) ` +
            `sample="${p.sampleMessage.slice(0, 60)}"`,
        );
      }

      if (!run || !heartbeat) {
        console.log('[spam-msg-recompute] dry run only; rerun with --apply to persist');
        return;
      }
      const res = await persistIncidents(items, cfg, run);
      await heartbeat();
      await finishAnalysisRun(run, {
        status: 'complete',
        checkpoint: records.length,
        generation: analysisGeneration(items.map((item) => item.publicJson)),
        metadata: {
          incidents: items.length,
          lifecycleExpired: res.lifecycleExpired,
          manualRecompute: true,
        },
      });
      console.log(`[spam-msg-recompute] persisted: upserted=${res.upserted} removed=${res.removed} active=${res.active}`);
    } catch (error) {
      if (run && heartbeat) {
        try {
          const timedOut = Date.now() >= run.deadlineAt.getTime();
          if (timedOut) await heartbeat.stopForTerminal();
          else await heartbeat();
          await finishAnalysisRun(run, {
            status: timedOut ? 'timed_out' : 'failed',
            checkpoint: 0,
            error: error instanceof Error ? error.message : String(error),
            metadata: { manualRecompute: true },
          });
        } catch (finishError) {
          console.error('[spam-msg-recompute] could not record terminal run:', finishError);
        }
      }
      throw error;
    } finally {
      await heartbeat?.().catch(() => {});
    }
  });
  if (result === null) throw new Error('SPAM_ANALYZER_LEASE_BUSY');
}

main()
  .catch((err: unknown) => {
    console.error('[spam-msg-recompute] failed:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
