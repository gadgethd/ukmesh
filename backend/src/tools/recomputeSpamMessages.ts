import { pool, query } from '../db/index.js';
import { loadSpamMessageConfig } from '../spam/config.js';
import { loadRecentMessages, persistIncidents } from '../spam/repository.js';
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

  const records = await loadRecentMessages(cfg);
  console.log(`[spam-msg-recompute] loaded ${records.length} decoded message transmissions`);

  const items = await buildIncidentsWithPaths(records, Date.now(), query, cfg);
  console.log(`[spam-msg-recompute] built ${items.length} incident(s):`);
  for (const it of items.slice(0, 25)) {
    const p = it.publicJson;
    console.log(
      `  - [${p.status}] msgs=${p.messageCount} obs=${p.observerCount} ` +
        `conf=${p.confidence} origin="${p.origin.region}" (${p.origin.level}) ` +
        `sample="${p.sampleMessage.slice(0, 60)}"`,
    );
  }

  if (!apply) {
    console.log('[spam-msg-recompute] dry run only; rerun with --apply to persist');
    return;
  }

  const res = await persistIncidents(items, cfg);
  console.log(`[spam-msg-recompute] persisted: upserted=${res.upserted} removed=${res.removed} active=${res.active}`);
}

main()
  .catch((err: unknown) => {
    console.error('[spam-msg-recompute] failed:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
