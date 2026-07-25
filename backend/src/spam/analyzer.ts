import { query as dbQuery } from '../db/index.js';
import { loadSpamMessageConfig, type SpamMessageConfig } from './config.js';
import { clusterMessages, incidentStatus } from './cluster.js';
import { estimateOrigin } from './origin.js';
import { resolveSpamOrigin } from './spamResolver.js';
import { sanitizeIncident } from './sanitize.js';
import {
  loadRecentMessages,
  persistIncidents,
  withSpamAnalyzerLease,
  type PersistableIncident,
  type PersistResult,
} from './repository.js';
import type { Incident, MessageRecord, OriginEstimate } from './types.js';

type SpamQueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

// ---------------------------------------------------------------------------
// Orchestration: load -> cluster -> estimate origin -> sanitize -> persist.
// ---------------------------------------------------------------------------

/**
 * Pure pipeline step: turn raw message records + a clock into persistable
 * incidents using only observer signal/hops for the origin. Exposed separately
 * so it can be unit-tested without a database (no path resolution).
 */
export function buildIncidents(
  records: MessageRecord[],
  now: number,
  cfg: SpamMessageConfig,
): PersistableIncident[] {
  const incidents = clusterMessages(records, cfg);
  return incidents.map((incident) => {
    const observers = incident.members.flatMap((m) => m.observers);
    const origin = estimateOrigin(observers, cfg);
    const status = incidentStatus(incident.lastSeen, now, cfg);
    const publicJson = sanitizeIncident(incident, origin, status, cfg);
    return { incident, origin, status, publicJson };
  });
}

/**
 * Best available origin for one incident: prefer the relay-path anchor (the
 * near-source repeater is the source of truth), falling back to the observer
 * signal/hop estimate when paths can't be resolved confidently. Path resolution
 * is only spent on incidents that will be shown publicly.
 */
async function originForIncident(
  incident: Incident,
  query: SpamQueryFn,
  cfg: SpamMessageConfig,
): Promise<OriginEstimate> {
  const observers = incident.members.flatMap((m) => m.observers);
  const fallback = estimateOrigin(observers, cfg);
  if (!cfg.originUsePaths || incident.score < cfg.publicMinScore) return fallback;
  try {
    const packetHashes = incident.members.map((m) => m.id);
    const pathOrigin = await resolveSpamOrigin(packetHashes, incident.network, query, cfg);
    if (pathOrigin) return pathOrigin;
  } catch (err: unknown) {
    console.error('[spam-msg] path origin failed:', err instanceof Error ? err.message : err);
  }
  return fallback;
}

/**
 * Path-aware pipeline step (async, needs DB): cluster -> resolve each incident's
 * origin from relay paths -> sanitize. Used by the live analyzer and the backfill
 * tool; `buildIncidents` stays as the pure, path-free variant for tests.
 */
export async function buildIncidentsWithPaths(
  records: MessageRecord[],
  now: number,
  query: SpamQueryFn,
  cfg: SpamMessageConfig,
): Promise<PersistableIncident[]> {
  const incidents = clusterMessages(records, cfg);
  const out: PersistableIncident[] = [];
  for (const incident of incidents) {
    const origin = await originForIncident(incident, query, cfg);
    const status = incidentStatus(incident.lastSeen, now, cfg);
    const publicJson = sanitizeIncident(incident, origin, status, cfg);
    out.push({ incident, origin, status, publicJson });
  }
  return out;
}

export interface AnalyzeResult extends PersistResult {
  messages: number;
  incidents: number;
}

/** Run one full analysis pass and persist the results. */
export async function analyzeOnce(cfg: SpamMessageConfig = loadSpamMessageConfig()): Promise<AnalyzeResult> {
  const result = await withSpamAnalyzerLease(async () => {
    const records = await loadRecentMessages(cfg);
    const items = await buildIncidentsWithPaths(records, Date.now(), dbQuery, cfg);
    const persisted = await persistIncidents(items, cfg);
    return { ...persisted, messages: records.length, incidents: items.length };
  });
  return result ?? { upserted: 0, removed: 0, active: 0, messages: 0, incidents: 0 };
}

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<AnalyzeResult> | null = null;

export function analyzeSingleFlight(cfg: SpamMessageConfig): Promise<AnalyzeResult> {
  if (inFlight) return inFlight;
  const tracked = analyzeOnce(cfg).finally(() => {
    if (inFlight === tracked) inFlight = null;
  });
  inFlight = tracked;
  return tracked;
}

/** Start the periodic in-process analyzer (no-op if disabled by config). */
export async function initSpamMessageAnalyzer(cfg: SpamMessageConfig = loadSpamMessageConfig()): Promise<void> {
  if (!cfg.analyzerEnabled) {
    console.log('[spam-msg] analyzer disabled by SPAM_MESSAGE_ANALYZER_ENABLED');
    return;
  }
  if (timer || inFlight) return;

  const run = async () => {
    try {
      const res = await analyzeSingleFlight(cfg);
      console.log(
        `[spam-msg] analyzed ${res.messages} messages -> ${res.incidents} incidents ` +
          `(${res.active} active, ${res.removed} stale removed)`,
      );
    } catch (err: unknown) {
      console.error('[spam-msg] analyzer error:', err instanceof Error ? err.message : err);
    }
  };

  const schedule = () => {
    timer = setTimeout(async () => {
      timer = null;
      await run();
      schedule();
    }, cfg.analyzerIntervalMs);
    timer.unref();
  };
  await run();
  schedule();
}
