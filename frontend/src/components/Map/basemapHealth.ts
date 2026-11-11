/**
 * Basemap health — decides when the remote vector basemap is not arriving and
 * whether to retry it (cache-busted style reload) or fall back to the bundled
 * offline outline.
 *
 * The live map's mesh layers (node dots, links, deck.gl arcs) are independent of
 * the basemap source, so when OpenFreeMap tiles stall or fail MapLibre keeps
 * painting the style background and the app keeps working — which looks like a
 * "blank map". This state machine makes that condition explicit and recoverable.
 */

export const BASEMAP_VECTOR_SOURCE_ID = 'openmaptiles';

/** How long a freshly loaded style may take to produce basemap source data. */
export const BASEMAP_SOURCE_LOAD_TIMEOUT_MS = 8_000;
/** How long a cache-busted retry may take before we fall back. */
export const BASEMAP_RETRY_TIMEOUT_MS = 10_000;
/** A burst of tile errors inside the window is treated as a failed basemap. */
export const BASEMAP_TILE_ERROR_THRESHOLD = 12;
export const BASEMAP_TILE_ERROR_WINDOW_MS = 10_000;

export type BasemapPhase = 'loading' | 'healthy' | 'retrying' | 'fallback';

export type BasemapHealth = {
  phase: BasemapPhase;
  /** When the current style finished loading (or the retry started). */
  styleLoadedAt: number | null;
  /** Cache-busted retries already attempted in this session. */
  retries: number;
  /** Timestamps of recent tile errors, pruned to the window. */
  tileErrors: number[];
  lastError: string | null;
};

export type BasemapDecision = 'none' | 'retry' | 'fallback';

/** Bypass metadata pinned by older service workers; tile snapshot URLs stay cacheable. */
export function refreshBasemapMetadataUrl(url: string, revision: number): string {
  try {
    const parsed = new URL(url);
    if (parsed.hostname !== 'tiles.openfreemap.org') return url;
    if (parsed.pathname !== '/planet' && !parsed.pathname.startsWith('/styles/')) return url;
    parsed.searchParams.set('v', String(revision));
    return parsed.toString();
  } catch {
    return url;
  }
}

/** isSourceLoaded means no requests remain, even if every tile failed. */
export function isSuccessfulBasemapTile(event: {
  sourceId?: string;
  tile?: { state?: string };
}): boolean {
  return event.sourceId === BASEMAP_VECTOR_SOURCE_ID && event.tile?.state === 'loaded';
}

export function createBasemapHealth(): BasemapHealth {
  return {
    phase: 'loading',
    styleLoadedAt: null,
    retries: 0,
    tileErrors: [],
    lastError: null,
  };
}

/** A style finished loading; restart the clock for source-data arrival. */
export function noteStyleLoaded(health: BasemapHealth, now: number): BasemapHealth {
  if (health.phase === 'fallback') return health;
  return {
    ...health,
    phase: health.phase === 'retrying' ? 'retrying' : 'loading',
    styleLoadedAt: now,
    tileErrors: [],
  };
}

/** A basemap tile was decoded successfully; the basemap is healthy. */
export function noteSourceData(
  health: BasemapHealth,
  sourceId: string | undefined,
  tileLoaded: boolean,
  _now: number,
): BasemapHealth {
  if (sourceId !== BASEMAP_VECTOR_SOURCE_ID || !tileLoaded) return health;
  if (health.phase === 'fallback' || health.phase === 'healthy') return health;
  return {
    ...health,
    phase: 'healthy',
    retries: 0,
    tileErrors: [],
  };
}

/** Record a tile/style error that may indicate a dead basemap. */
export function noteTileError(
  health: BasemapHealth,
  now: number,
  message: string | null,
  windowMs: number = BASEMAP_TILE_ERROR_WINDOW_MS,
): BasemapHealth {
  if (health.phase === 'fallback') return health;
  const tileErrors = [...health.tileErrors.filter((at) => now - at <= windowMs), now];
  return {
    ...health,
    tileErrors,
    lastError: message ?? health.lastError,
  };
}

/** Decide the next recovery step, if any. */
export function decideBasemapAction(
  health: BasemapHealth,
  now: number,
  options: { timeoutMs?: number; tileErrorThreshold?: number } = {},
): BasemapDecision {
  if (health.phase === 'healthy' || health.phase === 'fallback') return 'none';
  if (health.styleLoadedAt == null) return 'none';

  const timeoutMs = options.timeoutMs
    ?? (health.phase === 'retrying' ? BASEMAP_RETRY_TIMEOUT_MS : BASEMAP_SOURCE_LOAD_TIMEOUT_MS);
  const timedOut = now - health.styleLoadedAt >= timeoutMs;
  const errorStorm = health.tileErrors.length >= (options.tileErrorThreshold ?? BASEMAP_TILE_ERROR_THRESHOLD);
  if (!timedOut && !errorStorm) return 'none';

  return health.retries === 0 ? 'retry' : 'fallback';
}

/** A cache-busted style reload has been issued. */
export function noteRetryStarted(health: BasemapHealth, now: number): BasemapHealth {
  return {
    ...health,
    phase: 'retrying',
    retries: health.retries + 1,
    styleLoadedAt: now,
    tileErrors: [],
  };
}

/** The bundled offline basemap has taken over. */
export function noteFallback(health: BasemapHealth, reason: string | null = null): BasemapHealth {
  return {
    ...health,
    phase: 'fallback',
    tileErrors: [],
    lastError: reason ?? health.lastError,
  };
}

/** Remaining absolute attempt budget, including an early timer firing. */
export function basemapWatchdogDelay(health: BasemapHealth, now: number): number | null {
  if (health.phase === 'healthy' || health.phase === 'fallback' || health.styleLoadedAt == null) return null;
  const timeout = health.phase === 'retrying' ? BASEMAP_RETRY_TIMEOUT_MS : BASEMAP_SOURCE_LOAD_TIMEOUT_MS;
  return Math.max(1, timeout - (now - health.styleLoadedAt));
}
