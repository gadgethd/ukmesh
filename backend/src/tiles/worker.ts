/**
 * Tile pre-rendering worker.
 *
 * On startup and every hour, renders all node tiles covering the UK
 * (z=5–13) for each active network and writes them to Redis with a
 * 90-minute TTL.  Everything outside the UK bbox continues to render
 * on-demand via the tile API route.
 *
 * Yields to the event loop every batch flush so tile compression
 * does not monopolise the worker event loop.
 * is never starved.
 */
import { Redis } from 'ioredis';
import { buildTileIndex, renderTileFromIndex } from './renderer.js';
import { getTileSnapshotNodes } from './snapshot.js';

// UK bounding box — same as the viewshed eligibility check in index.ts
const UK_LAT_MIN = 49.5;
const UK_LAT_MAX = 61.5;
const UK_LON_MIN = -8.5;
const UK_LON_MAX =  2.5;

export const UK_ZOOM_MIN =  5;
export const UK_ZOOM_MAX = 13;

// Tile TTL: 90 min.  Worker runs every 60 min, so tiles never expire
// between passes.  On-demand route also uses this TTL for UK tiles.
export const UK_TILE_TTL_MS = 90 * 60 * 1000;

const REFRESH_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const BATCH_SIZE = 50; // tiles per pipeline flush + event-loop yield
const TILE_WORKER_STATE_KEY = 'meshcore:tile_worker:state';
const TILE_WORKER_RESUME_MAX_AGE_MS = 2 * 60 * 60 * 1000;

// Networks to pre-render.  Each entry is [db_network, cache_key_scope].
// 'all' scope uses undefined for getNodes() (returns all non-test nodes).
const NETWORKS: Array<{ db: string | undefined; scope: string }> = [
  { db: undefined,    scope: 'all' },
  { db: 'teesside',  scope: 'teesside' },
];

type TileCursor = {
  scopeIndex: number;
  z: number;
  x: number;
  y: number;
};

function firstCursor(): TileCursor {
  const firstScopeIndex = 0;
  const { xMin, yMin } = ukTileRange(UK_ZOOM_MIN);
  return {
    scopeIndex: firstScopeIndex,
    z: UK_ZOOM_MIN,
    x: xMin,
    y: yMin,
  };
}

function parseInteger(value: string | undefined): number | null {
  if (value == null || value === '') return null;
  const num = Number(value);
  return Number.isInteger(num) ? num : null;
}

function parseResumeCursor(state: Record<string, string>): TileCursor | null {
  if (state['status'] !== 'running') return null;
  const updatedAt = state['updated_at'] ? Date.parse(state['updated_at']) : Number.NaN;
  if (!Number.isFinite(updatedAt) || Date.now() - updatedAt > TILE_WORKER_RESUME_MAX_AGE_MS) return null;

  const scopeIndex = parseInteger(state['next_scope_index']);
  const z = parseInteger(state['next_zoom']);
  const x = parseInteger(state['next_x']);
  const y = parseInteger(state['next_y']);
  if (scopeIndex == null || z == null || x == null || y == null) return null;
  if (scopeIndex < 0 || scopeIndex >= NETWORKS.length) return null;
  if (z < UK_ZOOM_MIN || z > UK_ZOOM_MAX) return null;

  const { xMin, xMax, yMin, yMax } = ukTileRange(z);
  if (x < xMin || x > xMax || y < yMin || y > yMax) return null;
  return { scopeIndex, z, x, y };
}

function nextCursor(cursor: TileCursor): TileCursor | null {
  const { scopeIndex, z, x, y } = cursor;
  const range = ukTileRange(z);
  if (y < range.yMax) {
    return { scopeIndex, z, x, y: y + 1 };
  }
  if (x < range.xMax) {
    return { scopeIndex, z, x: x + 1, y: range.yMin };
  }
  if (z < UK_ZOOM_MAX) {
    const nextZ = z + 1;
    const nextRange = ukTileRange(nextZ);
    return { scopeIndex, z: nextZ, x: nextRange.xMin, y: nextRange.yMin };
  }
  if (scopeIndex + 1 < NETWORKS.length) {
    const nextScopeIndex = scopeIndex + 1;
    const nextRange = ukTileRange(UK_ZOOM_MIN);
    return { scopeIndex: nextScopeIndex, z: UK_ZOOM_MIN, x: nextRange.xMin, y: nextRange.yMin };
  }
  return null;
}

function cursorMatches(scopeIndex: number, z: number, x: number, y: number, cursor: TileCursor): boolean {
  return cursor.scopeIndex === scopeIndex && cursor.z === z && cursor.x === x && cursor.y === y;
}

export function isUkTile(z: number, x: number, y: number): boolean {
  const pow2 = Math.pow(2, z);
  const lonW = (x / pow2) * 360 - 180;
  const lonE = ((x + 1) / pow2) * 360 - 180;
  if (lonE < UK_LON_MIN || lonW > UK_LON_MAX) return false;
  const nN = Math.PI - (2 * Math.PI * y)       / pow2;
  const nS = Math.PI - (2 * Math.PI * (y + 1)) / pow2;
  const latN = Math.atan(Math.sinh(nN)) * (180 / Math.PI);
  const latS = Math.atan(Math.sinh(nS)) * (180 / Math.PI);
  return latN >= UK_LAT_MIN && latS <= UK_LAT_MAX;
}

function ukTileRange(z: number) {
  const pow2 = Math.pow(2, z);
  const xMin = Math.floor((UK_LON_MIN + 180) / 360 * pow2);
  const xMax = Math.floor((UK_LON_MAX + 180) / 360 * pow2);
  function mercY(lat: number) {
    const r = lat * Math.PI / 180;
    return Math.floor((1 - Math.log(Math.tan(r) + 1 / Math.cos(r)) / Math.PI) / 2 * pow2);
  }
  return { xMin, xMax, yMin: mercY(UK_LAT_MAX), yMax: mercY(UK_LAT_MIN) };
}

function totalUkTiles(): number {
  let n = 0;
  for (let z = UK_ZOOM_MIN; z <= UK_ZOOM_MAX; z++) {
    const { xMin, xMax, yMin, yMax } = ukTileRange(z);
    n += (xMax - xMin + 1) * (yMax - yMin + 1);
  }
  return n;
}

async function renderPass(redis: Redis): Promise<void> {
  const tileCount = totalUkTiles();
  const start = Date.now();
  const totalTilesAllScopes = tileCount * NETWORKS.length;
  const previousState = await redis.hgetall(TILE_WORKER_STATE_KEY);
  const resumeCursor = parseResumeCursor(previousState);
  const resumed = Boolean(resumeCursor);
  const startCursor = resumeCursor ?? firstCursor();

  await redis.hset(TILE_WORKER_STATE_KEY, {
    status: 'running',
    updated_at: new Date().toISOString(),
    started_at: resumed ? (previousState['started_at'] || new Date(start).toISOString()) : new Date(start).toISOString(),
    total_tiles: String(totalTilesAllScopes),
    done_tiles: resumed ? String(Number(previousState['done_tiles'] ?? 0)) : '0',
    remaining_tiles: resumed
      ? String(Math.max(0, totalTilesAllScopes - Number(previousState['done_tiles'] ?? 0)))
      : String(totalTilesAllScopes),
    scope: resumed ? String(previousState['scope'] ?? '') : '',
    zoom: resumed ? String(previousState['zoom'] ?? '') : '',
    next_scope_index: String(startCursor.scopeIndex),
    next_zoom: String(startCursor.z),
    next_x: String(startCursor.x),
    next_y: String(startCursor.y),
    resumed_from_checkpoint: resumed ? '1' : '0',
    last_error: '',
  });

  let globalDone = resumed ? Number(previousState['done_tiles'] ?? 0) : 0;

  if (resumed) {
    console.log(
      `[tile-worker] resuming pass from scope=${NETWORKS[startCursor.scopeIndex]?.scope ?? startCursor.scopeIndex} z=${startCursor.z} x=${startCursor.x} y=${startCursor.y} done=${globalDone.toLocaleString()}/${totalTilesAllScopes.toLocaleString()}`,
    );
  }

  for (let scopeIndex = startCursor.scopeIndex; scopeIndex < NETWORKS.length; scopeIndex++) {
    const { db, scope } = NETWORKS[scopeIndex]!;
    const nodes = await getTileSnapshotNodes(db);

    // Build spatial index once per network — buckets each node into the
    // exact tile(s) it falls in at every zoom level.  Avoids iterating
    // all nodes for every tile.
    const index = buildTileIndex(nodes, UK_ZOOM_MIN, UK_ZOOM_MAX);
    console.log(`[tile-worker] ${scope} — ${nodes.length} nodes indexed (${index.prohibited.length} prohibited)`);

    let done = 0;
    const resumingThisScope = startCursor.scopeIndex === scopeIndex;
    // Pipeline batches writes to Redis — 100 SET commands per round-trip
    // instead of one per tile.  We flush and yield to the event loop every
    // BATCH_SIZE tiles so HTTP/WS serving is never starved.
    let pipe = redis.pipeline();
    let pipeCount = 0;
    let pendingNextCursor: TileCursor | null = resumingThisScope ? startCursor : null;

    const flush = async () => {
      if (pipeCount === 0) return;
      await pipe.exec();
      pipe = redis.pipeline();
      pipeCount = 0;
      await redis.hset(TILE_WORKER_STATE_KEY, {
        status: 'running',
        updated_at: new Date().toISOString(),
        done_tiles: String(globalDone),
        remaining_tiles: String(Math.max(0, totalTilesAllScopes - globalDone)),
        scope,
        zoom: pendingNextCursor ? String(pendingNextCursor.z) : String(UK_ZOOM_MAX),
        next_scope_index: pendingNextCursor ? String(pendingNextCursor.scopeIndex) : String(NETWORKS.length),
        next_zoom: pendingNextCursor ? String(pendingNextCursor.z) : '',
        next_x: pendingNextCursor ? String(pendingNextCursor.x) : '',
        next_y: pendingNextCursor ? String(pendingNextCursor.y) : '',
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
    };

    const startZoom = resumingThisScope ? startCursor.z : UK_ZOOM_MIN;
    for (let z = startZoom; z <= UK_ZOOM_MAX; z++) {
      const { xMin, xMax, yMin, yMax } = ukTileRange(z);
      const startX = resumingThisScope && z === startCursor.z ? startCursor.x : xMin;
      for (let x = startX; x <= xMax; x++) {
        const startY = resumingThisScope && z === startCursor.z && x === startCursor.x ? startCursor.y : yMin;
        for (let y = startY; y <= yMax; y++) {
          const key = `tile:nodes:${scope}:${z}:${x}:${y}`;
          // Empty tiles return the cached 334-byte buffer instantly (no deflate).
          const png = await renderTileFromIndex(z, x, y, index);
          pipe.set(key, png, 'PX', UK_TILE_TTL_MS);
          done++;
          globalDone++;
          pipeCount++;
          pendingNextCursor = nextCursor({ scopeIndex, z, x, y });

          if (pipeCount >= BATCH_SIZE) await flush();
        }
      }

      await flush(); // flush remainder at end of each zoom level
      await redis.hset(TILE_WORKER_STATE_KEY, {
        status: 'running',
        updated_at: new Date().toISOString(),
        done_tiles: String(globalDone),
        remaining_tiles: String(Math.max(0, totalTilesAllScopes - globalDone)),
        scope,
        zoom: String(z),
        next_scope_index: pendingNextCursor ? String(pendingNextCursor.scopeIndex) : String(NETWORKS.length),
        next_zoom: pendingNextCursor ? String(pendingNextCursor.z) : '',
        next_x: pendingNextCursor ? String(pendingNextCursor.x) : '',
        next_y: pendingNextCursor ? String(pendingNextCursor.y) : '',
      });
      const pct = Math.round(done / tileCount * 100);
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`[tile-worker] ${scope} z=${z} — ${done.toLocaleString()}/${tileCount.toLocaleString()} tiles (${pct}%) — ${elapsed}s elapsed`);
    }
    await flush();

    const totalElapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`[tile-worker] ${scope} complete — ${done.toLocaleString()} tiles in ${totalElapsed}s`);
  }

  await redis.hset(TILE_WORKER_STATE_KEY, {
    status: 'idle',
    updated_at: new Date().toISOString(),
    done_tiles: String(totalTilesAllScopes),
    remaining_tiles: '0',
    last_pass_finished_at: new Date().toISOString(),
    last_pass_tiles: String(totalTilesAllScopes),
    next_scope_index: '',
    next_zoom: '',
    next_x: '',
    next_y: '',
  });
}

export function startTileWorker(): void {
  const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';
  const redis = new Redis(redisUrl);
  redis.on('error', (e: Error) => console.error('[tile-worker] redis error', e.message));

  let running = false;

  const run = () => {
    if (running) {
      console.log('[tile-worker] skipping pass start — previous pass still running');
      return;
    }
    running = true;
    console.log(`[tile-worker] starting pass — ${totalUkTiles()} UK tiles × ${NETWORKS.length} networks`);
    void renderPass(redis)
      .catch(async (err: Error) => {
        await redis.hset(TILE_WORKER_STATE_KEY, {
          status: 'error',
          updated_at: new Date().toISOString(),
          last_error: err.message,
        }).catch(() => {});
        console.error('[tile-worker] pass failed:', err.message);
      })
      .finally(() => {
        running = false;
      });
  };

  // First pass after 5 s (let server finish starting up), then every hour.
  setTimeout(run, 5_000);
  setInterval(run, REFRESH_INTERVAL_MS);
}
