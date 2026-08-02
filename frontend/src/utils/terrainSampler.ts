/**
 * Client-side Terrarium elevation sampler.
 *
 * Decoded tiles are scoped to the public terrain tileset, retained for at most
 * 10 minutes, and bounded to 16 entries / 32 MiB (ImageData + ImageBitmap).
 * Concurrent callers share one load per tile; at most four tile decodes run at
 * once and sixteen unique tile loads may be in flight.
 */

import { RequestLimiter } from './requestLimiter.js';

const TILE_SIZE = 512;
const SAMPLE_ZOOM = 11;
const TILE_TTL_MS = 10 * 60_000;
const MAX_TILE_ENTRIES = 16;
const MAX_TILE_BYTES = 32 * 1024 * 1024;
const MAX_TILE_INFLIGHT = 16;
const tileDecodeLimiter = new RequestLimiter(4, 12);

export type TerrainTile = {
  data: ImageData;
  bitmap: ImageBitmap;
};

type TileLoader = (key: string, signal: AbortSignal) => Promise<TerrainTile | null>;
type CacheEntry = {
  tile: TerrainTile;
  bytes: number;
  expiresAt: number;
};
type InflightEntry = {
  controller: AbortController;
  promise: Promise<TerrainTile | null>;
  consumers: number;
  settled: boolean;
  generation: number;
};

export type TerrainTileCacheSnapshot = {
  entries: number;
  bytes: number;
  inflight: number;
  maxEntries: number;
  maxBytes: number;
  maxInflight: number;
};

function abortError(message: string): DOMException {
  return new DOMException(message, 'AbortError');
}

function estimateTileBytes(tile: TerrainTile): number {
  const dataBytes = tile.data.width * tile.data.height * 4;
  const bitmapBytes = tile.bitmap.width * tile.bitmap.height * 4;
  return Math.max(1, dataBytes + bitmapBytes);
}

export class TerrainTileCache {
  readonly #loader: TileLoader;
  readonly #entries = new Map<string, CacheEntry>();
  readonly #inflight = new Map<string, InflightEntry>();
  readonly #maxEntries: number;
  readonly #maxBytes: number;
  readonly #maxInflight: number;
  readonly #ttlMs: number;
  readonly #now: () => number;
  #bytes = 0;
  #generation = 0;

  constructor(
    loader: TileLoader,
    options: {
      maxEntries?: number;
      maxBytes?: number;
      maxInflight?: number;
      ttlMs?: number;
      now?: () => number;
    } = {},
  ) {
    this.#loader = loader;
    this.#maxEntries = options.maxEntries ?? MAX_TILE_ENTRIES;
    this.#maxBytes = options.maxBytes ?? MAX_TILE_BYTES;
    this.#maxInflight = options.maxInflight ?? MAX_TILE_INFLIGHT;
    this.#ttlMs = options.ttlMs ?? TILE_TTL_MS;
    this.#now = options.now ?? Date.now;
  }

  #remove(key: string): void {
    const entry = this.#entries.get(key);
    if (!entry) return;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    entry.tile.bitmap.close();
  }

  #getCached(key: string): TerrainTile | undefined {
    const entry = this.#entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= this.#now()) {
      this.#remove(key);
      return undefined;
    }
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    return entry.tile;
  }

  #store(key: string, tile: TerrainTile): void {
    this.#remove(key);
    const bytes = estimateTileBytes(tile);
    if (bytes > this.#maxBytes) {
      tile.bitmap.close();
      return;
    }
    this.#entries.set(key, {
      tile,
      bytes,
      expiresAt: this.#now() + this.#ttlMs,
    });
    this.#bytes += bytes;
    while (this.#entries.size > this.#maxEntries || this.#bytes > this.#maxBytes) {
      const oldest = this.#entries.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.#remove(oldest);
    }
  }

  async get(key: string, signal?: AbortSignal): Promise<TerrainTile | null> {
    if (signal?.aborted) throw signal.reason ?? abortError('Terrain tile request aborted');
    const cached = this.#getCached(key);
    if (cached) return cached;

    let pending = this.#inflight.get(key);
    if (!pending) {
      if (this.#inflight.size >= this.#maxInflight) {
        throw new Error('Terrain tile in-flight limit exceeded');
      }
      const controller = new AbortController();
      const generation = this.#generation;
      const record: InflightEntry = {
        controller,
        consumers: 0,
        settled: false,
        generation,
        promise: Promise.resolve(null),
      };
      record.promise = this.#loader(key, controller.signal)
        .then((tile) => {
          if (!tile) return null;
          if (controller.signal.aborted || generation !== this.#generation) {
            tile.bitmap.close();
            return null;
          }
          this.#store(key, tile);
          return this.#getCached(key) ?? null;
        });
      record.promise.then(
        () => {
          record.settled = true;
          if (this.#inflight.get(key) === record) this.#inflight.delete(key);
        },
        () => {
          record.settled = true;
          if (this.#inflight.get(key) === record) this.#inflight.delete(key);
        },
      );
      pending = record;
      this.#inflight.set(key, record);
    }

    pending.consumers += 1;
    try {
      if (!signal) return await pending.promise;
      return await new Promise<TerrainTile | null>((resolve, reject) => {
        let finished = false;
        const finish = (callback: () => void) => {
          if (finished) return;
          finished = true;
          signal.removeEventListener('abort', onAbort);
          callback();
        };
        const onAbort = () => finish(() => reject(signal.reason ?? abortError('Terrain tile request aborted')));
        signal.addEventListener('abort', onAbort, { once: true });
        pending!.promise.then(
          (tile) => finish(() => resolve(tile)),
          (error) => finish(() => reject(error)),
        );
      });
    } finally {
      pending.consumers -= 1;
      if (pending.consumers === 0 && !pending.settled) {
        pending.controller.abort(abortError('All terrain tile consumers cancelled'));
      }
    }
  }

  peek(key: string): TerrainTile | undefined {
    return this.#getCached(key);
  }

  clear(): void {
    this.#generation += 1;
    for (const pending of this.#inflight.values()) {
      pending.controller.abort(abortError('Terrain tile cache cleared'));
    }
    this.#inflight.clear();
    for (const key of [...this.#entries.keys()]) this.#remove(key);
  }

  snapshot(): TerrainTileCacheSnapshot {
    for (const key of [...this.#entries.keys()]) this.#getCached(key);
    return {
      entries: this.#entries.size,
      bytes: this.#bytes,
      inflight: this.#inflight.size,
      maxEntries: this.#maxEntries,
      maxBytes: this.#maxBytes,
      maxInflight: this.#maxInflight,
    };
  }
}

function lngLatToTileXY(lng: number, lat: number, z: number): [number, number] {
  const n = 2 ** z;
  const boundedLat = Math.max(-85.051129, Math.min(85.051129, lat));
  const x = Math.floor((lng + 180) / 360 * n);
  const latRad = boundedLat * Math.PI / 180;
  const y = Math.floor(
    (1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n,
  );
  return [x, y];
}

function lngLatToPixelWithinTile(
  lng: number,
  lat: number,
  z: number,
  tx: number,
  ty: number,
): [number, number] {
  const n = 2 ** z;
  const boundedLat = Math.max(-85.051129, Math.min(85.051129, lat));
  const px = Math.floor(((lng + 180) / 360 * n - tx) * TILE_SIZE);
  const latRad = boundedLat * Math.PI / 180;
  const py = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n - ty) * TILE_SIZE,
  );
  return [
    Math.max(0, Math.min(TILE_SIZE - 1, px)),
    Math.max(0, Math.min(TILE_SIZE - 1, py)),
  ];
}

async function decodeTerrainTile(key: string, signal: AbortSignal): Promise<TerrainTile | null> {
  return tileDecodeLimiter.run(signal, async () => {
    const response = await fetch(`/terrain-tiles/${key}.png`, { signal, cache: 'force-cache' });
    if (!response.ok) return null;
    const bitmap = await createImageBitmap(await response.blob());
    try {
      if (signal.aborted) throw signal.reason ?? abortError('Terrain decode aborted');
      const canvas = document.createElement('canvas');
      canvas.width = TILE_SIZE;
      canvas.height = TILE_SIZE;
      const context = canvas.getContext('2d', { willReadFrequently: true });
      if (!context) throw new Error('Terrain canvas context unavailable');
      context.drawImage(bitmap, 0, 0, TILE_SIZE, TILE_SIZE);
      return {
        data: context.getImageData(0, 0, TILE_SIZE, TILE_SIZE),
        bitmap,
      };
    } catch (error) {
      bitmap.close();
      throw error;
    }
  });
}

const terrainTiles = new TerrainTileCache(decodeTerrainTile);

function decodeTerrarium(r: number, g: number, b: number): number {
  return r * 256 + g + b / 256 - 32768;
}

function sampleTile(tile: ImageData, px: number, py: number): number {
  const index = (py * TILE_SIZE + px) * 4;
  return decodeTerrarium(
    tile.data[index] ?? 0,
    tile.data[index + 1] ?? 0,
    tile.data[index + 2] ?? 0,
  );
}

export async function sampleElevationAt(
  lng: number,
  lat: number,
  signal?: AbortSignal,
): Promise<number> {
  const [tx, ty] = lngLatToTileXY(lng, lat, SAMPLE_ZOOM);
  const tile = await terrainTiles.get(`${SAMPLE_ZOOM}/${tx}/${ty}`, signal);
  if (!tile) return 0;
  const [px, py] = lngLatToPixelWithinTile(lng, lat, SAMPLE_ZOOM, tx, ty);
  return sampleTile(tile.data, px, py);
}

export async function sampleTerrainProfile(
  lng1: number,
  lat1: number,
  lng2: number,
  lat2: number,
  nSamples = 60,
  signal?: AbortSignal,
): Promise<[number, number, number][]> {
  const sampleCount = Math.max(1, Math.min(500, Math.trunc(nSamples)));
  const output: [number, number, number][] = Array.from(
    { length: sampleCount + 1 },
    () => [0, 0, 0],
  );
  const pointsByTile = new Map<string, Array<{
    index: number;
    lng: number;
    lat: number;
    px: number;
    py: number;
  }>>();

  for (let index = 0; index <= sampleCount; index += 1) {
    const ratio = index / sampleCount;
    const lng = lng1 + ratio * (lng2 - lng1);
    const lat = lat1 + ratio * (lat2 - lat1);
    const [tx, ty] = lngLatToTileXY(lng, lat, SAMPLE_ZOOM);
    const key = `${SAMPLE_ZOOM}/${tx}/${ty}`;
    const [px, py] = lngLatToPixelWithinTile(lng, lat, SAMPLE_ZOOM, tx, ty);
    const group = pointsByTile.get(key) ?? [];
    group.push({ index, lng, lat, px, py });
    pointsByTile.set(key, group);
  }

  const groups = [...pointsByTile.entries()];
  for (let offset = 0; offset < groups.length; offset += 4) {
    if (signal?.aborted) throw signal.reason ?? abortError('Terrain profile aborted');
    await Promise.all(groups.slice(offset, offset + 4).map(async ([key, points]) => {
      const tile = await terrainTiles.get(key, signal).catch((error: unknown) => {
        if (signal?.aborted) throw error;
        return null;
      });
      for (const point of points) {
        const elevation = tile ? sampleTile(tile.data, point.px, point.py) : 0;
        output[point.index] = [point.lng, point.lat, Math.max(0, elevation)];
      }
    }));
  }
  return output;
}

export function terrainTileCacheSnapshot(): TerrainTileCacheSnapshot {
  return terrainTiles.snapshot();
}

export function clearTerrainTileCache(): void {
  terrainTiles.clear();
}
