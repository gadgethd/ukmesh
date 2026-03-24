/**
 * Client-side terrain elevation sampler.
 * Decodes terrarium-encoded PNG tiles served from /terrain-tiles/{z}/{x}/{y}.png.
 * Terrarium encoding: elevation = R*256 + G + B/256 - 32768
 */

const TILE_SIZE = 512;
const SAMPLE_ZOOM = 11;  // Good balance: ~20m/px, covers UK at z5-z12

// Simple LRU-ish tile cache — keyed as "z/x/y"
const tileCache = new Map<string, ImageData>();
const MAX_CACHE = 200;

function lngLatToTileXY(lng: number, lat: number, z: number): [number, number] {
  const n = 2 ** z;
  const x = Math.floor((lng + 180) / 360 * n);
  const latRad = lat * Math.PI / 180;
  const y = Math.floor(
    (1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n,
  );
  return [x, y];
}

function lngLatToPixelWithinTile(
  lng: number, lat: number, z: number, tx: number, ty: number,
): [number, number] {
  const n = 2 ** z;
  const px = Math.floor(((lng + 180) / 360 * n - tx) * TILE_SIZE);
  const latRad = lat * Math.PI / 180;
  const py = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n - ty) * TILE_SIZE,
  );
  return [
    Math.max(0, Math.min(TILE_SIZE - 1, px)),
    Math.max(0, Math.min(TILE_SIZE - 1, py)),
  ];
}

async function fetchTile(z: number, x: number, y: number): Promise<ImageData | null> {
  const key = `${z}/${x}/${y}`;
  if (tileCache.has(key)) return tileCache.get(key)!;

  try {
    const resp = await fetch(`/terrain-tiles/${z}/${x}/${y}.png`);
    if (!resp.ok) return null;
    const blob = await resp.blob();
    const img = await createImageBitmap(blob);
    const canvas = document.createElement('canvas');
    canvas.width = TILE_SIZE;
    canvas.height = TILE_SIZE;
    const ctx = canvas.getContext('2d')!;
    ctx.drawImage(img, 0, 0);
    const data = ctx.getImageData(0, 0, TILE_SIZE, TILE_SIZE);
    if (tileCache.size >= MAX_CACHE) {
      tileCache.delete(tileCache.keys().next().value!);
    }
    tileCache.set(key, data);
    return data;
  } catch {
    return null;
  }
}

function decodeTerrarium(r: number, g: number, b: number): number {
  return r * 256 + g + b / 256 - 32768;
}

function sampleTile(tile: ImageData, px: number, py: number): number {
  const i = (py * TILE_SIZE + px) * 4;
  return decodeTerrarium(tile.data[i], tile.data[i + 1], tile.data[i + 2]);
}

/**
 * Sample elevation at a single point. Returns 0 if tile not available.
 */
export async function sampleElevationAt(lng: number, lat: number): Promise<number> {
  const [tx, ty] = lngLatToTileXY(lng, lat, SAMPLE_ZOOM);
  const tile = await fetchTile(SAMPLE_ZOOM, tx, ty);
  if (!tile) return 0;
  const [px, py] = lngLatToPixelWithinTile(lng, lat, SAMPLE_ZOOM, tx, ty);
  return sampleTile(tile, px, py);
}

/**
 * Sample a terrain cross-section between two points.
 * Returns array of [lon, lat, elevation_m] for nSamples+1 points.
 * Tiles are prefetched in parallel before sampling.
 */
export async function sampleTerrainProfile(
  lng1: number, lat1: number,
  lng2: number, lat2: number,
  nSamples = 60,
): Promise<[number, number, number][]> {
  // Collect all tile keys needed
  const tileKeys = new Set<string>();
  for (let i = 0; i <= nSamples; i++) {
    const t = i / nSamples;
    const lng = lng1 + t * (lng2 - lng1);
    const lat = lat1 + t * (lat2 - lat1);
    const [tx, ty] = lngLatToTileXY(lng, lat, SAMPLE_ZOOM);
    tileKeys.add(`${SAMPLE_ZOOM}/${tx}/${ty}`);
  }

  // Fetch all tiles in parallel
  await Promise.all(
    Array.from(tileKeys).map((key) => {
      const [z, x, y] = key.split('/').map(Number);
      return fetchTile(z, x, y);
    }),
  );

  // Sample each point synchronously from cached tiles
  const profile: [number, number, number][] = [];
  for (let i = 0; i <= nSamples; i++) {
    const t = i / nSamples;
    const lng = lng1 + t * (lng2 - lng1);
    const lat = lat1 + t * (lat2 - lat1);
    const [tx, ty] = lngLatToTileXY(lng, lat, SAMPLE_ZOOM);
    const tile = tileCache.get(`${SAMPLE_ZOOM}/${tx}/${ty}`);
    let elev = 0;
    if (tile) {
      const [px, py] = lngLatToPixelWithinTile(lng, lat, SAMPLE_ZOOM, tx, ty);
      elev = sampleTile(tile, px, py);
    }
    profile.push([lng, lat, Math.max(0, elev)]);
  }

  return profile;
}
