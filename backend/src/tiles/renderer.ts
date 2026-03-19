/**
 * Server-side node tile renderer.
 * Generates 256×256 RGBA PNG tiles showing node dots using only Node built-ins
 * (no native canvas/skia dependency — uses zlib for PNG deflate).
 */
import { deflate } from 'node:zlib';
import { promisify } from 'node:util';

const deflateAsync = promisify(deflate);

const TILE_SIZE = 256;
const BUFFER = 4;
const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
const SEVEN_DAYS_MS     =  7 * 24 * 60 * 60 * 1000;
const PROHIBITED_MARKER = '\u{1F6AB}'; // 🚫

// ── Privacy masking (FNV-1a hash, port of frontend/src/utils/pathing.ts) ─────

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function stablePointWithinMiles(lat: number, lon: number, seed: string): [number, number] {
  const radiusKm = 1.609344; // 1 mile
  const distanceUnit = hashSeed(`${seed}:distance`) / 0xffffffff;
  const bearingUnit  = hashSeed(`${seed}:bearing`)  / 0xffffffff;
  const distanceKm = Math.sqrt(distanceUnit) * radiusKm;
  const bearing    = bearingUnit * Math.PI * 2;
  const latRad     = lat * (Math.PI / 180);
  const dLat = (distanceKm / 111) * Math.cos(bearing);
  const lonScale = Math.max(0.01, Math.cos(latRad));
  const dLon = (distanceKm / (111 * lonScale)) * Math.sin(bearing);
  return [lat + dLat, lon + dLon];
}

// ── Tile math ─────────────────────────────────────────────────────────────────

function tileBounds(z: number, x: number, y: number) {
  const pow2 = Math.pow(2, z);
  const lonW = (x / pow2) * 360 - 180;
  const lonE = ((x + 1) / pow2) * 360 - 180;
  const nN = Math.PI - (2 * Math.PI * y)       / pow2;
  const nS = Math.PI - (2 * Math.PI * (y + 1)) / pow2;
  const latN = Math.atan(Math.sinh(nN)) * (180 / Math.PI);
  const latS = Math.atan(Math.sinh(nS)) * (180 / Math.PI);
  return { lonW, lonE, latN, latS };
}

function latToMercY(lat: number): number {
  const latRad = lat * (Math.PI / 180);
  return Math.log(Math.tan(Math.PI / 4 + latRad / 2));
}

// ── Pure-JS PNG encoder ───────────────────────────────────────────────────────

function crc32(buf: Uint8Array): number {
  const table = crc32Table();
  let crc = 0xffffffff;
  for (const b of buf) {
    crc = (crc >>> 8) ^ table[(crc ^ b) & 0xff]!;
  }
  return (crc ^ 0xffffffff) >>> 0;
}

let _crcTable: Uint32Array | null = null;
function crc32Table(): Uint32Array {
  if (_crcTable) return _crcTable;
  _crcTable = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    _crcTable[n] = c;
  }
  return _crcTable;
}

function pngChunk(type: string, data: Uint8Array): Uint8Array {
  const typeBytes = new TextEncoder().encode(type);
  const out = new Uint8Array(4 + 4 + data.length + 4);
  const view = new DataView(out.buffer);
  view.setUint32(0, data.length);
  out.set(typeBytes, 4);
  out.set(data, 8);
  const crcData = new Uint8Array(4 + data.length);
  crcData.set(typeBytes);
  crcData.set(data, 4);
  view.setUint32(8 + data.length, crc32(crcData));
  return out;
}

async function encodePng(pixels: Uint8Array, width: number, height: number): Promise<Buffer> {
  // PNG signature
  const sig = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]);

  // IHDR
  const ihdr = new Uint8Array(13);
  const ihdrView = new DataView(ihdr.buffer);
  ihdrView.setUint32(0, width);
  ihdrView.setUint32(4, height);
  ihdr[8] = 8;  // bit depth
  ihdr[9] = 6;  // color type: RGBA
  // bytes 10-12: compression, filter, interlace = 0

  // Raw scanlines with filter byte 0 prepended to each row
  const stride = width * 4;
  const filtered = new Uint8Array(height * (stride + 1));
  for (let y = 0; y < height; y++) {
    filtered[y * (stride + 1)] = 0; // filter type: None
    filtered.set(pixels.subarray(y * stride, (y + 1) * stride), y * (stride + 1) + 1);
  }

  const compressed = await deflateAsync(Buffer.from(filtered.buffer));

  const chunks = [
    sig,
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', new Uint8Array(compressed.buffer, compressed.byteOffset, compressed.byteLength)),
    pngChunk('IEND', new Uint8Array(0)),
  ];

  const total = chunks.reduce((sum, c) => sum + c.length, 0);
  const out = Buffer.allocUnsafe(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

// ── Software rasteriser ───────────────────────────────────────────────────────

function blendPixel(pixels: Uint8Array, px: number, py: number, r: number, g: number, b: number, a: number) {
  const xi = Math.round(px);
  const yi = Math.round(py);
  if (xi < 0 || xi >= TILE_SIZE || yi < 0 || yi >= TILE_SIZE) return;
  const idx = (yi * TILE_SIZE + xi) * 4;
  // Alpha-composite over existing pixel
  const srcA = a / 255;
  const dstA = pixels[idx + 3]! / 255;
  const outA = srcA + dstA * (1 - srcA);
  if (outA < 1e-6) return;
  pixels[idx]!     = Math.round((r * srcA + pixels[idx]!   * dstA * (1 - srcA)) / outA);
  pixels[idx + 1]! = Math.round((g * srcA + pixels[idx + 1]! * dstA * (1 - srcA)) / outA);
  pixels[idx + 2]! = Math.round((b * srcA + pixels[idx + 2]! * dstA * (1 - srcA)) / outA);
  pixels[idx + 3]! = Math.round(outA * 255);
}

/** Filled anti-aliased circle using radial distance */
function drawDot(pixels: Uint8Array, cx: number, cy: number, radius: number, r: number, g: number, b: number, a: number) {
  const r0 = radius;
  const x0 = Math.floor(cx - r0 - 1);
  const x1 = Math.ceil(cx + r0 + 1);
  const y0 = Math.floor(cy - r0 - 1);
  const y1 = Math.ceil(cy + r0 + 1);
  for (let py = y0; py <= y1; py++) {
    for (let px = x0; px <= x1; px++) {
      const dist = Math.hypot(px - cx, py - cy);
      // Soft edge: full alpha inside, fades over 1px at edge
      const alpha = Math.max(0, Math.min(1, r0 + 0.5 - dist));
      if (alpha > 0) {
        blendPixel(pixels, px, py, r, g, b, Math.round(a * alpha));
      }
    }
  }
}

/** Dashed circle ring */
function drawDashedCircle(
  pixels: Uint8Array, cx: number, cy: number, radius: number,
  r: number, g: number, b: number, a: number,
  dashLen = 4, gapLen = 6, lineWidth = 1.4,
) {
  const circumference = 2 * Math.PI * radius;
  const steps = Math.ceil(circumference * 3); // oversample for smooth curve
  const halfW = lineWidth / 2;
  for (let i = 0; i < steps; i++) {
    const angle = (2 * Math.PI * i) / steps;
    // Arc length position for dash/gap pattern
    const arcLen = (angle / (2 * Math.PI)) * circumference;
    const period = dashLen + gapLen;
    const phase  = arcLen % period;
    if (phase > dashLen) continue; // in gap
    const px = cx + Math.cos(angle) * radius;
    const py = cy + Math.sin(angle) * radius;
    drawDot(pixels, px, py, halfW, r, g, b, a);
  }
}

// ── Node type ─────────────────────────────────────────────────────────────────

export type NodeRow = {
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  role: number | null;
  last_seen: string;
  is_online: boolean;
};

function isValidCoord(lat: number | null, lon: number | null): lat is number {
  if (typeof lat !== 'number' || typeof lon !== 'number') return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 5 && Math.abs(lon) < 5) return false;
  return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

// RGBA components at 0.9 alpha (230/255) — mirrors App.tsx gpuNodes colour logic
const COLORS = {
  stale:     [255,  68,  68, 178] as [number, number, number, number], // red, 0.7 alpha
  offline:   [100, 100, 100, 230] as [number, number, number, number],
  companion: [251, 146,  60, 230] as [number, number, number, number],
  room:      [168,  85, 247, 230] as [number, number, number, number],
  repeater:  [ 34, 211, 238, 230] as [number, number, number, number],
} as const;

function nodeColor(node: NodeRow, now: number): [number, number, number, number] {
  if (now - new Date(node.last_seen).getTime() > SEVEN_DAYS_MS) return COLORS.stale;
  if (!node.is_online) return COLORS.offline;
  if (node.role === 1)  return COLORS.companion;
  if (node.role === 3)  return COLORS.room;
  return COLORS.repeater;
}

// ── Empty tile (pre-computed once, reused for every tile with no nodes) ───────

// Lazily computed so module load stays fast.
let _emptyTile: Promise<Buffer> | null = null;
function emptyTile(): Promise<Buffer> {
  if (_emptyTile) return _emptyTile;
  _emptyTile = encodePng(new Uint8Array(TILE_SIZE * TILE_SIZE * 4), TILE_SIZE, TILE_SIZE);
  return _emptyTile;
}

// ── Tile index (bucket nodes by tile coordinate per zoom, computed once) ──────

export type TileIndex = {
  /** z → Map<"x:y", nodes that fall in that tile> */
  byZoom:     Map<number, Map<string, NodeRow[]>>;
  /** Privacy nodes — included in every tile render (very rare, usually <5) */
  prohibited: NodeRow[];
};

function nodeTileXY(lat: number, lon: number, z: number): [number, number] {
  const pow2 = Math.pow(2, z);
  const x = Math.floor((lon + 180) / 360 * pow2);
  const latRad = lat * Math.PI / 180;
  const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * pow2);
  return [x, y];
}

/**
 * Pre-bucket valid, non-stale nodes by tile coordinate for every zoom in
 * [zMin, zMax].  Call once per network, then pass the result to
 * renderTileFromIndex for fast per-tile rendering.
 */
export function buildTileIndex(nodes: NodeRow[], zMin: number, zMax: number): TileIndex {
  const byZoom     = new Map<number, Map<string, NodeRow[]>>();
  const prohibited: NodeRow[] = [];
  const now = Date.now();

  // Initialise zoom maps
  for (let z = zMin; z <= zMax; z++) byZoom.set(z, new Map());

  for (const node of nodes) {
    if (!isValidCoord(node.lat, node.lon)) continue;
    if (now - new Date(node.last_seen).getTime() > FOURTEEN_DAYS_MS) continue;

    if (node.name?.includes(PROHIBITED_MARKER)) {
      prohibited.push(node);
      continue;
    }

    for (let z = zMin; z <= zMax; z++) {
      const [x, y] = nodeTileXY(node.lat, node.lon!, z);
      const key = `${x}:${y}`;
      const zMap = byZoom.get(z)!;
      const bucket = zMap.get(key);
      if (bucket) bucket.push(node);
      else zMap.set(key, [node]);
    }
  }

  return { byZoom, prohibited };
}

/**
 * Render a tile using a pre-built index. Returns the cached empty PNG
 * immediately when there are no nodes to draw.
 */
export function renderTileFromIndex(z: number, x: number, y: number, index: TileIndex): Promise<Buffer> {
  const tileNodes = index.byZoom.get(z)?.get(`${x}:${y}`);
  if (!tileNodes?.length && !index.prohibited.length) return emptyTile();
  const combined = tileNodes ? [...tileNodes, ...index.prohibited] : index.prohibited;
  return renderNodeTile(z, x, y, combined);
}

// ── Main export ───────────────────────────────────────────────────────────────

export async function renderNodeTile(z: number, x: number, y: number, nodes: NodeRow[]): Promise<Buffer> {
  const { lonW, lonE, latN, latS } = tileBounds(z, x, y);
  const lonRange  = lonE - lonW;
  const mercN     = latToMercY(latN);
  const mercS     = latToMercY(latS);
  const mercRange = mercN - mercS;
  const pow2z     = Math.pow(2, z);
  const now       = Date.now();

  // Transparent pixel buffer: TILE_SIZE × TILE_SIZE × RGBA
  const pixels = new Uint8Array(TILE_SIZE * TILE_SIZE * 4); // all zeros = transparent

  const DOT_R = 3.5;

  for (const node of nodes) {
    if (!isValidCoord(node.lat, node.lon)) continue;
    if (now - new Date(node.last_seen).getTime() > FOURTEEN_DAYS_MS) continue;

    const isProhibited = Boolean(node.name?.includes(PROHIBITED_MARKER));

    let dotLat = node.lat;
    let dotLon = node.lon!;
    let circleLat: number | null = null;
    let circleLon: number | null = null;

    if (isProhibited) {
      const center = stablePointWithinMiles(node.lat, node.lon!, node.node_id);
      const activityKey = node.last_seen ?? 'unknown';
      const point = stablePointWithinMiles(center[0], center[1], `${node.node_id}|${activityKey}`);
      circleLat = center[0];
      circleLon = center[1];
      dotLat = point[0];
      dotLon = point[1];
    }

    // Compute pixel position for the dot
    const mercY = latToMercY(dotLat);
    const px = (dotLon - lonW) / lonRange * TILE_SIZE;
    const py = (mercN - mercY) / mercRange * TILE_SIZE;

    const [r, g, b, a] = nodeColor(node, now);

    // Draw dashed circle for privacy nodes
    if (isProhibited && circleLat !== null && circleLon !== null) {
      const cMercY = latToMercY(circleLat);
      const cpx = (circleLon - lonW) / lonRange * TILE_SIZE;
      const cpy = (mercN - cMercY) / mercRange * TILE_SIZE;

      const metersPerPixel = 156543.03 * Math.cos(circleLat * Math.PI / 180) / pow2z;
      const radiusPx = 1609.344 / metersPerPixel;

      if (radiusPx >= 4) {
        drawDashedCircle(pixels, cpx, cpy, radiusPx, 245, 158, 11, 140);
      }
    }

    // Skip dot if outside tile (with buffer)
    if (px < -BUFFER || px > TILE_SIZE + BUFFER || py < -BUFFER || py > TILE_SIZE + BUFFER) continue;

    drawDot(pixels, px, py, DOT_R, r, g, b, a);
    // Stroke ring: slightly darker
    drawDot(pixels, px, py, DOT_R + 0.8, Math.round(r * 0.65), Math.round(g * 0.65), Math.round(b * 0.65), Math.round(a * 0.85));
  }

  return encodePng(pixels, TILE_SIZE, TILE_SIZE);
}
