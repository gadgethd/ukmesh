import * as maplibregl from 'maplibre-gl';
import type { RfCoverageBounds } from '../../hooks/useRfCoverage.js';

const PROTOCOL = 'hopreach-rf';
const TILE_SIZE = 256;
const MAX_MERCATOR_LATITUDE = 85.0511287798066;

type SourceTile = {
  url: string;
  bounds: RfCoverageBounds;
};

type Dataset = {
  tiles: SourceTile[];
  bitmaps: Map<string, Promise<ImageBitmap | null>>;
  renderedTiles: Map<string, Promise<ImageBitmap>>;
  abortController: AbortController;
};

type TileCanvas = OffscreenCanvas | HTMLCanvasElement;

const datasets = new Map<string, Dataset>();
let protocolInstalled = false;
let nextDatasetId = 0;

function clampLatitude(latitude: number): number {
  return Math.max(-MAX_MERCATOR_LATITUDE, Math.min(MAX_MERCATOR_LATITUDE, latitude));
}

function normalizedMercatorY(latitude: number): number {
  const radians = clampLatitude(latitude) * Math.PI / 180;
  return (1 - Math.asinh(Math.tan(radians)) / Math.PI) / 2;
}

export function rfRasterTileBounds(z: number, x: number, y: number): RfCoverageBounds {
  const scale = 2 ** z;
  const latitude = (tileY: number) => Math.atan(Math.sinh(Math.PI * (1 - 2 * tileY / scale))) * 180 / Math.PI;
  return {
    West: x / scale * 360 - 180,
    East: (x + 1) / scale * 360 - 180,
    North: latitude(y),
    South: latitude(y + 1),
  };
}

export function rfRasterSourceBounds(tiles: SourceTile[]): [number, number, number, number] {
  return [
    Math.min(...tiles.map((tile) => tile.bounds.West)),
    Math.min(...tiles.map((tile) => tile.bounds.South)),
    Math.max(...tiles.map((tile) => tile.bounds.East)),
    Math.max(...tiles.map((tile) => tile.bounds.North)),
  ];
}

function destinationX(longitude: number, z: number, x: number): number {
  return ((((longitude + 180) / 360) * (2 ** z)) - x) * TILE_SIZE;
}

function destinationY(latitude: number, z: number, y: number): number {
  return ((normalizedMercatorY(latitude) * (2 ** z)) - y) * TILE_SIZE;
}

function createTileCanvas(): TileCanvas {
  if (typeof OffscreenCanvas !== 'undefined') return new OffscreenCanvas(TILE_SIZE, TILE_SIZE);
  const canvas = document.createElement('canvas');
  canvas.width = TILE_SIZE;
  canvas.height = TILE_SIZE;
  return canvas;
}

function canvasToImageBitmap(canvas: TileCanvas): Promise<ImageBitmap> {
  if (typeof OffscreenCanvas !== 'undefined' && canvas instanceof OffscreenCanvas) {
    return Promise.resolve(canvas.transferToImageBitmap());
  }
  return createImageBitmap(canvas);
}

function loadBitmap(dataset: Dataset, source: SourceTile): Promise<ImageBitmap | null> {
  const cached = dataset.bitmaps.get(source.url);
  if (cached) return cached;
  const pending = fetch(source.url, {
    cache: 'force-cache',
    credentials: 'same-origin',
    signal: dataset.abortController.signal,
  }).then(async (response) => {
    if (!response.ok) throw new Error(`${source.url} returned ${response.status}`);
    return createImageBitmap(await response.blob());
  }).catch((error: unknown) => {
    if (!dataset.abortController.signal.aborted) {
      console.warn('[rf-coverage] unable to load source tile', source.url, error);
    }
    return null;
  });
  dataset.bitmaps.set(source.url, pending);
  return pending;
}

async function renderRasterTile(dataset: Dataset, z: number, x: number, y: number): Promise<ImageBitmap> {
  const canvas = createTileCanvas();
  const context = canvas.getContext('2d');
  if (!context) throw new Error('2D canvas is unavailable for RF coverage');
  context.clearRect(0, 0, TILE_SIZE, TILE_SIZE);
  context.imageSmoothingEnabled = false;

  const tileBounds = rfRasterTileBounds(z, x, y);
  const overlapping = dataset.tiles.filter((source) => (
    source.bounds.West < tileBounds.East
    && source.bounds.East > tileBounds.West
    && source.bounds.South < tileBounds.North
    && source.bounds.North > tileBounds.South
  ));

  await Promise.all(overlapping.map(async (source) => {
    const bitmap = await loadBitmap(dataset, source);
    if (!bitmap) return;

    const west = Math.max(tileBounds.West, source.bounds.West);
    const east = Math.min(tileBounds.East, source.bounds.East);
    const north = Math.min(tileBounds.North, source.bounds.North);
    const south = Math.max(tileBounds.South, source.bounds.South);
    if (west >= east || south >= north) return;

    const sourceX = (longitude: number) => (
      (longitude - source.bounds.West) / (source.bounds.East - source.bounds.West) * bitmap.width
    );
    const sourceY = (latitude: number) => (
      (source.bounds.North - latitude) / (source.bounds.North - source.bounds.South) * bitmap.height
    );
    const dxWest = destinationX(west, z, x);
    const dxEast = destinationX(east, z, x);
    const sxWest = sourceX(west);
    const sxEast = sourceX(east);
    const syNorth = sourceY(north);
    const sySouth = sourceY(south);
    const dyNorth = destinationY(north, z, y);
    const dySouth = destinationY(south, z, y);
    // Each requested Web Mercator tile spans only a few degrees at the zooms
    // used by this overlay. One draw per intersecting source keeps conversion
    // fast; MapLibre then owns terrain draping of the resulting XYZ tile.
    context.drawImage(
      bitmap,
      sxWest,
      syNorth,
      sxEast - sxWest,
      sySouth - syNorth,
      dxWest,
      dyNorth,
      dxEast - dxWest,
      dySouth - dyNorth,
    );
  }));

  return canvasToImageBitmap(canvas);
}

function parseTileUrl(url: string): { datasetId: string; z: number; x: number; y: number } | null {
  const match = url.match(/^hopreach-rf:\/\/([^/]+)\/(\d+)\/(\d+)\/(\d+)(?:\.png)?(?:\?.*)?$/);
  if (!match) return null;
  return {
    datasetId: match[1],
    z: Number(match[2]),
    x: Number(match[3]),
    y: Number(match[4]),
  };
}

function ensureProtocolInstalled(): void {
  if (protocolInstalled) return;
  maplibregl.addProtocol(PROTOCOL, async (parameters, abortController) => {
    const request = parseTileUrl(parameters.url);
    if (!request) throw new Error(`Invalid RF raster tile URL: ${parameters.url}`);
    const dataset = datasets.get(request.datasetId);
    if (!dataset) throw new Error(`RF raster dataset is no longer available: ${request.datasetId}`);
    if (abortController.signal.aborted) throw new DOMException('Aborted', 'AbortError');

    const cacheKey = `${request.z}/${request.x}/${request.y}`;
    let rendered = dataset.renderedTiles.get(cacheKey);
    if (!rendered) {
      rendered = renderRasterTile(dataset, request.z, request.x, request.y);
      dataset.renderedTiles.set(cacheKey, rendered);
    }
    const image = await rendered;
    if (abortController.signal.aborted) throw new DOMException('Aborted', 'AbortError');
    return { data: image };
  });
  protocolInstalled = true;
}

export function registerRfRasterDataset(
  tiles: SourceTile[],
  maxZoom: number,
): { tileTemplate: string; bounds: [number, number, number, number]; release: () => void } {
  ensureProtocolInstalled();
  const datasetId = `dataset-${++nextDatasetId}-z${maxZoom}`;
  const dataset: Dataset = {
    tiles,
    bitmaps: new Map(),
    renderedTiles: new Map(),
    abortController: new AbortController(),
  };
  datasets.set(datasetId, dataset);

  return {
    tileTemplate: `${PROTOCOL}://${datasetId}/{z}/{x}/{y}.png`,
    bounds: rfRasterSourceBounds(tiles),
    release: () => {
      if (!datasets.delete(datasetId)) return;
      dataset.abortController.abort();
      for (const bitmap of dataset.bitmaps.values()) {
        void bitmap.then((image) => image?.close());
      }
      for (const bitmap of dataset.renderedTiles.values()) {
        void bitmap.then((image) => image.close());
      }
      dataset.bitmaps.clear();
      dataset.renderedTiles.clear();
    },
  };
}

export function maxRfRasterZoom(tier: 'standard' | 'precision'): number {
  return tier === 'precision' ? 10 : 9;
}
