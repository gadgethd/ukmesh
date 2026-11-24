import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open, realpath } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { Router, type Response } from 'express';
import { publicRfGeneration, type RfPosition, type RfVisibility } from './visibility.js';

type Document = Record<string, unknown>;
type Tier = 'standard' | 'precision';
const tiers = ['standard', 'precision'] as const;
const nodeKey = /^[0-9a-f]{64}$/;
const datasetId = /^n[0-9a-f]{24}$/;
const filename = /^\d{1,5}-\d{1,5}\.png$/;
const digest = /^[0-9a-f]{64}$/;
const runId = /^[a-zA-Z0-9._-]{1,100}$/;
const pngSignature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
const manifestLimit = 2 * 1024 * 1024;
const pngLimit = 5 * 1024 * 1024;
const progressLimit = 16 * 1024;
const maxArchives = 128;
const maxConcurrentReads = 8;

const stages: Readonly<Record<string, string>> = Object.freeze({
  fetching_repeaters: 'Loading repeaters',
  loading_terrain: 'Loading terrain',
  computing_coverage: 'Computing Standard coverage',
  loading_precision_terrain: 'Loading Precision terrain',
  computing_coverage_precision: 'Computing Precision coverage',
  waiting_for_gpu: 'Waiting for the remote GPU worker',
  done: 'Coverage ready',
  error: 'Coverage calculation interrupted',
});

type Tile = {
  image: string;
  bounds: { North: number; South: number; West: number; East: number };
  sha256: string;
};
type Archive = {
  key: string;
  id: string;
  data: Document;
  product: Document;
  generation: number;
  position: RfPosition;
};

function document(value: unknown): Document {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Document : {};
}

function finite(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function text(value: unknown, max = 256): string | undefined {
  return typeof value === 'string' && value.length <= max ? value : undefined;
}

function timestamp(value: unknown): string | undefined {
  const string = text(value, 64);
  return string !== undefined && Number.isFinite(Date.parse(string)) ? string : undefined;
}

function count(value: unknown): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

function positionOf(value: Document): RfPosition | null {
  const { lat, lon } = value;
  return finite(lat) && finite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180
    ? { lat, lon } : null;
}

function samePosition(actual: RfPosition | undefined, expected: RfPosition | null): boolean {
  return expected !== null && actual?.lat === expected.lat && actual.lon === expected.lon;
}

async function readFileWithin(root: string, path: string, limit: number): Promise<Buffer> {
  const target = join(root, path);
  if (await realpath(target) !== target) throw new Error('RF_ARTIFACT_PATH_UNAVAILABLE');
  const file = await open(target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size < 1 || stat.size > limit) throw new Error('RF_ARTIFACT_SIZE_UNAVAILABLE');
    const result = Buffer.alloc(stat.size);
    for (let offset = 0; offset < result.length;) {
      const { bytesRead } = await file.read(result, offset, result.length - offset, offset);
      if (bytesRead === 0) throw new Error('RF_ARTIFACT_CHANGED');
      offset += bytesRead;
    }
    return result;
  } finally {
    await file.close();
  }
}

async function readDocument(root: string, path: string, limit: number): Promise<Document> {
  return document(JSON.parse((await readFileWithin(root, path, limit)).toString('utf8')));
}

function tilesFor(product: Document, prefix: string, generation: number): Tile[] {
  if (!Array.isArray(product['tiles'])) return [];
  const result: Tile[] = [];
  const seen = new Set<string>();
  for (const tile of product['tiles'].map(document)) {
    const proof = document(tile['public_rf_proof']);
    const { North, South, West, East } = document(tile['bounds']);
    const { image } = tile;
    const sha256 = proof['sha256'];
    if (publicRfGeneration(proof['visibility_generation']) !== generation
      || typeof image !== 'string' || !image.startsWith(prefix) || !filename.test(image.slice(prefix.length))
      || typeof sha256 !== 'string' || !digest.test(sha256)
      || !finite(North) || !finite(South) || !finite(West) || !finite(East)
      || South < -90 || North > 90 || West < -180 || East > 180 || South >= North || West >= East) continue;
    if (seen.has(image)) throw new Error('RF_DUPLICATE_IMAGE');
    seen.add(image);
    result.push({ image, bounds: { North, South, West, East }, sha256 });
  }
  return result;
}

function projectProduct(data: Document, tiles: Tile[]) {
  if (tiles.length === 0) return undefined;
  const assumptions = document(data['assumptions']);
  const finiteField = (value: unknown) => finite(value) ? value : undefined;
  return {
    tiles: tiles.map(({ image, bounds }) => ({ image, bounds })),
    frequency_mhz: finiteField(data['frequency_mhz']),
    max_search_range_km: finiteField(data['max_search_range_km']),
    dem_zoom_level: finiteField(data['dem_zoom_level']),
    generated_at: timestamp(data['generated_at']),
    assumptions: {
      tx_power_dbm: finiteField(assumptions['tx_power_dbm']),
      tx_antenna_gain_dbi: finiteField(assumptions['tx_antenna_gain_dbi']),
      rx_antenna_gain_dbi: finiteField(assumptions['rx_antenna_gain_dbi']),
      rx_sensitivity_dbm: finiteField(assumptions['rx_sensitivity_dbm']),
      fade_margin_db: finiteField(assumptions['fade_margin_db']),
      antenna_height_m: finiteField(assumptions['antenna_height_m']),
      rx_height_m: finiteField(assumptions['rx_height_m']),
      note: text(assumptions['note'], 512),
    },
  };
}

function networkTiles(meta: Document, tier: Tier, generation: number): Tile[] {
  return tilesFor(document(document(meta['coverage'])[tier]), `tiles/${tier}/`, generation);
}

function archiveEntries(meta: Document, generation: number): Archive[] {
  const entries = Object.entries(document(meta['node_coverage']));
  if (entries.length > maxArchives) throw new Error('RF_ARCHIVE_LIMIT');
  const result: Archive[] = [];
  for (const [key, value] of entries) {
    if (!nodeKey.test(key)) continue;
    const data = document(value);
    const product = document(data['standard']);
    const proof = document(product['public_rf_node']);
    const archiveGeneration = publicRfGeneration(proof['visibility_generation']);
    const position = positionOf(proof);
    const id = text(proof['dataset_id']);
    if (archiveGeneration === null || archiveGeneration > generation || proof['key'] !== key || position === null
      || !id || !datasetId.test(id)) continue;
    result.push({ key, id, data, product, generation: archiveGeneration, position });
  }
  return result;
}

function projectManifest(meta: Document, visibility: RfVisibility, archives: Archive[]) {
  const coverage: Document = {};
  for (const tier of tiers) {
    const product = projectProduct(document(document(meta['coverage'])[tier]), networkTiles(meta, tier, visibility.generation));
    if (product) coverage[tier] = product;
  }

  const nodeCoverage: Document = {};
  let archiveTimestamp: string | undefined;
  for (const archive of archives) {
    if (!samePosition(visibility.positions.get(archive.key), archive.position)) continue;
    const { id } = archive;
    const standard = projectProduct(archive.product, tilesFor(archive.product, `tiles/nodes/${id}/`, archive.generation));
    if (!standard) continue;
    if (standard.generated_at && (!archiveTimestamp || standard.generated_at > archiveTimestamp)) {
      archiveTimestamp = standard.generated_at;
    }
    const status = archive.data['position_status'];
    nodeCoverage[archive.key] = {
      dataset_id: id,
      run_id: `node-${id}`,
      state: 'available',
      position_status: status === 'active' || status === 'degraded' ? status : 'silent',
      last_heard: timestamp(archive.data['last_heard']),
      ...archive.position,
      requested_at: timestamp(archive.data['requested_at']),
      updated_at: timestamp(archive.data['updated_at']),
      fresh_until: timestamp(archive.data['fresh_until']),
      completed_tiles: standard.tiles.length,
      total_tiles: standard.tiles.length,
      standard,
    };
  }

  const manifestCurrent = publicRfGeneration(meta['visibility_generation']) === visibility.generation;
  if (!manifestCurrent && Object.keys(nodeCoverage).length === 0 && Object.keys(coverage).length === 0) {
    throw new Error('RF_PUBLICATION_STALE');
  }
  const rawRun = document(meta['run']);
  const id = text(rawRun['id'], 100);
  return {
    source: 'HopReach',
    version: text(meta['version']),
    generated_at: (manifestCurrent ? timestamp(meta['generated_at']) : archiveTimestamp) ?? new Date(0).toISOString(),
    complete: manifestCurrent && meta['complete'] === true,
    visibility_generation: visibility.generation,
    coverage,
    node_coverage: nodeCoverage,
    run: manifestCurrent && id && runId.test(id) ? {
      id: `${id}-g${visibility.generation}`,
      started_at: timestamp(rawRun['started_at']),
      model: 'HopReach terrain-aware RF coverage',
      source_version: text(rawRun['source_version']),
    } : undefined,
  };
}

function projectProgress(progress: Document, generation: number) {
  const id = text(progress['run_id'], 100);
  const stage = text(progress['stage'], 64);
  if (publicRfGeneration(progress['visibility_generation']) !== generation
    || !id || !runId.test(id) || !stage || !Object.hasOwn(stages, stage)) throw new Error('RF_PROGRESS_STALE');
  const backend = progress['backend'];
  const percent = progress['percent'];
  const eta = progress['eta_seconds'];
  return {
    visibility_generation: generation,
    run_id: `${id}-g${generation}`,
    stage,
    done: count(progress['done']),
    total: count(progress['total']),
    percent: finite(percent) ? Math.min(100, Math.max(0, percent)) : 0,
    message: stages[stage],
    updated_at: timestamp(progress['updated_at']),
    eta_seconds: finite(eta) && eta >= 0 ? eta : undefined,
    backend: backend === 'remote_gpu' || backend === 'gpu' || backend === 'cpu' ? backend : undefined,
  };
}

function respond(res: Response, status: number, body: Buffer | object) {
  res.status(status);
  res.setHeader('Cache-Control', 'private, no-store, max-age=0');
  res.setHeader('CDN-Cache-Control', 'no-store');
  res.setHeader('Content-Type', Buffer.isBuffer(body) ? 'image/png' : 'application/json; charset=utf-8');
  // This GET cannot turn into a cached 304, including conditional requests
  // carrying an ETag from another handler on the same host.
  res.end(Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

export function createPublicRfProjectionRoutes(options: {
  outputDirectory: string;
  readVisibility: (keys: readonly string[]) => Promise<RfVisibility>;
}): Router {
  const router = Router();
  const readVisibility = options.readVisibility;
  const output = resolve(options.outputDirectory);
  let activeRequests = 0;
  const serve = (handler: (root: string, initial: RfVisibility) => Promise<Buffer | object | null>) =>
    async (_req: unknown, res: Response) => {
      if (activeRequests >= maxConcurrentReads) {
        respond(res, 503, { error: 'RF coverage temporarily unavailable' });
        return;
      }
      activeRequests += 1;
      try {
        const initial = await readVisibility([]);
        const root = await realpath(output);
        if (root !== output) throw new Error('RF_OUTPUT_UNAVAILABLE');
        const result = await handler(root, initial);
        if (result === null) respond(res, 404, { error: 'RF coverage not found' });
        else respond(res, 200, result);
      } catch {
        respond(res, 503, { error: 'RF coverage temporarily unavailable' });
      } finally {
        if (res.closed || res.writableFinished) activeRequests -= 1;
        else {
          const release = () => {
            res.off('finish', release);
            res.off('close', release);
            activeRequests -= 1;
          };
          res.once('finish', release);
          res.once('close', release);
        }
      }
    };
  async function current(initial: RfVisibility, keys: readonly string[] = []): Promise<RfVisibility> {
    const verified = await readVisibility(keys);
    if (initial.generation !== verified.generation) throw new Error('RF_VISIBILITY_CHANGED');
    return verified;
  }
  router.get('/meta.json', serve(async (root, initial) => {
    const meta = await readDocument(root, 'meta.json', manifestLimit);
    const archives = archiveEntries(meta, initial.generation);
    return projectManifest(meta, await current(initial, archives.map((entry) => entry.key)), archives);
  }));
  router.get('/progress.json', serve(async (root, initial) => {
    const raw = await readDocument(root, 'progress.json', progressLimit);
    return projectProgress(raw, (await current(initial)).generation);
  }));

  function sendTile(image: string, tier: Tier | null, archiveId: string | null) {
    return serve(async (root, initial) => {
      const meta = await readDocument(root, 'meta.json', manifestLimit);
      let archive: Archive | undefined;
      let tile: Tile | undefined;
      if (tier !== null) {
        tile = networkTiles(meta, tier, initial.generation).find((item) => item.image === image);
      } else {
        const matches = archiveEntries(meta, initial.generation)
          .filter((entry) => entry.id === archiveId);
        if (matches.length === 1) {
          archive = matches[0];
          if (archive) {
            tile = tilesFor(archive.product, `tiles/nodes/${archiveId}/`, archive.generation)
              .find((item) => item.image === image);
          }
        }
      }
      if (!tile) return null;
      if (archive && !samePosition((await current(initial, [archive.key])).positions.get(archive.key), archive.position)) {
        return null;
      }

      const body = await readFileWithin(root, image, pngLimit);
      if (!body.subarray(0, pngSignature.length).equals(pngSignature)
        || createHash('sha256').update(body).digest('hex') !== tile.sha256) throw new Error('RF_ARTIFACT_MISMATCH');
      const authorised = await current(initial, archive ? [archive.key] : []);
      if (archive && !samePosition(authorised.positions.get(archive.key), archive.position)) return null;
      return body;
    });
  }

  router.get('/tiles/:tier/:file', (req, res) => {
    const { tier, file } = req.params;
    if ((tier !== 'standard' && tier !== 'precision') || !filename.test(file)) {
      respond(res, 404, { error: 'RF coverage not found' });
      return;
    }
    return sendTile(`tiles/${tier}/${file}`, tier, null)(req, res);
  });
  router.get('/tiles/nodes/:dataset/:file', (req, res) => {
    const { dataset, file } = req.params;
    if (!datasetId.test(dataset) || !filename.test(file)) {
      respond(res, 404, { error: 'RF coverage not found' });
      return;
    }
    return sendTile(`tiles/nodes/${dataset}/${file}`, null, dataset)(req, res);
  });
  router.use((_req, res) => respond(res, 404, { error: 'RF coverage not found' }));
  return router;
}
