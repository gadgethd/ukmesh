import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test, { type TestContext } from 'node:test';
import express from 'express';
import { createPublicRfProjectionRoutes } from './publicProjection.js';
import type { RfPosition, RfVisibility } from './visibility.js';

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=', 'base64');
const hash = createHash('sha256').update(png).digest('hex');
const bounds = { South: 50, North: 51, West: -3, East: -2 };
const timestamp = '2026-11-24T00:00:00Z';
const PUBLIC = 'a'.repeat(64);
const PRIVATE = 'b'.repeat(64);
const MOVED = 'c'.repeat(64);
const LEGACY = 'd'.repeat(64);
const DATASET = 'n' + '1'.repeat(24);
const PENDING_DATASET = 'n' + '2'.repeat(24);
const NODE_TILE = `tiles/nodes/${DATASET}/0-0.png`;
const PRIVATE_TILE = `tiles/nodes/n${'3'.repeat(24)}/0-0.png`;
const MOVED_TILE = `tiles/nodes/n${'4'.repeat(24)}/0-0.png`;
const LEGACY_TILE = `tiles/nodes/n${'5'.repeat(24)}/0-0.png`;
const STANDARD = 'tiles/standard/0-0.png';
const STALE = 'tiles/standard/0-1.png';
const PRECISION = 'tiles/precision/1-2.png';

function tile(image: string, generation?: number) {
  return { image, bounds, public_rf_proof: generation === undefined ? undefined : { visibility_generation: generation, sha256: hash } };
}

function node(key: string, image: string, proof = true) {
  const id = image.split('/')[2];
  return { dataset_id: id, state: 'available', position_status: 'active', lat: 50, lon: -2,
    standard: { tiles: [tile(image, 7)], public_rf_node: proof
      ? { key, dataset_id: id, lat: 50, lon: -2, visibility_generation: 7 } : undefined } };
}

async function fixture(t: TestContext) {
  const root = await mkdtemp(join(tmpdir(), 'public-rf-projection-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const raw = {
    source: 'http://private-source.invalid',
    generated_at: timestamp,
    complete: true,
    version: 'test',
    visibility_generation: 9,
    run: { id: 'run-test', started_at: timestamp, failure: 'private exception' },
    coverage: { standard: { tiles: [tile(STANDARD, 9), tile(STALE, 8)] }, precision: { tiles: [tile(PRECISION, 9)] } },
    node_coverage: {
      [PUBLIC]: { ...node(PUBLIC, NODE_TILE), dataset_id: PENDING_DATASET, state: 'computing' },
      [PRIVATE]: node(PRIVATE, PRIVATE_TILE),
      [MOVED]: node(MOVED, MOVED_TILE),
      [LEGACY]: node(LEGACY, LEGACY_TILE, false),
    },
  };
  await writeFile(join(root, 'meta.json'), JSON.stringify(raw));
  await writeFile(join(root, 'progress.json'), JSON.stringify({
    run_id: 'run-test', visibility_generation: 9, stage: 'computing_coverage_precision',
    done: 1, total: 4, percent: 25, updated_at: timestamp, backend: 'remote_gpu',
    message: 'private terrain path and stream address',
  }));
  for (const image of [STANDARD, STALE, PRECISION, NODE_TILE, PRIVATE_TILE, MOVED_TILE, LEGACY_TILE]) {
    await mkdir(join(root, dirname(image)), { recursive: true });
    await writeFile(join(root, image), png);
  }
  const state = {
    generation: 9,
    calls: 0,
    beforeRead: undefined as ((call: number) => void) | undefined,
    positions: new Map<string, RfPosition>([[PUBLIC, { lat: 50, lon: -2 }], [MOVED, { lat: 50.5, lon: -2 }],
      [LEGACY, { lat: 50, lon: -2 }]]),
  };
  const app = express();
  app.use('/internal/rf', createPublicRfProjectionRoutes({
    outputDirectory: root,
    readVisibility: async (keys): Promise<RfVisibility> => {
      state.beforeRead?.(++state.calls);
      return { generation: state.generation, positions: new Map(keys.flatMap((key) => {
        const position = state.positions.get(key);
        return position ? [[key, position]] : [];
      })) };
    },
  }));
  const server = createServer(app);
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  t.after(() => new Promise<void>((done, fail) => {
    server.close((error) => error ? fail(error) : done());
  }));
  const address = server.address();
  assert(address && typeof address === 'object');
  const get = (path: string, init?: RequestInit) => fetch(`http://127.0.0.1:${address.port}/internal/rf/${path}`, init);
  return { root, state, get };
}

test('the RF projection publishes current network tiles and only exactly authorised solo archives', async (t) => {
  const { state, get } = await fixture(t);
  const response = await get('meta.json', { headers: { 'if-none-match': '*' } });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('cache-control') ?? '', /no-store/);
  assert.equal(response.headers.get('cdn-cache-control'), 'no-store');
  assert.equal(response.headers.has('etag'), false);
  const body = await response.text();
  assert(!body.includes('private-source') && !body.includes('private exception'));
  const meta = JSON.parse(body);
  assert.deepEqual(meta.coverage.standard.tiles, [{ image: STANDARD, bounds }]);
  assert.deepEqual(meta.coverage.precision.tiles, [{ image: PRECISION, bounds }]);
  assert.deepEqual(Object.keys(meta.node_coverage), [PUBLIC]);
  assert.equal(meta.node_coverage[PUBLIC].dataset_id, DATASET);

  for (const image of [STANDARD, PRECISION, NODE_TILE]) {
    const response = await get(image);
    assert.equal(response.status, 200, image);
    assert.equal(response.headers.get('content-type'), 'image/png');
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
  }
  for (const image of [STALE, PRIVATE_TILE, MOVED_TILE, LEGACY_TILE]) {
    assert.equal((await get(image)).status, 404, image);
  }

  const progress = await (await get('progress.json')).text();
  assert(progress.includes('Computing Precision coverage') && !progress.includes('private terrain'));
  state.generation = 10;
  assert.equal((await get(STANDARD)).status, 404);
  assert.equal((await get(PRECISION)).status, 404);
  assert.equal((await get('progress.json')).status, 503);
  assert.equal((await get(NODE_TILE)).status, 200);
  assert.deepEqual((await (await get('meta.json')).json() as { coverage: object }).coverage, {});
  state.positions.delete(PUBLIC);
  assert.equal((await get(NODE_TILE)).status, 404);
  assert.equal((await get('meta.json')).status, 503);
});

test('artifact replacement, symlink, and policy races fail closed before RF bytes are sent', async (t) => {
  const { root, state, get } = await fixture(t);
  const path = join(root, STANDARD);
  await writeFile(path, Buffer.concat([png, Buffer.from([0])]));
  assert.equal((await get(STANDARD)).status, 503);
  await rm(path);
  await symlink(join(root, PRECISION), path);
  assert.equal((await get(STANDARD)).status, 503);
  await rm(path);
  await writeFile(path, png);

  state.calls = 0;
  state.beforeRead = (call) => { if (call === 2) state.generation = 10; };
  const raced = await get(STANDARD);
  assert.equal(raced.status, 503);
  assert.notEqual(raced.headers.get('content-type'), 'image/png');
  assert.equal(state.calls, 2);
});

test('concurrent tile requests, manifest replacement and rewrites see the current proofs', async (t) => {
  const { root, get } = await fixture(t);
  const manifest = join(root, 'meta.json');
  const parallel = await Promise.all([STANDARD, STANDARD, PRECISION, NODE_TILE].map(async image => {
    const response = await get(image);
    assert.equal(response.status, 200);
    return Buffer.from(await response.arrayBuffer());
  }));
  assert.deepEqual(parallel, [png, png, png, png]);

  const changed = { visibility_generation: 9, coverage: { standard: { tiles: [tile(STALE, 9)] } } };
  const staging = join(root, 'meta.next.json');
  await writeFile(staging, JSON.stringify(changed));
  await rename(staging, manifest);
  assert.equal((await get(STANDARD)).status, 404);
  assert.equal((await get(STALE)).status, 200);

  changed.coverage.standard.tiles = [tile(STANDARD, 9)];
  await writeFile(manifest, JSON.stringify(changed));
  assert.equal((await get(STALE)).status, 404);
  assert.equal((await get(STANDARD)).status, 200);

  await writeFile(manifest, 'invalid manifest');
  assert.equal((await get(STANDARD)).status, 503);
  await writeFile(manifest, JSON.stringify(changed));
  assert.equal((await get(STANDARD)).status, 200);
});
