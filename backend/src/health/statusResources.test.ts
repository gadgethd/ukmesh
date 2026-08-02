import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  measureConfiguredVolumes,
  measureDirectoryBytes,
  toPublicHealthOverview,
} from './status.js';

test('volume measurements use only configured data roots and expose unknown on incomplete scans', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'meshcore-health-volume-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const database = path.join(root, 'database');
  const unrelated = path.join(root, 'unrelated-root');
  fs.mkdirSync(database);
  fs.mkdirSync(unrelated);
  fs.writeFileSync(path.join(database, 'chunk'), Buffer.alloc(80));
  fs.writeFileSync(path.join(unrelated, 'noise'), Buffer.alloc(10_000));

  assert.deepEqual(measureDirectoryBytes(database), {
    bytes: 80,
    entries: 1,
    complete: true,
  });
  const volumes = measureConfiguredVolumes(`database=${database}=100`);
  assert.deepEqual(volumes['database'], {
    path: database,
    used_bytes: 80,
    budget_bytes: 100,
    used_pct: 80,
    complete: true,
  });
  assert.equal(JSON.stringify(volumes).includes('unrelated-root'), false);
  assert.deepEqual(measureDirectoryBytes(database, 0), {
    bytes: null,
    entries: 1,
    complete: false,
  });
});

test('public health is a coarse allowlist and omits operator platform evidence', () => {
  const detail = {
    status: 'degraded',
    problems: [{
      code: 'disk_pressure',
      severity: 'warning',
      message: 'secret mount /var/lib/postgresql is 81%',
    }],
    maintenance: { active: false, message: null },
    system: {
      generated_at: '2026-07-29T12:00:00.000Z',
      disk: { used_pct: 81, volumes: { database: { path: '/secret' } } },
      runtime: { node_version: 'secret-version' },
    },
    ingest: { packet_age_minutes: 1 },
    workers: [{ worker_name: 'link-worker', status: 'running' }],
    database: { connection_count: 123, cache_hit_ratio: 0.99 },
    redis: { appendonly: 'yes' },
  } as unknown as Parameters<typeof toPublicHealthOverview>[0];

  const publicHealth = toPublicHealthOverview(detail);
  assert.deepEqual(publicHealth, {
    status: 'degraded',
    generatedAt: '2026-07-29T12:00:00.000Z',
    maintenance: { active: false, message: null },
    incidents: [{ code: 'disk_pressure', severity: 'warning' }],
    components: {
      ingest: { status: 'ok' },
      workers: { status: 'running' },
      storage: { status: 'warning' },
    },
  });
  const encoded = JSON.stringify(publicHealth);
  for (const secret of ['/var/lib/postgresql', '/secret', 'secret-version', 'cache_hit_ratio']) {
    assert.equal(encoded.includes(secret), false);
  }
});
