import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import pg from 'pg';
import { BASE_SCHEMA_BOOTSTRAP_APPROVAL, BASE_SCHEMA_BOOTSTRAP_ENV, runMigrations } from '../db/migrations.js';
import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { generateSampleDataset } from './generateSampleDataset.js';
import { loadSampleDataset } from './loadSampleDataset.js';
import { prepareSampleDataset } from './prepareSampleDataset.js';
import { assertLocalDatabaseUrl } from './sampleData.js';
import { refreshSampleRecent } from './refreshSampleRecent.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];

test('synthetic loader and derived-state preparation work on the complete migrated schema', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async (t) => {
  assertLocalDatabaseUrl(endpoint!);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `sample_pipeline_${process.pid}_${Date.now()}`;
  const url = new URL(endpoint!);
  url.pathname = `/${name}`;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 2 });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-synthetic-'));
  const previous = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  t.after(async () => {
    if (previous === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = previous;
    await pool.end();
    await admin.query(`DROP DATABASE ${name} WITH (FORCE)`);
    await admin.end();
    await rm(directory, { recursive: true, force: true });
  });
  await pool.query(await readFile(new URL('../db/schema/base.sql', import.meta.url), 'utf8'));
  await admin.query(await readFile(new URL('../../../scripts/bootstrap-backend-roles.sql', import.meta.url), 'utf8'));
  await admin.query(await readFile(new URL('../../../scripts/bootstrap-reader-roles.sql', import.meta.url), 'utf8'));
  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  await runMigrations(pool);
  assert.equal((await pool.query("SELECT relkind FROM pg_class WHERE oid='public.packets'::regclass")).rows[0].relkind, 'v');
  await generateSampleDataset(['--data-dir', directory, '--count', '25']);
  const args = ['--data-dir', directory, '--database-url', url.href];
  await loadSampleDataset([...args, '--reset', '--fast']);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets WHERE raw_hex IS NOT NULL AND observation_id IS NOT NULL')).rows[0].n, 25);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 0,
    'bulk import must not certify public visibility');
  await prepareSampleDataset(url.href);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.multibyte_path_facts WHERE fully_decoded AND visibility_ok')).rows[0].n, 25);
  // A policy change after loading must remain authoritative. Preparation uses
  // the actual generation and the existing privacy implementation.
  await pool.query("UPDATE core.nodes SET name='Synthetic private 🚫' WHERE node_id LIKE 'AABB%'");
  await prepareSampleDataset(url.href);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 0);
  // Reset must reach base relations, remove derived rows and permit a normal
  // trigger-maintained reload without relying on search_path.
  await loadSampleDataset([...args, '--reset']);
  await prepareSampleDataset(url.href);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packet_paths')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets WHERE packet_uid IS NOT NULL')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.ingest_observation_ledger')).rows[0].n, 25);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM core.private_node_prefixes')).rows[0].n, 0);
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p WHERE ${publicPacketPrivacySql('p')}`)).rows[0].n, 25);
  await refreshSampleRecent(url.href);
  await refreshSampleRecent(url.href);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM mesh.packets')).rows[0].n, 75,
    'the recent overlay must refresh a fixed set, not accumulate new packets');
  assert.equal((await pool.query(`SELECT COUNT(*)::int AS n FROM mesh.packets p
    WHERE p.time > NOW() - INTERVAL '5 minutes' AND payload->>'_local_fixture' = 'recent-v1'
      AND ${publicPacketPrivacySql('p')}`)).rows[0].n, 50);
});
