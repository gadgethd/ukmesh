import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { OWNER_ACL_RENDERER_VERSION, renderOwnerAcl } from '../mqtt/aclManager.js';
import { createOwnerAclArtifactStore, type OwnerAclArtifactInput } from './ownerAclArtifacts.js';

// Only explicitly supplied test fixtures are used, never DATABASE_URL. CI uses
// its disposable PostgreSQL service; the in-memory WASM fixture remains useful
// for local checks without a server.
const databaseUrl = process.env['TEST_OWNER_ACL_DATABASE_URL'];
const moduleUrl = process.env['TEST_OWNER_ACL_PGLITE_MODULE'];
if (process.env['CI'] && !databaseUrl && !moduleUrl) {
  throw new Error('Owner ACL SQL tests require an explicit test fixture in CI');
}

// The owner tables were moved into the `core` namespace by P1. Apply the real
// migration that defines them rather than restating its DDL in a fixture that
// can drift again. 071 creates the namespace but also depends on the legacy
// base schema, so the namespace is created directly here.
const OWNER_TABLES_MIGRATION = '../db/migrations/073_owner_auth_tables.sql';

async function provisionOwnerSchema(
  db: { exec(sql: string): Promise<unknown> },
): Promise<void> {
  await db.exec('CREATE SCHEMA IF NOT EXISTS core');
  await db.exec(fs.readFileSync(new URL(OWNER_TABLES_MIGRATION, import.meta.url), 'utf8'));
}

test('real owner ACL artifact SQL preserves certificates only for the same verified bytes', {
  skip: databaseUrl || moduleUrl ? false : 'No owner ACL SQL test fixture is configured',
  timeout: 30_000,
}, async (t) => {
  const db = databaseUrl ? await (async () => {
    // The artifact queries are schema-qualified, so a throwaway search_path
    // cannot stand in for the `core` namespace. Provision a dedicated database
    // instead, matching the other schema-isolating integration fixtures.
    const admin = new pg.Client({ connectionString: databaseUrl });
    await admin.connect();
    const database = `owner_acl_artifacts_${randomUUID().replaceAll('-', '')}`;
    await admin.query(`CREATE DATABASE "${database}"`);
    const ownerUrl = new URL(databaseUrl);
    ownerUrl.pathname = `/${database}`;
    const client = new pg.Client({ connectionString: ownerUrl.toString() });
    t.after(async () => {
      await client.end().catch(() => undefined);
      await admin.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
      await admin.end();
    });
    await client.connect();
    return {
      exec: (sql: string) => client.query(sql),
      query: (sql: string, params?: unknown[]) => client.query(sql, params),
    };
  })() : await (async () => {
    const { PGlite } = await import(moduleUrl!);
    const fixture = await PGlite.create();
    t.after(() => fixture.close());
    return fixture;
  })();
  await provisionOwnerSchema(db);
  const store = createOwnerAclArtifactStore((sql, params) => db.query(sql, params));
  const rendered = renderOwnerAcl('', [{ mqttUsername: 'owner', nodeIds: ['A'.repeat(64)] }], []);
  const artifact: OwnerAclArtifactInput = {
    ...rendered, rendererVersion: OWNER_ACL_RENDERER_VERSION, mode: 'apply',
  };

  await t.test('rendering alone is not evidence of an applied policy', async () => {
    await store.save(artifact);
    assert.equal(await store.getApplied(artifact.generation), null);
    await store.save({ ...artifact, applied: true });
    assert.equal((await store.getApplied(artifact.generation))?.content, artifact.content);
    const before = await db.query('SELECT applied_at FROM core.owner_acl_artifacts');
    await store.save(artifact);
    const after = await db.query('SELECT applied_at FROM core.owner_acl_artifacts');
    assert.deepEqual(after.rows, before.rows);
  });

  await t.test('renderer, mode, content, hash or validation rewrites invalidate the old certificate', async () => {
    for (const mutation of [
      { rendererVersion: 'other-renderer' },
      { mode: 'shadow' as const },
      { content: `${artifact.content}# a different file\n` },
      { contentSha256: 'different' },
      { validation: { ok: false } },
    ]) {
      await store.save({ ...artifact, applied: true });
      await store.save({ ...artifact, ...mutation });
      assert.equal(await store.getApplied(artifact.generation), null);
      const result = await db.query('SELECT applied_at FROM core.owner_acl_artifacts');
      assert.equal(result.rows[0].applied_at, null);
    }
  });

  await t.test('a verified reapplication repairs stale renderer metadata and becomes stable', async () => {
    await store.save({ ...artifact, rendererVersion: 'stale', applied: true });
    await store.save(artifact);
    assert.equal(await store.getApplied(artifact.generation), null);
    await store.save({ ...artifact, applied: true });
    assert.deepEqual(await store.getApplied(artifact.generation), {
      generation: artifact.generation, rendererVersion: artifact.rendererVersion,
      content: artifact.content, contentSha256: artifact.contentSha256,
    });
    await store.save(artifact);
    assert.equal((await store.getApplied(artifact.generation))?.rendererVersion, OWNER_ACL_RENDERER_VERSION);
  });
});
