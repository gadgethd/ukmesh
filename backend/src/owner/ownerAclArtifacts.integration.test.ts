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

test('real owner ACL artifact SQL preserves certificates only for the same verified bytes', {
  skip: databaseUrl || moduleUrl ? false : 'No owner ACL SQL test fixture is configured',
  timeout: 30_000,
}, async (t) => {
  const db = databaseUrl ? await (async () => {
    const client = new pg.Client({ connectionString: databaseUrl });
    await client.connect();
    const schema = `owner_acl_artifacts_${randomUUID().replaceAll('-', '')}`;
    t.after(async () => {
      try {
        await client.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      } finally {
        await client.end();
      }
    });
    await client.query(`CREATE SCHEMA ${schema}`);
    await client.query(`SET search_path TO ${schema}`);
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
  await db.exec(fs.readFileSync(new URL('../db/owner-auth.sql', import.meta.url), 'utf8'));
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
    const before = await db.query('SELECT applied_at FROM owner_acl_artifacts');
    await store.save(artifact);
    const after = await db.query('SELECT applied_at FROM owner_acl_artifacts');
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
      const result = await db.query('SELECT applied_at FROM owner_acl_artifacts');
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
