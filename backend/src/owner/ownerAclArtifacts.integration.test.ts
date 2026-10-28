import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { OWNER_ACL_RENDERER_VERSION, renderOwnerAcl } from '../mqtt/aclManager.js';
import { createOwnerAclArtifactStore, type OwnerAclArtifactInput } from './ownerAclArtifacts.js';

// An in-memory PostgreSQL-WASM fixture; this suite never opens a server connection.
const moduleUrl = process.env['TEST_OWNER_ACL_PGLITE_MODULE'];

test('real owner ACL artifact SQL preserves certificates only for the same verified bytes', {
  skip: moduleUrl ? false : 'TEST_OWNER_ACL_PGLITE_MODULE is not configured',
  timeout: 30_000,
}, async (t) => {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  t.after(() => db.close());
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
