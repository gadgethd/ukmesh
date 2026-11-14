import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rename, rm, symlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { lookupSourceIdentity } from './lookupSource.js';

test('source identity is stable across creation order and changes with bytes or relative paths', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'lookup-source-'));
  try {
    for (const name of ['one', 'two']) await mkdir(path.join(root, name));
    await writeFile(path.join(root, 'one/b.ts'), 'beta'); await writeFile(path.join(root, 'one/a.ts'), 'alpha');
    await writeFile(path.join(root, 'two/a.ts'), 'alpha'); await writeFile(path.join(root, 'two/b.ts'), 'beta');
    const initial = await lookupSourceIdentity(path.join(root, 'one'), '2b4613a');
    assert.equal(initial.sha256, (await lookupSourceIdentity(path.join(root, 'two'), '2b4613a')).sha256);
    await writeFile(path.join(root, 'two/a.ts'), 'edited');
    assert.notEqual(initial.sha256, (await lookupSourceIdentity(path.join(root, 'two'))).sha256);
    await rename(path.join(root, 'one/a.ts'), path.join(root, 'one/renamed.ts'));
    assert.notEqual(initial.sha256, (await lookupSourceIdentity(path.join(root, 'one'))).sha256);
    assert.equal(initial.files, 2);
  } finally { await rm(root, { recursive: true }); }
});

test('empty sources, invalid references and symlink escapes cannot be sealed', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'lookup-source-'));
  try {
    await assert.rejects(lookupSourceIdentity(root), /empty/);
    await assert.rejects(lookupSourceIdentity(root, 'branch;remote'), /revision/);
    await symlink('/etc/passwd', path.join(root, 'outside.ts'));
    await assert.rejects(lookupSourceIdentity(root), /symbolic link/);
  } finally { await rm(root, { recursive: true }); }
});
