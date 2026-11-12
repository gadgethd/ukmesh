import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

test('docs gate catches broken links, duplicate-heading anchors and nonexistent package scripts while exempting history', () => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'ukmesh-docs-'));
  const put = (name, text) => {
    mkdirSync(path.dirname(path.join(root, name)), { recursive: true });
    writeFileSync(path.join(root, name), text);
  };
  const run = () => spawnSync(process.execPath, [new URL('./check-docs.mjs', import.meta.url).pathname, '--root', root], { encoding: 'utf8' });
  try {
    put('README.md', '[Guide](docs/guide.md#same-1)\n[Live route](/stats)\n');
    put('INSTALL.md', 'See [README](README.md).');
    put('docs/guide.md', '# Same\n\n# Same\n');
    put('docs/reviews/old.md', '[Deleted](never-existed.md)\nbackend/src/db/ownerAuth.ts');
    put('docs/evidence-2026-10-01.md', '[Deleted](never-existed.md)');
    put('frontend/package.json', JSON.stringify({ scripts: { check: 'tsc' } }));
    assert.equal(run().status, 0);
    put('README.md', '[Guide](docs/guide.md#absent)\n[Missing](absent.md)');
    assert.match(run().stderr, /missing anchor/);
    assert.match(run().stderr, /missing relative link/);
    put('README.md', '```bash\ncd frontend\nnpm run nonexistent\n```');
    assert.match(run().stderr, /missing npm script nonexistent/);
    put('README.md', '`backend/src/db/ownerAuth.ts`');
    assert.match(run().stderr, /stale path/);
    put('README.md', 'Historical: `backend/src/db/ownerAuth.ts` was removed.');
    assert.equal(run().status, 0);
    put('README.md', '```bash\ndocker compose exec -e REVIEWED=true backend npm run db:lifecycle\n```');
    assert.match(run().stderr, /backend runtime image has no npm/);
    put('README.md', '```bash\ndocker compose exec backend node dist/tools/dataLifecycle.js\n```');
    assert.equal(run().status, 0);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
