import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');

// Schema-scoped dumps silently omit Timescale hypertable/cagg contents.
// P1-1 proved the restore loses rows, so all production dumps must be full-DB.
const schemaScopedDump = /(?:^|\s)(?:pg_dump|pg_dumpall)\b[^\n]*?(?:\s-n\s|\s--schema(?:=|\s))/m;

function shellAndWorkflowFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) {
      out.push(...shellAndWorkflowFiles(path));
    } else if (/\.(sh|ya?ml)$/.test(entry)) {
      out.push(path);
    }
  }
  return out;
}

test('production dump scripts never use schema-scoped pg_dump', () => {
  const files = [
    ...shellAndWorkflowFiles(join(repoRoot, 'scripts')),
    ...shellAndWorkflowFiles(join(repoRoot, '.github', 'workflows')),
  ].filter((path) => readFileSync(path, 'utf8').includes('pg_dump'));

  assert.ok(files.some((path) => path.endsWith('scripts/backup.sh')), 'backup.sh must contain the production dump');

  for (const path of files) {
    const text = readFileSync(path, 'utf8');
    assert.doesNotMatch(text, schemaScopedDump, `${path} uses a schema-scoped pg_dump; use a full-database dump`);
  }
});
