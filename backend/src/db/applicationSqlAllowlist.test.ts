import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const srcRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const allowlist = JSON.parse(
  readFileSync(join(srcRoot, 'db', 'application-sql-allowlist.json'), 'utf8'),
) as { tables: string[]; files: Record<string, string[]> };

const tablePattern = new RegExp(
  `\\b(?:FROM|JOIN|INSERT\\s+INTO|UPDATE|DELETE\\s+FROM)\\s+(?:public\\.)?([a-z_][a-z0-9_]*)`,
  'i',
);
const tableNames = new Set(allowlist.tables);

function typescriptFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...typescriptFiles(path));
    else if (entry.endsWith('.ts')) out.push(path);
  }
  return out;
}

test('application SQL has no unreviewed unqualified target tables', () => {
  const unreviewed: string[] = [];
  for (const path of typescriptFiles(srcRoot)) {
    const file = relative(srcRoot, path).replaceAll('\\', '/');
    const text = readFileSync(path, 'utf8');
    const current = new Set<string>();
    for (const match of text.matchAll(new RegExp(tablePattern.source, 'gi'))) {
      const target = match[1];
      if (target && tableNames.has(target)) current.add(target);
    }
    const allowed = new Set(allowlist.files[file] ?? []);
    for (const table of current) {
      if (!allowed.has(table)) unreviewed.push(`${file}: ${table}`);
    }
  }

  assert.deepEqual(unreviewed, [], 'new unqualified SQL targets need reviewed allowlist entries');
});
