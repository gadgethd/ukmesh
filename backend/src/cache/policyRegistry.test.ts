import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { CACHE_POLICY_REGISTRY } from './policyRegistry.js';

function sourceFiles(root: string): string[] {
  const files: string[] = [];
  const pending = [root];
  while (pending.length > 0) {
    const directory = pending.pop()!;
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) pending.push(target);
      else if (entry.isFile() && entry.name.endsWith('.ts') && !entry.name.endsWith('.test.ts')) {
        files.push(target);
      }
    }
  }
  return files;
}

test('every process data cache is bounded or explicitly request-local', () => {
  const srcRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
  const discovered = new Set<string>();
  for (const file of sourceFiles(srcRoot)) {
    const relative = path.relative(path.dirname(srcRoot), file).replaceAll(path.sep, '/');
    const contents = fs.readFileSync(file, 'utf8');
    const pattern = /\bconst\s+([A-Za-z_$][\w$]*)\s*=\s*new\s+(BoundedTtlMap|Map)\b/g;
    for (const match of contents.matchAll(pattern)) {
      const name = match[1]!;
      const implementation = match[2]!;
      if (implementation === 'BoundedTtlMap' || /cache/i.test(name)) {
        discovered.add(`${relative}#${name}`);
      }
    }
  }

  const registered = new Set(CACHE_POLICY_REGISTRY.map((record) => record.source));
  assert.deepEqual([...discovered].filter((source) => !registered.has(source)), []);
  assert.equal(registered.size, CACHE_POLICY_REGISTRY.length, 'registry sources must be unique');
  for (const policy of CACHE_POLICY_REGISTRY) {
    assert.ok(discovered.has(policy.source), `stale cache policy: ${policy.source}`);
    assert.ok(policy.scope);
    assert.ok(policy.invalidation);
    assert.ok(policy.negativeCaching);
    assert.ok(policy.singleFlight);
    if (policy.disposition === 'bounded-cache') {
      assert.ok((policy.maxEntries ?? 0) > 0);
      assert.ok((policy.maxBytes ?? 0) > 0);
      assert.ok((policy.ttlMs ?? -1) >= 0);
    }
  }
});
