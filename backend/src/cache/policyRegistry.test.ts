import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { stripTypeScriptTypes } from 'node:module';
import test from 'node:test';
import { CACHE_POLICY_REGISTRY, REQUEST_LOCAL_CACHE_EXCLUSIONS } from './policyRegistry.js';

function sourceFiles(root: string): string[] {
  const files: string[] = [];
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const target = path.join(root, entry.name);
    if (entry.isDirectory()) files.push(...sourceFiles(target));
    else if (entry.isFile() && entry.name.endsWith('.ts') && !entry.name.endsWith('.test.ts')) files.push(target);
  }
  return files;
}

test('production storage uses executable policies and the inventory contains no duplicate limits', () => {
  const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
  const policies = CACHE_POLICY_REGISTRY.filter(policy => policy.disposition === 'bounded-cache');
  const discovered = new Map<string, number>();
  for (const file of sourceFiles(root)) {
    const source = path.relative(path.dirname(root), file).replaceAll(path.sep, '/');
    const contents = fs.readFileSync(file, 'utf8');
    if (!/\bnew\s+BoundedTtlMap\b/.test(contents)) continue;
    const code = stripTypeScriptTypes(contents);
    const constructors = [...code.matchAll(/\bnew\s+BoundedTtlMap\s*\(/g)];
    if (!constructors.length) continue;
    const definitions = policies.filter(policy => policy.source === source);
    if (!definitions.length && REQUEST_LOCAL_CACHE_EXCLUSIONS.some(policy => policy.source === source)) continue;
    assert.equal(constructors.length, definitions.length, `unreviewed storage in ${source}`);
    const references = [...code.matchAll(/\bnew\s+BoundedTtlMap\s*\(\s*[A-Z][A-Z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*\s*[,)]/g)];
    assert.equal(references.length, constructors.length, `${source} must use its reviewed policy directly`);
    discovered.set(source, constructors.length);
  }
  assert.equal(new Set(CACHE_POLICY_REGISTRY.map(policy => policy.id)).size, CACHE_POLICY_REGISTRY.length);
  for (const policy of policies) {
    assert.ok(discovered.has(policy.source), `unused policy: ${policy.id}`);
    assert.ok(Object.isFrozen(policy));
    assert.ok((policy.maxEntries ?? 0) > 0);
    assert.ok((policy.maxWeight ?? 0) > 0);
    assert.ok((policy.ttlMs ?? 0) > 0);
    assert.ok(policy.scope && policy.invalidation && policy.negativeCaching && policy.singleFlight);
  }
});
