// Migration 156's partial index is an access path for the spam message-window
// queries, not a semantic change. Pin the coupling: if a window query's type
// literal or window columns ever change, the index must change with them or it
// silently stops helping (replay tests alone would still pass).
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const srcRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const migration = readFileSync(join(srcRoot, 'db', 'migrations', '156_index_message_window.sql'), 'utf8');
const repository = readFileSync(join(srcRoot, 'spam', 'repository.ts'), 'utf8');

test('the message-window partial index matches every spam window query', () => {
  const definition = migration.match(
    /CREATE INDEX IF NOT EXISTS idx_packets_message_time\s+ON mesh\.packets \(([^)]+)\)\s+WHERE packet_type = (\d+);/,
  );
  assert.ok(definition, 'migration 156 must keep the named partial index');
  // Leading column serves network = ANY($3); the second serves the time range.
  assert.equal(definition[1].trim(), 'network, time DESC');
  const windowFilter = new RegExp(
    String.raw`WHERE p\.packet_type = ${definition[2]}\s+AND p\.time >= \$1\s+AND p\.time < \$2\s+AND p\.network = ANY\(\$3\)`,
    'g',
  );
  // countLogicalMessages, the logical_messages CTE and ranked_observers all
  // share the literal predicate, so all three can use the partial index.
  assert.equal((repository.match(windowFilter) ?? []).length, 3);
});
