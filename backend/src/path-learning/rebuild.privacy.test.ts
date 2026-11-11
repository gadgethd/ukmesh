import assert from 'node:assert/strict';
import test from 'node:test';
import type { QueryResultRow } from 'pg';
import {
  dropPrivatePrefixRows,
  isPrivatePathPrefix,
  pathLearningNetworksSql,
  pathLearningTrainingPacketsSql,
} from './rebuild.js';
import { publicPacketPrivacySql } from '../api/utils/networkFilters.js';
import {
  createPathingRepository,
  privatePrefixPathFilterSql,
} from '../pathing/pathingRepository.js';

const PRIVATE_NODE = 'C'.repeat(64);

test('path-model training selects only packets that pass the public privacy predicate', () => {
  const statements = [
    pathLearningTrainingPacketsSql('AND network = $1', true),
    pathLearningTrainingPacketsSql('', false),
    pathLearningNetworksSql(),
  ];
  for (const sql of statements) {
    // The whole canonical predicate is embedded, so training cannot drift from
    // public reads as the predicate gains fences.
    assert.ok(sql.includes(publicPacketPrivacySql()));
    assert.match(sql, /visibility_ok IS TRUE/);
    assert.match(sql, /is_private IS NOT TRUE/);
    assert.match(sql, /packet_visibility_materialization_state|privacy_rematerialization_queue/);
  }
});

test('training keeps its route, time and sampling predicates beside the privacy fence', () => {
  const sql = pathLearningTrainingPacketsSql('AND network = ANY($1)', true);
  assert.match(sql, /cardinality\(path_hashes\) > 0/);
  assert.match(sql, /path_hash_size_bytes >= 2/);
  assert.match(sql, /mod\(hashtext\(packet_hash\)::bigint \+ 2147483648, 100\) < \d+/);
  assert.match(sql, /AND network = ANY\(\$1\)/);
  assert.match(sql, /\$2::timestamptz/);
  assert.match(sql, /\$3::timestamptz/);
  assert.match(sql, /LIMIT \$4/);

  // The all-networks fallback has no network param, so window/limit shift down.
  const fallback = pathLearningTrainingPacketsSql('', false);
  assert.match(fallback, /\$1::timestamptz/);
  assert.match(fallback, /\$2::timestamptz/);
  assert.match(fallback, /LIMIT \$3/);
});

test('publication drops every prior whose prefix resolves onto a private node', () => {
  const privateNodeIds = [PRIVATE_NODE];
  assert.equal(isPrivatePathPrefix(PRIVATE_NODE.slice(0, 2).toLowerCase(), privateNodeIds), true);
  assert.equal(isPrivatePathPrefix(PRIVATE_NODE.slice(0, 6), privateNodeIds), true);
  assert.equal(isPrivatePathPrefix('', privateNodeIds), false);
  assert.equal(isPrivatePathPrefix(null, privateNodeIds), false);
  assert.equal(isPrivatePathPrefix('DD', privateNodeIds), false);

  const rows = [
    { prefix: 'AA', prev_prefix: '', node_id: 'AA11' },
    { prefix: PRIVATE_NODE.slice(0, 4), prev_prefix: 'AA', node_id: 'BB22' },
    { prefix: 'DD', prev_prefix: PRIVATE_NODE.slice(0, 2), node_id: 'EE33' },
    { prefix: 'FF', prev_prefix: 'GG', node_id: 'HH44' },
  ];
  assert.deepEqual(
    dropPrivatePrefixRows(rows, privateNodeIds).map((row) => row.node_id),
    ['AA11', 'HH44'],
  );
  // Without a private prefix list nothing is dropped, including head-of-path rows.
  assert.equal(dropPrivatePrefixRows(rows, []).length, rows.length);
});

test('public path reads exclude prefixes and prev prefixes that resolve onto a private node', async () => {
  const statements: string[] = [];
  const repository = createPathingRepository({
    getPublicVisibilityGeneration: async () => 1,
    query: async <T extends QueryResultRow>(text: string) => {
      statements.push(text);
      return { rows: [] as T[] };
    },
  });
  await repository.fetchPathLearning('ukmesh', 10);

  const prefixQuery = statements[0]!;
  assert.equal((prefixQuery.match(/FROM private_node_prefixes private_prefix/g) ?? []).length, 2);
  assert.match(
    prefixQuery,
    /NOT EXISTS \(\s*SELECT 1\s*FROM private_node_prefixes private_prefix\s*WHERE LENGTH\(pp\.prefix\) > 0/,
  );
  assert.match(
    prefixQuery,
    /LEFT\(UPPER\(private_prefix\.node_id\), LENGTH\(pp\.prefix\)\)\s*= UPPER\(pp\.prefix\)/,
  );
  assert.match(
    prefixQuery,
    /NOT EXISTS \(\s*SELECT 1\s*FROM private_node_prefixes private_prefix\s*WHERE LENGTH\(pp\.prev_prefix\) > 0/,
  );
  // The head of every path has an empty prev_prefix: it must never be filtered.
  assert.match(privatePrefixPathFilterSql('pp.prev_prefix'), /LENGTH\(pp\.prev_prefix\) > 0/);
});
