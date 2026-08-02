import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  PUBLIC_MAP_MAX_PAGE_BYTES,
  PUBLIC_MAP_MAX_PAGE_ROWS,
  PublicMapInputError,
  encodePublicMapCursor,
  fitPublicMapRowsToByteBudget,
  parsePublicMapFields,
  parsePublicMapCursor,
  parsePublicMapLimit,
  parsePublicMapSnapshot,
  publicMapFreshPredicate,
} from './publicMap.js';

test('public map field selection rejects ambiguous or unknown projections', () => {
  assert.deepEqual(parsePublicMapFields('name,lat'), ['node_id', 'name', 'lat']);
  assert.throws(() => parsePublicMapFields('name,secret'));
  assert.throws(() => parsePublicMapFields(['name', 'lat']));
});

test('public map predicate is the single role, coordinate, privacy, and freshness contract', () => {
  const sql = publicMapFreshPredicate('n', '$2::timestamptz');
  assert.match(sql, /n\.lat BETWEEN -90 AND 90/);
  assert.match(sql, /n\.lon BETWEEN -180 AND 180/);
  assert.match(sql, /NOT \(ABS\(n\.lat\) < 5 AND ABS\(n\.lon\) < 5\)/);
  assert.match(sql, /n\.name NOT LIKE '%🚫%'/);
  assert.match(sql, /n\.role IS NULL OR n\.role NOT IN \(1, 3\)/);
  assert.match(sql, /GREATEST\(n\.last_seen, n\.last_path_evidence_at\)/);
  assert.match(sql, /> \$2::timestamptz - INTERVAL '28 days'/);
  assert.match(sql, /<= \$2::timestamptz/);
});

test('public map cursor is opaque, round-trips, and rejects malformed input', () => {
  const encoded = encodePublicMapCursor('ABCDEF012345');
  assert.equal(parsePublicMapCursor(encoded), 'ABCDEF012345');
  assert.throws(
    () => parsePublicMapCursor('not-json'),
    PublicMapInputError,
  );
  assert.throws(
    () => parsePublicMapCursor('x'.repeat(513)),
    PublicMapInputError,
  );
});

test('public map snapshot rejects expired, future, and non-canonical timestamps', () => {
  const now = Date.parse('2026-07-29T12:00:00.000Z');
  assert.equal(
    parsePublicMapSnapshot('2026-07-29T11:59:00.000Z', now),
    '2026-07-29T11:59:00.000Z',
  );
  assert.throws(
    () => parsePublicMapSnapshot('2026-07-29T11:00:00.000Z', now),
    /expired or invalid/,
  );
  assert.throws(
    () => parsePublicMapSnapshot('2026-07-29 12:00:00Z', now),
    /expired or invalid/,
  );
});

test('public map enforces row and encoded-byte budgets on large results', () => {
  assert.equal(parsePublicMapLimit(String(PUBLIC_MAP_MAX_PAGE_ROWS)), PUBLIC_MAP_MAX_PAGE_ROWS);
  assert.throws(
    () => parsePublicMapLimit(String(PUBLIC_MAP_MAX_PAGE_ROWS + 1)),
    /between 1 and/,
  );
  assert.throws(() => parsePublicMapLimit('-1'), /positive integer/);

  const manyRows = Array.from({ length: PUBLIC_MAP_MAX_PAGE_ROWS + 100 }, (_, index) => ({
    node_id: index.toString().padStart(64, '0'),
    name: `node-${index}`,
  }));
  const rowBounded = fitPublicMapRowsToByteBudget(
    manyRows,
    PUBLIC_MAP_MAX_PAGE_ROWS,
    PUBLIC_MAP_MAX_PAGE_BYTES,
  );
  assert.equal(rowBounded.rows.length, PUBLIC_MAP_MAX_PAGE_ROWS);
  assert.equal(rowBounded.truncatedByBytes, false);

  const byteBounded = fitPublicMapRowsToByteBudget(
    manyRows.map((row) => ({ ...row, name: 'x'.repeat(1000) })),
    PUBLIC_MAP_MAX_PAGE_ROWS,
    10_000,
  );
  assert.ok(byteBounded.rows.length > 0);
  assert.ok(byteBounded.rows.length < PUBLIC_MAP_MAX_PAGE_ROWS);
  assert.equal(byteBounded.truncatedByBytes, true);
  assert.ok(
    Buffer.byteLength(JSON.stringify(byteBounded.rows), 'utf8') < 10_000,
  );
});
