import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('observer-region backfill applies authoritative packet privacy to both source queries', () => {
  const source = readFileSync(
    new URL('./backfillStatsRollups.ts', import.meta.url),
    'utf8',
  );
  const regionBackfill = source.slice(
    source.indexOf('async function backfillObserverRegionRollups'),
    source.indexOf('async function main'),
  );
  assert.notEqual(regionBackfill, '');
  assert.equal(
    regionBackfill.match(/AND \$\{PUBLIC_PACKET_PRIVACY_SQL\}/g)?.length,
    2,
  );
  assert.equal(
    regionBackfill.match(
      /COALESCE\(NULLIF\(p\.topic_prefix, ''\), split_part\(p\.topic, '\/', 1\)\) <> 'meshcore-test'/g,
    )?.length,
    2,
  );
});

test('cutover reconciliation closes the completed-checkpoint gap in bounded locked slices', () => {
  const source = readFileSync(
    new URL('./backfillStatsRollups.ts', import.meta.url),
    'utf8',
  );
  const cutover = source.slice(
    source.indexOf('const MAX_CUTOVER_GAP_HOURS'),
    source.indexOf('/**\n * Reconstruct small stats rollups'),
  );
  assert.match(cutover, /MAX_CUTOVER_GAP_HOURS = 48/);
  assert.match(cutover, /maintenance_backfill_checkpoints/);
  assert.match(cutover, /completed_at IS NOT NULL/);
  assert.match(cutover, /while \(cursor < windowEnd\)/);
  assert.match(cutover, /LOCK TABLE packet_hourly_stats IN SHARE ROW EXCLUSIVE MODE/);
  assert.match(cutover, /p\.time >= \$1::timestamptz/);
  assert.match(cutover, /p\.time < \$2::timestamptz/);
});
