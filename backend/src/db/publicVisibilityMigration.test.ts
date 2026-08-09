import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const migration = readFileSync(
  new URL('./migrations/041_stable_public_visibility_generation.sql', import.meta.url),
  'utf8',
);

test('public visibility generation changes only when node privacy changes', () => {
  assert.match(migration, /old_private IS DISTINCT FROM new_private/);
  assert.doesNotMatch(migration, /decode_inputs_changed|OLD\.lat|OLD\.lon|OLD\.role/);
  assert.match(migration, /IF TG_OP = 'DELETE' THEN RETURN OLD/);
});
