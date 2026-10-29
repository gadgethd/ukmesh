import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BASEMAP_RETRY_TIMEOUT_MS,
  BASEMAP_SOURCE_LOAD_TIMEOUT_MS,
  BASEMAP_TILE_ERROR_THRESHOLD,
  createBasemapHealth,
  decideBasemapAction,
  noteFallback,
  noteRetryStarted,
  noteSourceData,
  noteStyleLoaded,
  noteTileError,
} from './basemapHealth.js';

test('basemap enters healthy state as soon as the vector source reports data', () => {
  let health = createBasemapHealth();
  health = noteStyleLoaded(health, 1_000);
  assert.equal(decideBasemapAction(health, 1_000 + BASEMAP_SOURCE_LOAD_TIMEOUT_MS - 1), 'none');

  health = noteSourceData(health, 'openmaptiles', true, 2_000);
  assert.equal(health.phase, 'healthy');
  assert.equal(decideBasemapAction(health, 10_000_000), 'none');

  // Late errors must not knock a healthy basemap back into recovery.
  health = noteTileError(health, 3_000, 'tile 404');
  assert.equal(decideBasemapAction(health, 10_000_000), 'none');
});

test('a stalled basemap retries once with a cache-busted style, then falls back', () => {
  let health = createBasemapHealth();
  health = noteStyleLoaded(health, 0);

  assert.equal(decideBasemapAction(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS - 1), 'none');
  assert.equal(decideBasemapAction(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS), 'retry');

  health = noteRetryStarted(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS);
  assert.equal(health.phase, 'retrying');
  assert.equal(health.retries, 1);

  // The retry's style.load restarts the clock, but no tiles arrive.
  health = noteStyleLoaded(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS + 500);
  assert.equal(
    decideBasemapAction(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS + 500 + BASEMAP_RETRY_TIMEOUT_MS - 1),
    'none',
  );
  assert.equal(
    decideBasemapAction(health, BASEMAP_SOURCE_LOAD_TIMEOUT_MS + 500 + BASEMAP_RETRY_TIMEOUT_MS),
    'fallback',
  );

  health = noteFallback(health, 'timeout');
  assert.equal(health.phase, 'fallback');
  assert.equal(decideBasemapAction(health, 10_000_000), 'none');
  // The fallback is terminal for the session: style loads must not reset it.
  assert.equal(noteStyleLoaded(health, 20_000).phase, 'fallback');
});

test('a burst of tile errors triggers recovery without waiting for the timeout', () => {
  let health = createBasemapHealth();
  health = noteStyleLoaded(health, 1_000);
  for (let index = 0; index < BASEMAP_TILE_ERROR_THRESHOLD; index += 1) {
    health = noteTileError(health, 1_500 + index, 'tile failed');
  }
  assert.equal(decideBasemapAction(health, 1_500 + BASEMAP_TILE_ERROR_THRESHOLD), 'retry');
  assert.equal(health.lastError, 'tile failed');
});

test('tile errors outside the window do not accumulate', () => {
  let health = createBasemapHealth();
  health = noteStyleLoaded(health, 0);
  for (let index = 0; index < BASEMAP_TILE_ERROR_THRESHOLD; index += 1) {
    health = noteTileError(health, index * 5_000, 'tile failed');
  }
  assert.equal(decideBasemapAction(health, 60_000), 'retry');
  assert.ok(health.tileErrors.length < BASEMAP_TILE_ERROR_THRESHOLD);
});
