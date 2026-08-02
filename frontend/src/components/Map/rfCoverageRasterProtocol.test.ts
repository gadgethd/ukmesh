import assert from 'node:assert/strict';
import test from 'node:test';
import {
  maxRfRasterZoom,
  rfRasterSourceBounds,
  rfRasterTileBounds,
} from './rfCoverageRasterProtocol.js';

test('RF raster XYZ bounds follow Web Mercator tile coordinates', () => {
  const world = rfRasterTileBounds(0, 0, 0);
  assert.equal(world.West, -180);
  assert.equal(world.East, 180);
  assert.ok(Math.abs(world.North - 85.0511287798066) < 1e-12);
  assert.ok(Math.abs(world.South + 85.0511287798066) < 1e-12);

  const ukTile = rfRasterTileBounds(6, 31, 20);
  assert.equal(ukTile.West, -5.625);
  assert.equal(ukTile.East, 0);
  assert.ok(ukTile.North > ukTile.South);
  assert.ok(ukTile.North > 53 && ukTile.South < 53);
});

test('RF raster source bounds contain every published coverage image', () => {
  assert.deepEqual(rfRasterSourceBounds([
    { url: '/north.png', bounds: { West: -9, East: -2, South: 54, North: 60 } },
    { url: '/south.png', bounds: { West: -3, East: 4, South: 49, North: 55 } },
  ]), [-9, 49, 4, 60]);
  assert.equal(maxRfRasterZoom('standard'), 9);
  assert.equal(maxRfRasterZoom('precision'), 10);
});
