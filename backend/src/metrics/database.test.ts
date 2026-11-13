import assert from 'node:assert/strict';
import test from 'node:test';
import { databaseOperation } from './database.js';

test('database telemetry accepts only bounded operation names and never SQL or identities', () => {
  assert.equal(databaseOperation('/* ukmesh:node-map */ SELECT 1'), 'node-map');
  for (const sql of ['SELECT secret FROM owners', '/* ukmesh:private-node-ABC123 */ SELECT 1',
    '/* ukmesh:node-map-with-an-actor-id */ SELECT 1', '/* ukmesh:other */ SELECT 1']) {
    assert.equal(databaseOperation(sql), 'other');
  }
});
