import assert from 'node:assert/strict';
import test from 'node:test';
import { boundedIntegerSetting } from './boundedNumber.js';
import { analyticsStatementTimeoutMs, loadDatabaseConfig } from './database.js';

test('database numeric settings use bounded deployment-safe defaults', () => {
  assert.deepEqual(loadDatabaseConfig({}), {
    schema: '',
    skipSchemaInit: false,
    applicationName: 'meshcore-backend',
    statementTimeoutMs: 30_000,
    connectionTimeoutMs: 5_000,
    poolMax: 8,
    idleTimeoutMs: 30_000,
  });
});

test('database numeric settings accept only canonical integers in their reviewed ranges', () => {
  assert.equal(boundedIntegerSetting('POOL', '1', 8, 1, 100), 1);
  assert.equal(boundedIntegerSetting('POOL', '100', 8, 1, 100), 100);
  assert.equal(boundedIntegerSetting('TIMEOUT', '0', 30_000, 0, 3_600_000), 0);
  assert.equal(boundedIntegerSetting('TIMEOUT', '3600000', 30_000, 0, 3_600_000), 3_600_000);
  assert.equal(loadDatabaseConfig({ DATABASE_CONNECTION_TIMEOUT_MS: '30000' }).connectionTimeoutMs, 30_000);

  for (const invalid of ['-1', '1.5', '1e2', '0x10', 'NaN', 'Infinity', ' 01 ', '101']) {
    assert.throws(
      () => boundedIntegerSetting('POOL', invalid, 8, 1, 100),
      /POOL must be an integer between 1 and 100/,
    );
  }
});

test('database configuration rejects invalid pool, timeout, and schema settings before connecting', () => {
  assert.throws(
    () => loadDatabaseConfig({ DATABASE_POOL_MAX: '0' }),
    /DATABASE_POOL_MAX must be an integer between 1 and 100/,
  );
  assert.throws(
    () => loadDatabaseConfig({ DATABASE_STATEMENT_TIMEOUT_MS: '-1' }),
    /DATABASE_STATEMENT_TIMEOUT_MS must be an integer between 0 and 3600000/,
  );
  assert.throws(
    () => loadDatabaseConfig({ DATABASE_CONNECTION_TIMEOUT_MS: '300001' }),
    /DATABASE_CONNECTION_TIMEOUT_MS must be an integer between 100 and 300000/,
  );
  assert.throws(
    () => loadDatabaseConfig({ DATABASE_SCHEMA: 'public; DROP SCHEMA public' }),
    /Invalid DATABASE_SCHEMA/,
  );
});

test('analytics queries honor a longer configured timeout while retaining a safe floor', () => {
  assert.equal(analyticsStatementTimeoutMs(30_000), 300_000);
  assert.equal(analyticsStatementTimeoutMs(900_000), 900_000);
  assert.equal(analyticsStatementTimeoutMs(0), 0);
});
