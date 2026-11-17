import assert from 'node:assert/strict';
import test from 'node:test';
import { boundedIntegerSetting } from './boundedNumber.js';
import { analyticsConnectionOptions, analyticsPlannerOptions, analyticsStatementTimeoutMs, databaseUrlForRole, loadDatabaseConfig } from './database.js';

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
  assert.equal(analyticsStatementTimeoutMs(30_000), 900_000);
  assert.equal(analyticsStatementTimeoutMs(900_000), 900_000);
  assert.equal(analyticsStatementTimeoutMs(0), 0);
});

test('analytics planner estimates are explicit and absent settings retain standalone defaults', () => {
  assert.equal(analyticsPlannerOptions({}), undefined);
  assert.equal(analyticsPlannerOptions({ DATABASE_APPLICATION_NAME: 'meshcore-backend' }), undefined);
  assert.equal(analyticsPlannerOptions({ DATABASE_ANALYTICS_RANDOM_PAGE_COST: '2.5' }), '-c random_page_cost=2.5');
  assert.equal(analyticsPlannerOptions({ DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE: '1GB' }), '-c effective_cache_size=1GB');
  assert.equal(analyticsPlannerOptions({ DATABASE_ANALYTICS_RANDOM_PAGE_COST: '4',
    DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE: '512MB' }), '-c random_page_cost=4 -c effective_cache_size=512MB');
});

test('analytics planner estimates reject malformed or injected startup options', () => {
  for (const value of ['-1', 'NaN', 'Infinity', '1e2', '01', '4 -c role=other']) {
    assert.throws(() => analyticsPlannerOptions({ DATABASE_ANALYTICS_RANDOM_PAGE_COST: value }),
      /DATABASE_ANALYTICS_RANDOM_PAGE_COST must be a nonnegative decimal/);
  }
  for (const value of ['0MB', '1kB', '16TB', '512mb', '1.5GB', '512MB -c role=other']) {
    assert.throws(() => analyticsPlannerOptions({ DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE: value }),
      /DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE must be a PostgreSQL cache estimate/);
  }
});

test('owner session options preserve worker URL flags and pg option precedence', () => {
  const env = { MESHCORE_WORKER_DATABASE_URL: 'postgres://worker@db/meshcore?options=-c%20search_path%3Dignored&options=-c%20default_transaction_read_only%3Don',
    DATABASE_ANALYTICS_RANDOM_PAGE_COST: '4' };
  const result = analyticsConnectionOptions(env, '-c search_path=core,public', '-c work_mem=16384kB');
  assert.deepEqual(new URL(result.connectionString!).searchParams.getAll('options'),
    ['-c default_transaction_read_only=on -c random_page_cost=4 -c work_mem=16384kB']);
  assert.equal(result.options, '-c search_path=core,public -c random_page_cost=4 -c work_mem=16384kB');
  const withoutUrlFlags = analyticsConnectionOptions({ MESHCORE_WORKER_DATABASE_URL: 'postgres://worker@db/meshcore' },
    '-c search_path=core,public', '-c work_mem=16384kB');
  assert.equal(withoutUrlFlags.connectionString, 'postgres://worker@db/meshcore');
  assert.equal(withoutUrlFlags.options, '-c search_path=core,public -c work_mem=16384kB');
});

test('role DSNs prefer role-specific URLs and allow a legacy fallback only before enforcement', () => {
  assert.equal(
    databaseUrlForRole('ingest', {
      MESHCORE_INGEST_DATABASE_URL: 'postgres://ingest@db/meshcore',
      DATABASE_URL: 'postgres://legacy@db/meshcore',
    }),
    'postgres://ingest@db/meshcore',
  );
  assert.equal(
    databaseUrlForRole('worker', {
      DATABASE_URL: 'postgres://legacy@db/meshcore',
    }),
    'postgres://legacy@db/meshcore',
  );
  assert.equal(
    databaseUrlForRole('http', {}),
    undefined,
  );
});

test('role DSN enforcement makes a missing role URL fatal', () => {
  assert.throws(
    () => databaseUrlForRole('http', {
      DATABASE_ROLE_ENFORCEMENT: 'true',
      DATABASE_URL: 'postgres://legacy@db/meshcore',
    }),
    /MESHCORE_HTTP_DATABASE_URL is required/,
  );
});
