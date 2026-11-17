import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import test from 'node:test';

// Instrument pool construction in a fresh process so unrelated import caches
// cannot mask configuration reads or accidental default execution ownership.
test('database execution definitions import without process configuration or pool construction', () => {
  const source = `
    import pg from 'pg';
    const OriginalPool = pg.Pool;
    let pools = 0;
    pg.Pool = class extends OriginalPool { constructor(...args) { super(...args); pools++; } };
    await import(${JSON.stringify(new URL('./databaseRuntime.ts', import.meta.url).href)});
    if (pools !== 0) throw new Error('factory import created pools');
    console.log('passive');
  `;
  const result = execFileSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', source], {
    env: { ...process.env, DATABASE_SCHEMA: 'invalid;schema', DATABASE_POOL_MAX: 'invalid' },
    encoding: 'utf8', timeout: 10_000,
  });
  assert.equal(result.trim(), 'passive');
});

test('only explicit analytics pools receive planner options while URL read-only and schema options survive', () => {
  const source = `
    import assert from 'node:assert/strict';
    import pg from 'pg';
    const { createDatabaseRuntime } = await import(${JSON.stringify(new URL('./databaseRuntime.ts', import.meta.url).href)});
    const base = 'postgresql://owned@127.0.0.1/owned';
    const options = pool => new pg.Client(pool.options).connectionParameters.options;
    for (const previous of [undefined, '-c default_transaction_read_only=on -c search_path=custom,public']) {
      const worker = new URL(base);
      if (previous) worker.searchParams.set('options', previous);
      const runtime = createDatabaseRuntime({ DATABASE_URL: base, MESHCORE_WORKER_DATABASE_URL: worker.href,
        DATABASE_SCHEMA: 'fixture', DATABASE_ANALYTICS_RANDOM_PAGE_COST: '4',
        DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE: '512MB' });
      try {
        assert.equal(options(runtime.analyticsPool), (previous ?? '-c search_path=fixture,public')
          + ' -c random_page_cost=4 -c effective_cache_size=512MB');
        assert.equal(options(runtime.pool), '-c search_path=fixture,public');
        assert.equal(options(runtime.ingestPool), '-c search_path=fixture,public');
        assert.equal(runtime.pool.options.connectionString, base);
        assert.equal(runtime.ingestPool.options.connectionString, base);
        assert.equal(runtime.pool.options.max, 8);
        assert.equal(runtime.analyticsPool.options.max, 2);
        assert.equal(runtime.ingestPool.options.max, 2);
      } finally { await runtime.close(); }
    }
    const standalone = createDatabaseRuntime({ DATABASE_URL: base, DATABASE_APPLICATION_NAME: 'meshcore-backend' });
    try { assert.equal(options(standalone.analyticsPool), undefined); }
    finally { await standalone.close(); }
    const OriginalPool = pg.Pool; let constructed = 0;
    pg.Pool = class extends OriginalPool { constructor(...args) { super(...args); constructed++; } };
    assert.throws(() => createDatabaseRuntime({ DATABASE_ANALYTICS_RANDOM_PAGE_COST: '4 -c role=other' }));
    assert.equal(constructed, 0, 'validate analytics options before creating any pool');
    console.log('isolated');
  `;
  const result = execFileSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', source], {
    env: { ...process.env, PGOPTIONS: '' }, encoding: 'utf8', timeout: 10_000,
  });
  assert.equal(result.trim(), 'isolated');
});
