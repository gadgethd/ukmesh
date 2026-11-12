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
