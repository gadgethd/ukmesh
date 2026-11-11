import assert from 'node:assert/strict';
import test from 'node:test';
import { convergenceErrorMessage, parseConvergenceArgs } from '../tools/convergePacketVisibility.js';
import { convergePacketVisibility, PrivacyConvergenceError } from './convergence.js';

test('convergence CLI defaults to dry-run and requires an exact reviewed generation for apply', () => {
  assert.deepEqual(parseConvergenceArgs([]), {});
  assert.deepEqual(parseConvergenceArgs(['--dry-run']), { apply: false });
  assert.deepEqual(parseConvergenceArgs(['--apply', '--expected-generation=42', '--statement-timeout-ms=1000']),
    { apply: true, expectedGeneration: '42', statementTimeoutMs: 1000 });
  for (const args of [
    ['--apply'], ['--apply=false'], ['--aply'], ['--expected-generation=0'],
    ['--expected-generation=-1'], ['--expected-generation=1e3'],
    ['--expected-generation=9007199254740991'], ['--statement-timeout-ms=NaN'],
    ['--statement-timeout-ms=999'], ['--statement-timeout-ms=3600001'],
    ['--apply', '--dry-run'], ['--apply', '--apply'],
  ]) assert.throws(() => parseConvergenceArgs(args), undefined, args.join(' '));
});

test('unsafe apply parameters fail before connecting to any database', async () => {
  const pool = { connect() { assert.fail('invalid parameters must not connect'); } };
  await assert.rejects(convergePacketVisibility(pool as never, { apply: true }), /EXPECTED_GENERATION_REQUIRED/);
  await assert.rejects(convergePacketVisibility(pool as never, { statementTimeoutMs: 999 }), /STATEMENT_TIMEOUT_OUT_OF_RANGE/);
});

test('CLI diagnostics expose controlled failures and SQLSTATE without server data or credentials', () => {
  assert.equal(convergenceErrorMessage(new PrivacyConvergenceError('VISIBILITY_GENERATION_CHANGED')),
    'VISIBILITY_GENERATION_CHANGED');
  assert.equal(convergenceErrorMessage({ code: '55P03', message: 'sensitive server detail', detail: 'packet content' }),
    'database operation failed (SQLSTATE 55P03); inspect server diagnostics');
  assert.equal(convergenceErrorMessage(new Error('postgresql://user:password@host/database')),
    'database connection or operation failed; inspect local diagnostics');
});
