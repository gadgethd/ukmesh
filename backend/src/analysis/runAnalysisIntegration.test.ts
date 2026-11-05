import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import test from 'node:test';

// The analysis integration step once reported a 25 ms fixture error and then
// idled until GitHub's six-hour job timeout because an open pg socket kept the
// event loop alive. These tests pin the runner's contract: an absent or
// unreachable database must produce a fast, non-zero, self-explaining exit.
const scriptUrl = new URL('../../scripts/run-analysis-integration.mjs', import.meta.url);

function runAnalysisIntegration(env: NodeJS.ProcessEnv) {
  const startedAt = Date.now();
  return new Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string; elapsedMs: number }>(
    (resolve, reject) => {
      const child = spawn(process.execPath, [scriptUrl.pathname], { env, stdio: ['ignore', 'ignore', 'pipe'] });
      let stderr = '';
      child.stderr.setEncoding('utf8');
      child.stderr.on('data', (chunk: string) => { stderr += chunk; });
      child.on('error', reject);
      child.on('exit', (code, signal) => {
        resolve({ code, signal, stderr, elapsedMs: Date.now() - startedAt });
      });
    },
  );
}

function withoutDatabaseUrl(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env['TEST_ANALYSIS_DATABASE_URL'];
  return env;
}

test('analysis integration runner fails fast when the database dependency is absent', { timeout: 10_000 }, async () => {
  const result = await runAnalysisIntegration(withoutDatabaseUrl());
  assert.equal(result.code, 1);
  assert.match(result.stderr, /TEST_ANALYSIS_DATABASE_URL is not set/);
  assert.ok(result.elapsedMs < 5_000, `expected a fast failure, took ${result.elapsedMs}ms`);
});

test('analysis integration runner fails fast when PostgreSQL refuses connections', { timeout: 15_000 }, async () => {
  const result = await runAnalysisIntegration({
    ...process.env,
    TEST_ANALYSIS_DATABASE_URL: 'postgresql://postgres:unused@127.0.0.1:1/postgres',
    ANALYSIS_INTEGRATION_CONNECT_TIMEOUT_MS: '2000',
  });
  assert.equal(result.code, 1);
  assert.match(result.stderr, /PostgreSQL at 127\.0\.0\.1 is unavailable/);
  assert.ok(result.elapsedMs < 10_000, `expected a fast failure, took ${result.elapsedMs}ms`);
});
