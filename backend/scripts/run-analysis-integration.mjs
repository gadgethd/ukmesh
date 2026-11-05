#!/usr/bin/env node
// Runs the analysis integration test with a fail-fast dependency preflight and
// a hard wall-clock bound.
//
// Why this exists: the test used to leave a pg socket open when its fixture
// setup failed, so node:test reported the failure in 25 ms but the process
// never exited. npm then waited until GitHub's six-hour job timeout, burning a
// runner and hiding the real error. A missing dependency must therefore fail
// in seconds with an actionable message, and a wedged test must be killed
// rather than allowed to idle.
import { spawn } from 'node:child_process';
import pg from 'pg';

const ENV_VAR = 'TEST_ANALYSIS_DATABASE_URL';
const TEST_FILE = 'src/analysis/runState.integration.test.ts';
const CONNECT_TIMEOUT_MS = Number(process.env['ANALYSIS_INTEGRATION_CONNECT_TIMEOUT_MS'] ?? 5_000);
const RUN_TIMEOUT_MS = Number(process.env['ANALYSIS_INTEGRATION_TIMEOUT_MS'] ?? 120_000);

function fail(message) {
  console.error(`::error::analysis-integration: ${message}`);
  process.exit(1);
}

const databaseUrl = String(process.env[ENV_VAR] ?? '').trim();
if (!databaseUrl) {
  fail(
    `${ENV_VAR} is not set. CI supplies it from the postgres service in `
      + '.github/workflows/ci.yml; locally export a disposable PostgreSQL URL.',
  );
}

let host;
try {
  host = new URL(databaseUrl).hostname;
} catch {
  fail(`${ENV_VAR} is not a valid connection URL`);
}

const startedAt = Date.now();
const client = new pg.Client({
  connectionString: databaseUrl,
  application_name: 'ukmesh-analysis-integration-preflight',
  connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
  statement_timeout: CONNECT_TIMEOUT_MS,
  query_timeout: CONNECT_TIMEOUT_MS,
});

// pg's own timeouts cover socket setup and the statement, but a server that
// accepts TCP and then stalls the startup handshake would still block, so the
// probe races an explicit deadline as well.
const probe = (async () => {
  await client.connect();
  await client.query('SELECT 1');
})();
const deadline = new Promise((_resolve, reject) => {
  const timer = setTimeout(
    () => reject(new Error(`connection probe exceeded ${CONNECT_TIMEOUT_MS}ms`)),
    CONNECT_TIMEOUT_MS,
  );
  timer.unref();
});

try {
  await Promise.race([probe, deadline]);
} catch (error) {
  await client.end().catch(() => {});
  fail(`PostgreSQL at ${host} is unavailable: ${error.message}`);
}
await client.end().catch(() => {});
console.log(`analysis-integration: PostgreSQL reachable at ${host} in ${Date.now() - startedAt}ms`);

const child = spawn(process.execPath, ['--import', 'tsx', '--test', TEST_FILE], { stdio: 'inherit' });
let killedByTimeout = false;
const killer = setTimeout(() => {
  killedByTimeout = true;
  console.error(
    `::error::analysis-integration: test exceeded ${RUN_TIMEOUT_MS}ms; killing it so the job fails now instead of hanging`,
  );
  child.kill('SIGKILL');
}, RUN_TIMEOUT_MS);
killer.unref();

const exitCode = await new Promise((resolve) => {
  child.on('error', (error) => {
    console.error(`::error::analysis-integration: cannot start the test runner: ${error.message}`);
    resolve(1);
  });
  child.on('exit', (code, signal) => {
    if (killedByTimeout) resolve(124);
    else if (typeof code === 'number') resolve(code);
    else resolve(signal ? 128 : 1);
  });
});
clearTimeout(killer);
process.exit(exitCode);
